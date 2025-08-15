// DtSearchManager.cs — Final Run4 (reflection-only, diagnostics + defaults)
// Target: .NET Framework 4.8.1 (x64)
// Note : Compiles without dtSearchNetApi4.dll. At runtime, ensure dtSearchNetApi4.dll + dtengine64.dll are available.
// Test change for PR

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Collections.Generic;
using System.Collections.Specialized;

namespace DtSearchBridge2
{
    // -------------------- Lightweight logger (assign Sink from host) --------------------
    public static class DtLog
    {
        // Example from Python: DtLog.Sink = lambda s: print(s)
        public static Action<string> Sink { get; set; }
        public static void Info(string msg)
        {
            try { Sink?.Invoke($"[DtSearch] {msg}"); } catch { /* ignore */ }
        }
    }

    // -------------------- DTOs & options --------------------
    public class SearchResultDto
    {
        public string FilePath { get; set; } = "";
        public int Page { get; set; }
        public string Snippet { get; set; } = "";
        public double Score { get; set; }
        public string Title { get; set; } = "";
        public int HitCount { get; set; }
    }

    public class SearchOptions
    {
        public int TopK { get; set; } = 20;
        public int? TimeoutMs { get; set; }
        public bool? UseStemming { get; set; }
        public bool? CaseSensitive { get; set; }
        public bool? AccentSensitive { get; set; }
        public string SearchFlags { get; set; }
        public int? MaxContextBytes { get; set; }
    }

    public class DtSearchManager
    {
        public string LastError { get; private set; }
        public string LastWarning { get; private set; }

        // -------------------- Diagnostics toggle --------------------
        public static bool DiagnosticsEnabled { get; set; } = false;

        // -------------------- Runtime defaults (configurable from Python) --------------------
        public class DtSearchDefaults
        {
            public int? TimeoutMs { get; set; } = 30000;
            public bool? UseStemming { get; set; } = true;
            public bool? CaseSensitive { get; set; } = null;
            public bool? AccentSensitive { get; set; } = null;
            public int? MaxContextBytes { get; set; } = 1024;
            public string SearchFlags { get; set; } = null;
        }

        private static DtSearchDefaults _defaults = new DtSearchDefaults();

        public static void SetDefaults(DtSearchDefaults cfg)
        {
            _defaults = cfg ?? new DtSearchDefaults();
            if (DiagnosticsEnabled)
            {
                DtLog.Info($"Defaults set: TimeoutMs={_defaults.TimeoutMs}, Stemming={_defaults.UseStemming}, " +
                           $"Case={_defaults.CaseSensitive}, Accent={_defaults.AccentSensitive}, " +
                           $"MaxCtx={_defaults.MaxContextBytes}, Flags={_defaults.SearchFlags ?? "(null)"}");
            }
        }

        // -------------------- dtSearch type helpers (reflection) --------------------
        private static Type FindType(string fullName)
        {
            var t = Type.GetType(fullName, throwOnError: false);
            if (t != null) return t;

            foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
            {
                t = asm.GetType(fullName, throwOnError: false);
                if (t != null) return t;
            }

            foreach (var name in new[] { "dtSearchNetApi4", "dtSearchNetStdApi", "dtSearch.Engine" })
            {
                try
                {
                    var asm = Assembly.Load(new AssemblyName(name));
                    t = asm.GetType(fullName, throwOnError: false);
                    if (t != null) return t;
                }
                catch { /* ignore */ }
            }
            return null;
        }

        private static object Create(string fullTypeName)
        {
            var t = FindType(fullTypeName);
            return t != null ? Activator.CreateInstance(t) : null;
        }

        private static Assembly GetDtSearchAssembly()
        {
            foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
            {
                var n = asm.GetName().Name ?? "";
                if (n.IndexOf("dtsearch", StringComparison.OrdinalIgnoreCase) >= 0) return asm;
            }
            try { return Assembly.Load(new AssemblyName("dtSearchNetApi4")); } catch { }
            try { return Assembly.Load(new AssemblyName("dtSearchNetStdApi")); } catch { }
            return null;
        }

        // -------------------- Reflection utilities (with diagnostics) --------------------
        private static bool TrySet(object obj, string propName, object value)
        {
            if (obj == null) return false;
            var t = obj.GetType();
            var p = t.GetProperty(propName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
            if (p == null || !p.CanWrite)
            {
                if (DiagnosticsEnabled)
                {
                    var props = string.Join(", ", t.GetProperties(BindingFlags.Instance | BindingFlags.Public).Select(x => x.Name));
                    DtLog.Info($"TrySet failed: {t.FullName}.{propName} not found or read-only. Available props: {props}");
                }
                return false;
            }
            try
            {
                object val = value;
                if (value != null && !p.PropertyType.IsAssignableFrom(value.GetType()))
                {
                    if (p.PropertyType == typeof(int) && value is bool b) val = b ? 1 : 0;
                    else if (p.PropertyType == typeof(bool) && value is int i) val = i != 0;
                    else if (p.PropertyType.IsEnum && value is string s) val = Enum.Parse(p.PropertyType, s, ignoreCase: true);
                    else val = Convert.ChangeType(value, p.PropertyType);
                }
                p.SetValue(obj, val);
                return true;
            }
            catch (Exception ex)
            {
                if (DiagnosticsEnabled) DtLog.Info($"TrySet threw: {t.FullName}.{propName} -> {ex.Message}");
                return false;
            }
        }

        private static object Get(object obj, string propName)
        {
            if (obj == null) return null;
            var p = obj.GetType().GetProperty(propName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
            return p != null ? p.GetValue(obj) : null;
        }

        private static bool TryCall(object obj, string methodName, params object[] args)
        {
            if (obj == null) return false;
            var t = obj.GetType();
            var m = t.GetMethod(methodName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
            if (m == null)
            {
                if (DiagnosticsEnabled)
                {
                    var methods = string.Join(", ", t.GetMethods(BindingFlags.Instance | BindingFlags.Public)
                                             .Where(mi => !mi.IsSpecialName).Select(mi => mi.Name));
                    DtLog.Info($"TryCall failed: {t.FullName}.{methodName} not found. Available methods: {methods}");
                }
                return false;
            }
            try { m.Invoke(obj, args); return true; }
            catch (Exception ex)
            {
                if (DiagnosticsEnabled) DtLog.Info($"TryCall threw: {t.FullName}.{methodName} -> {ex.Message}");
                return false;
            }
        }

        private static object TryCallRet(object obj, string methodName, params object[] args)
        {
            if (obj == null) return null;
            var t = obj.GetType();
            var m = t.GetMethod(methodName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
            if (m == null)
            {
                if (DiagnosticsEnabled)
                {
                    var methods = string.Join(", ", t.GetMethods(BindingFlags.Instance | BindingFlags.Public)
                                             .Where(mi => !mi.IsSpecialName).Select(mi => mi.Name));
                    DtLog.Info($"TryCallRet failed: {t.FullName}.{methodName} not found. Available methods: {methods}");
                }
                return null;
            }
            try { return m.Invoke(obj, args); }
            catch (Exception ex)
            {
                if (DiagnosticsEnabled) DtLog.Info($"TryCallRet threw: {t.FullName}.{methodName} -> {ex.Message}");
                return null;
            }
        }

        private static (bool ok, int rc) TryExecute(object job)
        {
            foreach (var name in new[] { "Execute", "Run", "DoExecute", "DoJob", "Perform", "Start" })
            {
                var m = job.GetType().GetMethod(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
                if (m == null) continue;
                try
                {
                    var ret = m.Invoke(job, null);
                    if (ret is int i) return (true, i);
                    if (ret is bool b) return (true, b ? 0 : -1);
                    return (true, 0);
                }
                catch (Exception ex)
                {
                    if (DiagnosticsEnabled) DtLog.Info($"Execute threw via {name}: {ex.Message}");
                    return (false, -1);
                }
            }
            if (DiagnosticsEnabled) DtLog.Info("No execute-like method found (Execute/Run/DoExecute/DoJob/Perform/Start).");
            return (false, -1);
        }

        private void Warn(string msg) { LastWarning = msg; if (DiagnosticsEnabled) DtLog.Info("WARN: " + msg); }
        private static bool NameContainsAny(string name, params string[] needles)
        {
            foreach (var n in needles) if (name.IndexOf(n, StringComparison.OrdinalIgnoreCase) >= 0) return true;
            return false;
        }

        // -------------------- Engine safety/options (reflection) --------------------
        private static void EnsureEngineOptions()
        {
            try
            {
                var asm = GetDtSearchAssembly();
                if (asm == null) return;

                var engineType = asm.GetType("dtSearch.Engine.Engine");
                var setOption = engineType?.GetMethod("SetOption", BindingFlags.Public | BindingFlags.Static);
                if (setOption == null) return;

                var optionsEnum = asm.GetType("dtSearch.Engine.EngineOptions");
                if (optionsEnum != null && optionsEnum.IsEnum)
                {
                    object Parse(string name)
                    {
                        foreach (var n in Enum.GetNames(optionsEnum))
                            if (string.Equals(n, name, StringComparison.OrdinalIgnoreCase))
                                return Enum.Parse(optionsEnum, n);
                        return null;
                    }

                    var noMsg = Parse("NoExceptionMessageBox");
                    if (noMsg != null) setOption.Invoke(null, new object[] { noMsg, true });

                    var threads = Parse("Threads");
                    if (threads != null)
                    {
                        var threadCount = Math.Max(2, Environment.ProcessorCount / 2);
                        setOption.Invoke(null, new object[] { threads, threadCount });
                    }
                }
            }
            catch { /* ignore */ }
        }

        // -------------------- Optional engine path helpers (reflection) --------------------
        private static bool InvokeStaticOnAnyTypeInDtSearchAssembly(string methodName, params object[] args)
        {
            try
            {
                var asm = GetDtSearchAssembly();
                if (asm == null) return false;

                foreach (var t in asm.GetExportedTypes())
                {
                    var m = t.GetMethod(methodName,
                        BindingFlags.Public | BindingFlags.Static | BindingFlags.IgnoreCase,
                        binder: null,
                        types: args.Select(a => a?.GetType() ?? typeof(object)).ToArray(),
                        modifiers: null);
                    if (m != null)
                    {
                        m.Invoke(null, args);
                        return true;
                    }
                }
            }
            catch { /* ignore */ }
            return false;
        }

        public bool SetEnginePath(string engineDir)
        {
            LastError = null;
            try
            {
                if (string.IsNullOrWhiteSpace(engineDir)) { LastError = "engineDir is empty."; return false; }
                if (!Directory.Exists(engineDir)) Warn($"Engine directory does not exist: {engineDir}");

                var ok = InvokeStaticOnAnyTypeInDtSearchAssembly("SetEnginePath", engineDir);
                if (!ok) Warn("SetEnginePath not found in this dtSearch build (safe to ignore).");
                return true;
            }
            catch (Exception ex) { LastError = ex.ToString(); return false; }
        }

        public bool SetHomeDirXml(string homedirXmlPath)
        {
            LastError = null;
            try
            {
                if (string.IsNullOrWhiteSpace(homedirXmlPath)) { LastError = "homedirXmlPath is empty."; return false; }
                if (!File.Exists(homedirXmlPath)) { LastError = $"homedir.xml not found: {homedirXmlPath}"; return false; }

                var ok = InvokeStaticOnAnyTypeInDtSearchAssembly("SetResourceFilePath", homedirXmlPath);
                if (!ok)
                {
                    var dir = Path.GetDirectoryName(homedirXmlPath) ?? "";
                    ok = InvokeStaticOnAnyTypeInDtSearchAssembly("SetHomeDir", dir);
                }
                if (!ok) Warn("Neither SetResourceFilePath nor SetHomeDir found (likely unnecessary if homedir.xml is colocated).");
                return true;
            }
            catch (Exception ex) { LastError = ex.ToString(); return false; }
        }

        // -------------------- FILE LIST HELPERS --------------------
        private static IEnumerable<string> ExpandFiles(string folder, string[] patterns)
        {
            foreach (var pat in patterns)
            {
                IEnumerable<string> files = Enumerable.Empty<string>();
                try { files = Directory.EnumerateFiles(folder, pat, SearchOption.AllDirectories); }
                catch { /* ignore access errors */ }

                foreach (var f in files)
                {
                    string full;
                    try { full = Path.GetFullPath(f); } catch { continue; }
                    yield return full;
                }
            }
        }

        private static string WriteTempFileList(IEnumerable<string> files)
        {
            var listPath = Path.Combine(Path.GetTempPath(), "dt_add_" + Guid.NewGuid().ToString("N") + ".lst");
            File.WriteAllLines(listPath, files);
            return listPath;
        }

        private static bool IndexLooksValid(string indexPath)
        {
            try
            {
                if (!Directory.Exists(indexPath)) return false;
                return Directory.GetFiles(indexPath, "*.ix").Length > 0;
            }
            catch { return false; }
        }

        // -------------------- INDEXING --------------------
        public bool RunIndexSingle(string indexDirectory, string folderToIndex)
            => RunIndex(indexDirectory, new string[] { folderToIndex });

        public bool RunIndex(string indexPath, string folder)
            => RunIndex(indexPath, new string[] { folder });

        public bool RunIndex(string indexPath, string[] folders)
        {
            LastError = null;
            EnsureEngineOptions();

            try
            {
                if (string.IsNullOrWhiteSpace(indexPath)) { LastError = "indexPath is empty."; return false; }
                if (folders == null || folders.Length == 0) { LastError = "folders is empty."; return false; }

                var parent = Path.GetDirectoryName(indexPath.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
                if (!string.IsNullOrEmpty(parent) && !Directory.Exists(parent))
                    Directory.CreateDirectory(parent);

                var pats = new[] { "*.txt", "*.pdf", "*.htm", "*.html", "*.doc", "*.docx" };
                var all = new List<string>();
                foreach (var f in folders.Where(s => !string.IsNullOrWhiteSpace(s)))
                    if (Directory.Exists(f)) all.AddRange(ExpandFiles(f, pats));

                if (all.Count == 0) { LastError = "No files found to index in provided folder(s)."; return false; }

                var listPath = WriteTempFileList(all);

                try
                {
                    var job = Create("dtSearch.Engine.IndexJob");
                    if (job == null) { LastError = "Failed to create dtSearch.Engine.IndexJob (assembly not loaded?)."; return false; }

                    if (!TrySet(job, "IndexPath", indexPath))
                    { LastError = "IndexJob.IndexPath property not found."; return false; }

                    bool create = !IndexLooksValid(indexPath);
                    TrySet(job, "ActionCreate", create);
                    TrySet(job, "ActionAdd", true);
                    TrySet(job, "ActionRemoveDeleted", true);
                    TrySet(job, "CreateRelativePaths", false);

                    TrySet(job, "ToAddFileListName", listPath);
                    TrySet(job, "TempFileDir", Path.GetTempPath());

                    var (ok, rc) = TryExecute(job);
                    if (!ok || rc != 0) { LastError = $"IndexJob.Execute failed (rc={rc})."; return false; }
                }
                finally
                {
                    try { if (File.Exists(listPath)) File.Delete(listPath); } catch { /* ignore */ }
                }

                if (!IndexLooksValid(indexPath))
                { LastError = "Indexing completed but no index structure (*.ix) found."; return false; }

                return true;
            }
            catch (Exception ex) { LastError = ex.ToString(); return false; }
        }

        public bool RunIndexAddSingleFile(string indexPath, string filePath, bool rebuild = false)
        {
            LastError = null;
            EnsureEngineOptions();

            try
            {
                if (string.IsNullOrWhiteSpace(indexPath)) { LastError = "indexPath is empty."; return false; }
                if (string.IsNullOrWhiteSpace(filePath) || !File.Exists(filePath))
                { LastError = $"filePath not found: {filePath}"; return false; }

                var parent = Path.GetDirectoryName(indexPath.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
                if (!string.IsNullOrEmpty(parent) && !Directory.Exists(parent))
                    Directory.CreateDirectory(parent);

                var listPath = WriteTempFileList(new[] { Path.GetFullPath(filePath) });

                try
                {
                    var job = Create("dtSearch.Engine.IndexJob");
                    if (job == null) { LastError = "Failed to create dtSearch.Engine.IndexJob."; return false; }

                    if (!TrySet(job, "IndexPath", indexPath))
                    { LastError = "IndexJob.IndexPath property not found."; return false; }

                    bool create = rebuild || !IndexLooksValid(indexPath);
                    TrySet(job, "ActionCreate", create);
                    TrySet(job, "ActionAdd", true);
                    TrySet(job, "ActionRemoveDeleted", true);
                    TrySet(job, "CreateRelativePaths", false);

                    TrySet(job, "ToAddFileListName", listPath);
                    TrySet(job, "TempFileDir", Path.GetTempPath());

                    var (ok, rc) = TryExecute(job);
                    if (!ok || rc != 0) { LastError = $"IndexJob.Execute failed (rc={rc})."; return false; }
                }
                finally
                {
                    try { if (File.Exists(listPath)) File.Delete(listPath); } catch { /* ignore */ }
                }

                if (!IndexLooksValid(indexPath))
                { LastError = "Indexing completed but no index structure (*.ix) found."; return false; }

                return true;
            }
            catch (Exception ex) { LastError = ex.ToString(); return false; }
        }

        // -------------------- SEARCHING --------------------
        public List<SearchResultDto> SearchText(string indexPath, string query) =>
            SearchCore(new[] { indexPath }, query, new SearchOptions { TopK = 20 });

        public List<SearchResultDto> SearchTextSingleTopK(string indexDirectory, string query, int topK)
        {
            if (topK <= 0) topK = 20;
            return SearchCore(new[] { indexDirectory }, query, new SearchOptions { TopK = topK });
        }

        public List<SearchResultDto> SearchText(IEnumerable<string> indexPaths, string query, int topK = 20)
            => SearchCore(indexPaths?.ToArray() ?? Array.Empty<string>(), query, new SearchOptions { TopK = topK });

        private static object TryGetIndexedItem(object collection, int i)
        {
            object item = TryCallRet(collection, "get_Item", i);
            if (item != null) return item;

            foreach (var name in new[] { "GetNthDoc", "GetNthDocument", "GetResult", "GetItem", "Get", "GetNthItem" })
            {
                item = TryCallRet(collection, name, i);
                if (item != null) return item;
            }
            return null;
        }

        private List<SearchResultDto> SearchCore(string[] indexPaths, string query, SearchOptions opts)
        {
            LastError = null;
            EnsureEngineOptions();

            var outList = new List<SearchResultDto>();
            System.Diagnostics.Stopwatch _sw = null;
            if (DiagnosticsEnabled) _sw = System.Diagnostics.Stopwatch.StartNew();

            try
            {
                if (indexPaths == null || indexPaths.Length == 0) { LastError = "No indexPaths provided."; return outList; }
                if (string.IsNullOrWhiteSpace(query)) { LastError = "Query is empty."; return outList; }

                var sj = Create("dtSearch.Engine.SearchJob");
                if (sj == null) { LastError = "Failed to create dtSearch.Engine.SearchJob (assembly not loaded?)."; return outList; }

                bool anyIndex = false;
                var idxList = Get(sj, "IndexesToSearch") as StringCollection;
                if (idxList != null)
                {
                    foreach (var ip in indexPaths.Where(s => !string.IsNullOrWhiteSpace(s))) { idxList.Add(ip); anyIndex = true; }
                }
                else
                {
                    foreach (var ip in indexPaths.Where(s => !string.IsNullOrWhiteSpace(s)))
                        anyIndex |= TryCall(sj, "AddIndex", ip);
                    if (!anyIndex && indexPaths.Length == 1)
                    {
                        anyIndex |= TrySet(sj, "IndexToSearch", indexPaths[0]);
                        anyIndex |= TrySet(sj, "SearchIndex", indexPaths[0]);
                    }
                }
                if (!anyIndex) { LastError = "Could not attach index path(s) to SearchJob."; return outList; }

                if (!TrySet(sj, "Request", query))
                {
                    if (!TryCall(sj, "SetRequest", query))
                    { LastError = "Could not set query on SearchJob."; return outList; }
                }

                // merge defaults for unspecified options
                opts ??= new SearchOptions();
                if (!opts.TimeoutMs.HasValue)       opts.TimeoutMs       = _defaults.TimeoutMs;
                if (!opts.UseStemming.HasValue)     opts.UseStemming     = _defaults.UseStemming;
                if (!opts.CaseSensitive.HasValue)   opts.CaseSensitive   = _defaults.CaseSensitive;
                if (!opts.AccentSensitive.HasValue) opts.AccentSensitive = _defaults.AccentSensitive;
                if (!opts.MaxContextBytes.HasValue) opts.MaxContextBytes = _defaults.MaxContextBytes;
                if (string.IsNullOrWhiteSpace(opts.SearchFlags) && !string.IsNullOrWhiteSpace(_defaults.SearchFlags))
                    opts.SearchFlags = _defaults.SearchFlags;

                if (DiagnosticsEnabled)
                {
                    DtLog.Info($"SearchCore: indexes={string.Join(";", indexPaths)} | query=\"{query}\" | TopK={opts.TopK} | " +
                               $"TimeoutMs={opts.TimeoutMs} | Stemming={opts.UseStemming} | Case={opts.CaseSensitive} | " +
                               $"Accent={opts.AccentSensitive} | MaxCtx={opts.MaxContextBytes} | Flags={opts.SearchFlags ?? "(null)"}");
                }

                // apply options
                TrySet(sj, "MaxFilesToRetrieve", opts.TopK);
                TrySet(sj, "MaxDocumentsToRetrieve", opts.TopK);
                if (opts.MaxContextBytes.HasValue)
                {
                    TrySet(sj, "MaxContextBytes", opts.MaxContextBytes.Value);
                    TrySet(sj, "MaxContext",      opts.MaxContextBytes.Value);
                }
                if (opts.TimeoutMs.HasValue)       TrySet(sj, "TimeoutMilliseconds", opts.TimeoutMs.Value);
                if (opts.UseStemming.HasValue)     TrySet(sj, "Stemming",            opts.UseStemming.Value);
                if (opts.CaseSensitive.HasValue)   TrySet(sj, "CaseSensitive",       opts.CaseSensitive.Value);
                if (opts.AccentSensitive.HasValue) TrySet(sj, "AccentSensitive",     opts.AccentSensitive.Value);
                if (!string.IsNullOrWhiteSpace(opts.SearchFlags)) TrySet(sj, "SearchFlags", opts.SearchFlags);

                var (ok, rc) = TryExecute(sj);
                if (!ok || rc != 0) { LastError = $"SearchJob.Execute failed (rc={rc})."; return outList; }

                object raw = Get(sj, "Results") ?? Get(sj, "SearchResults");

                if (raw is System.Collections.IEnumerable enumerable)
                {
                    foreach (var r in enumerable)
                    {
                        AppendHit(outList, r);
                        if (outList.Count >= opts.TopK) break;
                    }

                    if (DiagnosticsEnabled)
                    {
                        DtLog.Info($"SearchCore: returned {outList.Count} results (TopK={opts.TopK}).");
                        if (_sw != null) { _sw.Stop(); DtLog.Info($"SearchCore: elapsed {_sw.ElapsedMilliseconds} ms."); }
                    }
                    return outList;
                }

                if (raw != null)
                {
                    int count = 0;
                    object countObj = Get(raw, "Count") ?? Get(raw, "HitCount") ?? Get(raw, "NumResults") ?? Get(raw, "Length");
                    if (countObj is int ci) count = ci;
                    if (count > 0)
                    {
                        for (int i = 0; i < count && outList.Count < opts.TopK; i++)
                        {
                            var item = TryGetIndexedItem(raw, i);
                            if (item != null) AppendHit(outList, item);
                        }
                    }
                }

                if (DiagnosticsEnabled)
                {
                    DtLog.Info($"SearchCore: returned {outList.Count} results (TopK={opts.TopK}).");
                    if (_sw != null) { _sw.Stop(); DtLog.Info($"SearchCore: elapsed {_sw.ElapsedMilliseconds} ms."); }
                }
                return outList;
            }
            catch (Exception ex)
            {
                LastError = ex.ToString();
                if (DiagnosticsEnabled && _sw != null) { _sw.Stop(); DtLog.Info($"SearchCore: EXCEPTION after {_sw.ElapsedMilliseconds} ms: {ex.Message}"); }
                return outList;
            }
        }

        private static void AppendHit(List<SearchResultDto> outList, object r)
        {
            string filename =
                (Get(r, "FilePath") as string) ??
                (Get(r, "Filename") as string) ??
                (Get(r, "FileName") as string) ??
                (Get(r, "DocPath") as string) ?? "";

            int page = 0; var pageObj = Get(r, "PageNumber") ?? Get(r, "Page");
            if (pageObj is int p) page = p;

            string snippet =
                (Get(r, "Summary") as string) ??
                (Get(r, "Context") as string) ??
                (Get(r, "Snippet") as string) ??
                (Get(r, "DocSummary") as string) ?? "";

            if (string.IsNullOrWhiteSpace(snippet))
            {
                var rep = Get(r, "Report") as string ?? Get(r, "Highlights") as string;
                if (!string.IsNullOrWhiteSpace(rep)) snippet = rep;
            }

            snippet = NormalizeSnippet(snippet);

            double score = 0; var sc = Get(r, "Score");
            if (sc is int si) score = si; else if (sc is double sd) score = sd;

            int hitCount = 0; var hc = Get(r, "HitCount");
            if (hc is int hci) hitCount = hci;

            string title =
                (Get(r, "Title") as string) ??
                (Get(r, "DocTitle") as string) ??
                Path.GetFileName(filename);

            outList.Add(new SearchResultDto
            {
                FilePath = filename ?? "",
                Page = page,
                Snippet = snippet ?? "",
                Score = score,
                Title = title ?? "",
                HitCount = hitCount
            });
        }

        private static string NormalizeSnippet(string s, int maxLen = 600)
        {
            if (string.IsNullOrEmpty(s)) return "";
            var t = s.Replace("\r", " ").Replace("\n", " ").Trim();
            return t.Length <= maxLen ? t : t.Substring(0, maxLen) + " …";
        }

        // -------------------- DEBUG HELPERS --------------------
        public string GetIndexInfo(string indexPath)
        {
            try
            {
                var abs = Path.GetFullPath(indexPath);
                var ij = Create("dtSearch.Engine.IndexJob");
                if (ij == null) return "GetIndexInfo ERROR: cannot create IndexJob.";

                var info = TryCallRet(ij, "GetIndexInfo", abs);
                if (info == null) return $"GetIndexInfo: no info for {abs}";

                var sb = new StringBuilder();
                sb.AppendLine($"IndexInfo for {abs}");
                sb.AppendLine($"DocCount={Get(info, "DocCount")}");
                sb.AppendLine($"WordCount={Get(info, "WordCount")}");
                sb.AppendLine($"IndexSize={Get(info, "IndexSize")}");
                sb.AppendLine($"UpdatedDate={Get(info, "UpdatedDate")}");
                sb.AppendLine($"CreatedDate={Get(info, "CreatedDate")}");
                sb.AppendLine($"StructureVersion={Get(info, "StructureVersion")}");
                return sb.ToString();
            }
            catch (Exception ex) { return "GetIndexInfo ERROR: " + ex; }
        }

        public string DebugIndexJobMembers()
        {
            try
            {
                var job = Create("dtSearch.Engine.IndexJob");
                if (job == null) return "Could not instantiate dtSearch.Engine.IndexJob.";
                var t = job.GetType();

                var sb = new StringBuilder();
                sb.AppendLine("=== IndexJob Properties (public instance) ===");
                foreach (var p in t.GetProperties(BindingFlags.Instance | BindingFlags.Public).OrderBy(p => p.Name))
                {
                    if (NameContainsAny(p.Name, "File", "Input", "Document", "Item", "Folder", "Directory", "Path", "Include", "Create", "Temp", "Action"))
                    {
                        sb.AppendLine($"{p.PropertyType.FullName} {p.Name} (CanWrite={p.CanWrite})");
                    }
                }

                sb.AppendLine("=== IndexJob Methods (public instance, 0-1 string param) ===");
                foreach (var m in t.GetMethods(BindingFlags.Instance | BindingFlags.Public).OrderBy(m => m.Name))
                {
                    var ps = m.GetParameters();
                    if (ps.Length <= 1 && (ps.Length == 0 || ps[0].ParameterType == typeof(string)))
                    {
                        if (NameContainsAny(m.Name, "Add", "Set", "Folder", "Directory", "File", "Input", "Document", "Path", "Action", "Create"))
                        {
                            string sig = ps.Length == 0 ? "()" : "(string)";
                            sb.AppendLine($"{m.Name}{sig}");
                        }
                    }
                }

                return sb.ToString();
            }
            catch (Exception ex) { return "DebugIndexJobMembers error: " + ex; }
        }

        public string SearchDebug(string indexPath, string request)
        {
            try
            {
                var abs = Path.GetFullPath(indexPath);
                var sb = new StringBuilder();
                sb.AppendLine($"INDEX USED: {abs}");
                sb.AppendLine($"REQUEST: {request}");
                sb.AppendLine($"IndexLooksValid={IndexLooksValid(abs)}");

                var sj = Create("dtSearch.Engine.SearchJob");
                if (sj == null) return "SearchDebug ERROR: cannot create SearchJob.";

                var idxList = Get(sj, "IndexesToSearch") as StringCollection;
                if (idxList != null) idxList.Add(abs);
                else TryCall(sj, "AddIndex", abs);

                TrySet(sj, "Request", request);
                TrySet(sj, "MaxFilesToRetrieve", 50);

                var (ok, rc) = TryExecute(sj);
                sb.AppendLine($"Execute ok={ok} rc={rc}");

                var raw = Get(sj, "Results") ?? Get(sj, "SearchResults");
                int count = 0;
                if (raw != null)
                {
                    var cObj = Get(raw, "Count") ?? Get(raw, "HitCount") ?? Get(raw, "NumResults") ?? Get(raw, "Length");
                    if (cObj is int ci) count = ci;
                }
                sb.AppendLine($"RESULTS: {count}");
                return sb.ToString();
            }
            catch (Exception ex) { return "SearchDebug ERROR: " + ex; }
        }
    }
}
