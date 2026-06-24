using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using FabricDaxLoadTest;

namespace FabricDaxLoadTest;

class Program
{
    // Set once when the access token is resolved, so EmitErrorEnvelope /
    // LogCallback can scrub it out of any string that gets shipped to
    // stdout JSON or stderr. Connection strings include `password=<token>`
    // and ADOMD has historically been willing to put connection strings
    // into exception messages, so blind ex.ToString() is unsafe.
    static string? _currentToken;
    internal static string Redact(string s)
    {
        if (string.IsNullOrEmpty(s)) return s;
        var t = _currentToken;
        if (!string.IsNullOrEmpty(t) && s.Contains(t!, StringComparison.Ordinal))
            s = s.Replace(t!, "***REDACTED-TOKEN***", StringComparison.Ordinal);
        // Defence in depth: also kill any literal `password=...;` or
        // `password=...<eol>` fragment, in case ADOMD/AS surfaces a
        // connection-string clone we haven't anticipated.
        s = System.Text.RegularExpressions.Regex.Replace(s,
            @"(?i)password\s*=\s*[^;\r\n]*",
            "password=***REDACTED***");
        return s;
    }

    static int Main(string[] args)
    {
        var xmlaOption = new Option<string>("--xmla", "XMLA endpoint (e.g. powerbi://api.powerbi.com/v1.0/myorg/Workspace)") { IsRequired = true };
        var datasetOption = new Option<string>("--dataset", "Semantic model name") { IsRequired = true };
        var durationOption = new Option<int>("--duration", () => 60, "Test duration in seconds");
        var usersOption = new Option<int>("--users", () => 100, "Number of concurrent simulated users");
        var concurrentQueriesPerUserOption = new Option<int>(
            name: "--concurrent-queries-per-user",
            getDefaultValue: () => 1,
            description: "Concurrent in-flight queries per virtual user. Each user runs a rolling drain over " +
                         "the iteration's queries — when one finishes, the next pending query is dispatched on " +
                         "the freed connection (Power BI Desktop-style; not batched all-finish-then-fire-next). " +
                         "1 = strictly serial.");
        var pauseIterOption = new Option<int>("--pause-iterations", () => 10000, "Pause between iterations (ms)");
        var pauseQueryOption = new Option<int>("--pause-queries", () => 0, "Pause between queries (ms)");
        var rampOption = new Option<int>("--ramp-time", () => 30, "User ramp-up time (seconds)");
        var replicaOption = new Option<string>("--replica", () => "", "Target replica ('readonly' for scale-out read replica, or '')");
        var queriesFileOption = new Option<FileInfo>("--queries-file", "Path to queries.json") { IsRequired = true };
        var usersFileOption = new Option<FileInfo>("--users-file", "Path to users.json") { IsRequired = true };
        var logDirOption = new Option<string>("--log-dir", () => "./logs", "Directory for telemetry CSV logs");
        var logFileOption = new Option<string>("--log-file", () => "", "Log filename (auto-generated if empty)");
        var tokenOption = new Option<string?>("--token", "Access token (prefer --token-file or $PBI_TOKEN)");
        var tokenFileOption = new Option<FileInfo?>("--token-file", "Path to file containing access token");
        var skipResultsOption = new Option<bool>("--skip-results", () => false, "Drain response without parsing rows");
        var noAuthOption = new Option<bool>("--no-auth", () => false,
            "Connect with integrated/Windows auth instead of a bearer token. Intended for local SSAS or " +
            "Power BI Desktop smoke tests; not for Fabric/Power BI Service which require a token.");
        var jsonProgressOption = new Option<bool>("--json-progress", () => false,
            "Emit JSONL progress on stdout (one envelope per line) and route diagnostics + run logs to stderr. " +
            "Designed for programmatic callers (notebook subprocess); the human-readable mode stays the default.");
        var errorPolicyOption = new Option<string>("--error-policy", () => "continue",
            "How to handle per-query errors. 'continue' (default): record and keep running so the run reports an error rate. " +
            "'abort': throw on the first per-query error and fail the run. Infrastructure failures always abort regardless.");
        var noTraceOption = new Option<bool>("--no-trace", () => false,
            "Disable XMLA trace subscription. By default, the run subscribes to engine trace events " +
            "(QueryEnd, ExecutionMetrics, VertiPaq SE) for the dataset and writes them to a *.trace.csv. " +
            "Use --no-trace if the principal lacks Build/Read trace permissions or the run is sensitive.");

        var rootCommand = new RootCommand("LoadGen — DAX load test runner for Power BI / Fabric semantic models")
        {
            xmlaOption, datasetOption, durationOption, usersOption,
            concurrentQueriesPerUserOption, pauseIterOption, pauseQueryOption,
            rampOption, replicaOption, queriesFileOption, usersFileOption,
            logDirOption, logFileOption, tokenOption, tokenFileOption,
            skipResultsOption, noAuthOption, jsonProgressOption,
            errorPolicyOption, noTraceOption,
        };

        rootCommand.SetHandler((InvocationContext ctx) =>
        {
            var xmla = ctx.ParseResult.GetValueForOption(xmlaOption)!;
            var dataset = ctx.ParseResult.GetValueForOption(datasetOption)!;
            var duration = ctx.ParseResult.GetValueForOption(durationOption);
            var userCount = ctx.ParseResult.GetValueForOption(usersOption);
            var concurrentQueriesPerUser = ctx.ParseResult.GetValueForOption(concurrentQueriesPerUserOption);
            var pauseIter = ctx.ParseResult.GetValueForOption(pauseIterOption);
            var pauseQuery = ctx.ParseResult.GetValueForOption(pauseQueryOption);
            var rampTime = ctx.ParseResult.GetValueForOption(rampOption);
            var replica = ctx.ParseResult.GetValueForOption(replicaOption)!;
            var queriesFile = ctx.ParseResult.GetValueForOption(queriesFileOption)!;
            var usersFile = ctx.ParseResult.GetValueForOption(usersFileOption)!;
            var logDir = ctx.ParseResult.GetValueForOption(logDirOption)!;
            var logFile = ctx.ParseResult.GetValueForOption(logFileOption)!;
            var tokenDirect = ctx.ParseResult.GetValueForOption(tokenOption);
            var tokenFile = ctx.ParseResult.GetValueForOption(tokenFileOption);
            var skipResults = ctx.ParseResult.GetValueForOption(skipResultsOption);
            var noAuth = ctx.ParseResult.GetValueForOption(noAuthOption);
            var jsonProgress = ctx.ParseResult.GetValueForOption(jsonProgressOption);
            var errorPolicyStr = (ctx.ParseResult.GetValueForOption(errorPolicyOption) ?? "continue").Trim().ToLowerInvariant();
            var errorPolicy = errorPolicyStr switch
            {
                "abort" => ErrorPolicy.Abort,
                "continue" or "" => ErrorPolicy.Continue,
                _ => throw new ArgumentException($"--error-policy must be 'continue' or 'abort', got '{errorPolicyStr}'"),
            };
            var noTrace = ctx.ParseResult.GetValueForOption(noTraceOption);

            ctx.ExitCode = RunOuter(xmla, dataset, duration, userCount, concurrentQueriesPerUser,
                pauseIter, pauseQuery, rampTime, replica, queriesFile, usersFile,
                logDir, logFile, tokenDirect, tokenFile, skipResults, noAuth, jsonProgress,
                errorPolicy, enableTracing: !noTrace);
        });

        return rootCommand.Invoke(args);
    }

    // Wraps Run() with a guaranteed-terminal-envelope try/catch in JSON
    // mode. Without this, anything that throws BEFORE the existing
    // try/catches inside RunJsonProgress (e.g. Directory.CreateDirectory,
    // ResolveToken, file I/O, even a malformed Run argument) would
    // produce a non-zero exit and a stderr trail with no terminal `error`
    // envelope on stdout — so the notebook subprocess reader would see
    // the stream EOF without a result|error envelope and have to guess.
    static int RunOuter(string xmla, string dataset, int duration, int userCount,
        int concurrentQueriesPerUser, int pauseIter, int pauseQuery, int rampTime,
        string replica, FileInfo queriesFile, FileInfo usersFile,
        string logDir, string logFile, string? tokenDirect, FileInfo? tokenFile,
        bool skipResults, bool noAuth, bool jsonProgress, ErrorPolicy errorPolicy,
        bool enableTracing)
    {
        try
        {
            return Run(xmla, dataset, duration, userCount, concurrentQueriesPerUser,
                pauseIter, pauseQuery, rampTime, replica, queriesFile, usersFile,
                logDir, logFile, tokenDirect, tokenFile, skipResults, noAuth, jsonProgress,
                errorPolicy, enableTracing);
        }
        catch (Exception ex)
        {
            if (jsonProgress)
            {
                EmitErrorEnvelope("fatal", ex);
            }
            else
            {
                Console.Error.WriteLine($"FATAL: {Redact(ex.ToString())}");
            }
            return 1;
        }
    }

    // In --json-progress mode, *every* line written to the LoadGen
    // stdout stream is a JSON envelope the caller will parse line-by-line.
    // Banner, info messages, and QueryRunner log echoes all go to stderr
    // so they never collide with the JSON protocol. The notebook drains
    // stderr concurrently and surfaces it on failure.
    static int Run(string xmla, string dataset, int duration, int userCount,
        int concurrentQueriesPerUser, int pauseIter, int pauseQuery, int rampTime,
        string replica, FileInfo queriesFile, FileInfo usersFile,
        string logDir, string logFile, string? tokenDirect, FileInfo? tokenFile,
        bool skipResults, bool noAuth, bool jsonProgress, ErrorPolicy errorPolicy,
        bool enableTracing)
    {
        TextWriter info = jsonProgress ? Console.Error : Console.Out;

        string token;
        if (noAuth)
        {
            token = "";  // QueryRunner.BuildConnectionString omits the password= clause when token is empty.
            info.WriteLine("--no-auth: connecting with integrated auth (no bearer token).");
        }
        else
        {
            var resolved = ResolveToken(tokenDirect, tokenFile, info);
            if (resolved == null)
            {
                EmitError(jsonProgress, "no_token",
                    "No access token provided. Use --token, --token-file, set $PBI_TOKEN, or pass --no-auth for integrated auth.");
                return 1;
            }
            token = resolved;
        }
        // Stash for redaction. Empty string => no-auth mode, no secret to protect.
        _currentToken = string.IsNullOrEmpty(token) ? null : token;

        if (duration > 3000)
            info.WriteLine("Warning: Long duration requested. Token may expire during the test.");

        if (!queriesFile.Exists)
        {
            EmitError(jsonProgress, "queries_file_not_found", $"Queries file not found: {queriesFile.FullName}");
            return 1;
        }

        string[] queries;
        try { queries = ParseQueries(File.ReadAllText(queriesFile.FullName)); }
        catch (Exception ex)
        {
            EmitError(jsonProgress, "queries_parse", $"Error parsing queries file: {ex.Message}");
            return 1;
        }

        if (!usersFile.Exists)
        {
            EmitError(jsonProgress, "users_file_not_found", $"Users file not found: {usersFile.FullName}");
            return 1;
        }

        VirtualUser[] allUsers;
        try { allUsers = ParseUsers(File.ReadAllText(usersFile.FullName)); }
        catch (Exception ex)
        {
            EmitError(jsonProgress, "users_parse", $"Error parsing users file: {ex.Message}");
            return 1;
        }

        if (allUsers.Length == 0)
        {
            EmitError(jsonProgress, "no_users", "users.json contains no users.");
            return 1;
        }

        var users = Enumerable.Range(0, userCount)
            .Select(i => allUsers[i % allUsers.Length])
            .ToArray();

        if (userCount > allUsers.Length)
            info.WriteLine($"Note: Reusing {allUsers.Length} users to fill {userCount} slots.");

        var xmlaEndpoint = !string.IsNullOrEmpty(replica) ? $"{xmla}?{replica}" : xmla;

        if (!jsonProgress)
        {
            info.WriteLine();
            info.WriteLine("═══════════════════════════════════════════════");
            info.WriteLine("  LoadGen — Power BI / Fabric DAX Load Test");
            info.WriteLine("═══════════════════════════════════════════════");
            info.WriteLine($"  Dataset:     {dataset}");
            info.WriteLine($"  Endpoint:    {xmlaEndpoint}");
            info.WriteLine($"  Duration:    {duration}s");
            info.WriteLine($"  Users:       {userCount} (from {allUsers.Length} in users.json)");
            info.WriteLine($"  Queries:     {queries.Length}");
            info.WriteLine($"  Concurrent/user: {concurrentQueriesPerUser}");
            info.WriteLine($"  Pause iter:  {pauseIter}ms");
            info.WriteLine($"  Pause query: {pauseQuery}ms");
            info.WriteLine($"  Ramp time:   {rampTime}s");
            info.WriteLine($"  Replica:     {(string.IsNullOrEmpty(replica) ? "(default)" : replica)}");
            info.WriteLine($"  SkipResults: {skipResults}");
            info.WriteLine($"  ErrorPolicy: {errorPolicy}");
            info.WriteLine($"  Tracing:     {(enableTracing ? "enabled" : "disabled")}");
            info.WriteLine($"  Log dir:     {logDir}");
            info.WriteLine($"  Token:       {token.Length} chars");
            info.WriteLine("═══════════════════════════════════════════════");
            info.WriteLine();
        }
        else
        {
            EmitEnvelope(new Dictionary<string, object?>
            {
                ["type"] = "started",
                ["dataset"] = dataset,
                ["endpoint"] = xmlaEndpoint,
                ["duration"] = duration,
                ["users"] = userCount,
                ["queries"] = queries.Length,
                ["concurrentQueriesPerUser"] = concurrentQueriesPerUser,
                ["rampTime"] = rampTime,
                ["logDir"] = logDir,
                ["skipResults"] = skipResults,
            });
        }

        var emailArr = users.Select(u => u.EffectiveUserName).ToArray();
        var customDataArr = users.Select(u => u.CustomData).ToArray();
        var roleArr = users.Select(u => u.Roles).ToArray();

        Directory.CreateDirectory(logDir);

        return jsonProgress
            ? RunJsonProgress(queries, xmlaEndpoint, dataset, token, emailArr, customDataArr, roleArr,
                duration, concurrentQueriesPerUser, pauseIter, pauseQuery, rampTime,
                logDir, logFile, skipResults, errorPolicy, enableTracing, users)
            : RunHumanReadable(queries, xmlaEndpoint, dataset, token, emailArr, customDataArr, roleArr,
                duration, concurrentQueriesPerUser, pauseIter, pauseQuery, rampTime,
                logDir, logFile, skipResults, errorPolicy, enableTracing, users);
    }

    // Per-virtual-user impersonation tuple. Any field may be empty to mean
    // "do not set this connection-string property for this slot".
    // See docs/impersonation.md.
    internal sealed record VirtualUser(string EffectiveUserName, string CustomData, string Roles);

    // Human-readable mode: stdout banner + log echo + PrintResults summary.
    static int RunHumanReadable(string[] queries, string xmlaEndpoint, string dataset,
        string token, string[] emailArr, string[] customDataArr, string[] roleArr,
        int duration, int concurrentQueriesPerUser, int pauseIter, int pauseQuery, int rampTime,
        string logDir, string logFile, bool skipResults, ErrorPolicy errorPolicy,
        bool enableTracing,
        VirtualUser[] users)
    {
        var config = new LoadTestConfig
        {
            Queries = queries,
            XmlaEndpoint = xmlaEndpoint,
            Dataset = dataset,
            Token = token,
            UserEffectiveNames = emailArr,
            UserCustomData = customDataArr,
            UserRoles = roleArr,
            DurationSeconds = duration,
            ConcurrentQueriesPerUser = concurrentQueriesPerUser,
            PauseBetweenIterationsMs = pauseIter,
            PauseBetweenQueriesMs = pauseQuery,
            LogDirectory = logDir,
            UserRampTimeSec = rampTime,
            LogFileName = logFile,
            SkipResults = skipResults,
            ErrorPolicy = errorPolicy,
            EnableTracing = enableTracing,
            LogCallback = Console.WriteLine,
        };

        string resultJson;
        try
        {
            using var handle = QueryRunner.StartLoadTest(config);
            resultJson = handle.Wait();
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"\nLoad test failed with exception: {ex.Message}");
            if (ex.InnerException != null)
                Console.Error.WriteLine($"  Inner: {ex.InnerException.Message}");
            return 1;
        }

        PrintResults(resultJson, users);
        return 0;
    }

    // JSON-progress mode: stdout is JSONL only, stderr carries banner +
    // log lines. Switches to StartLoadTest so we can wire Ctrl+C to a
    // graceful Cancel() instead of letting the runtime kill the process
    // mid-query and leak ADOMD connections.
    static int RunJsonProgress(string[] queries, string xmlaEndpoint, string dataset,
        string token, string[] emailArr, string[] customDataArr, string[] roleArr,
        int duration, int concurrentQueriesPerUser, int pauseIter, int pauseQuery, int rampTime,
        string logDir, string logFile, bool skipResults, ErrorPolicy errorPolicy,
        bool enableTracing,
        VirtualUser[] users)
    {
        var config = new LoadTestConfig
        {
            Queries = queries,
            XmlaEndpoint = xmlaEndpoint,
            Dataset = dataset,
            Token = token,
            UserEffectiveNames = emailArr,
            UserCustomData = customDataArr,
            UserRoles = roleArr,
            DurationSeconds = duration,
            ConcurrentQueriesPerUser = concurrentQueriesPerUser,
            PauseBetweenIterationsMs = pauseIter,
            PauseBetweenQueriesMs = pauseQuery,
            LogDirectory = logDir,
            UserRampTimeSec = rampTime,
            LogFileName = logFile,
            SkipResults = skipResults,
            ErrorPolicy = errorPolicy,
            EnableTracing = enableTracing,
            // Echo every QueryRunner log line to stderr so notebook
            // diagnostics work even when JSON parsing fails. Redact
            // the token in case any log line embeds a connection string.
            LogCallback = msg => Console.Error.WriteLine(Redact(msg)),
        };

        LoadTestHandle handle;
        try
        {
            handle = QueryRunner.StartLoadTest(config);
        }
        catch (Exception ex)
        {
            EmitErrorEnvelope("start_failed", ex);
            return 1;
        }

        // Now that we have a RunId, emit a second `started` envelope carrying it.
        // The earlier `started` envelope (in Run()) reports parameters but is
        // emitted before the handle exists; clients that need to correlate
        // CSV RunId values to the run wait for the one with `runId` populated.
        EmitEnvelope(new Dictionary<string, object?>
        {
            ["type"] = "started",
            ["runId"] = handle.RunId,
        });

        // Wire SIGINT (and Ctrl+Break) to a single graceful Cancel.
        // CancelKeyPress fires on the .NET thread-pool, so it's safe to
        // call handle.Cancel() — it only sets the CTS, it does not block.
        var sigintReceived = 0;
        ConsoleCancelEventHandler cancelHandler = (s, e) =>
        {
            // Default behaviour is to terminate the process; we want
            // to drain instead.
            e.Cancel = true;
            if (Interlocked.Exchange(ref sigintReceived, 1) == 0)
            {
                Console.Error.WriteLine("LoadGen: SIGINT received — requesting graceful cancel.");
                try { handle.Cancel(); } catch { /* idempotent */ }
            }
        };
        Console.CancelKeyPress += cancelHandler;

        try
        {
            // Snapshot loop. Emit one JSONL "progress" envelope per
            // second until the run completes. Polling LatestSnapshot is
            // lock-free; sleeping 1s gives Steady-phase QPS a chance to
            // accumulate without flooding stdout.
            while (!handle.IsCompleted)
            {
                EmitSnapshot(handle);
                if (handle.IsCompleted) break;
                Thread.Sleep(1000);
            }
            // Final snapshot after IsCompleted=true so callers see the
            // terminal phase + final counters.
            EmitSnapshot(handle);

            string resultJson;
            try
            {
                resultJson = handle.Wait();
            }
            catch (Exception ex)
            {
                EmitErrorEnvelope("run_failed", ex);
                return Volatile.Read(ref sigintReceived) != 0 ? 130 : 1;
            }

            // Persist the full result.json (includes raw executions
            // timeline) next to the .csv/.log files so callers can do
            // detailed analysis. The stdout envelope only carries the
            // summary fields; raw executions can be megabytes.
            var resultPath = Path.Combine(logDir, "result.json");
            try { File.WriteAllText(resultPath, resultJson); } catch { /* best effort */ }

            EmitResultEnvelope(resultJson, resultPath);
            return Volatile.Read(ref sigintReceived) != 0 ? 130 : 0;
        }
        finally
        {
            Console.CancelKeyPress -= cancelHandler;
            try { handle.Dispose(); } catch { }
        }
    }

    static void EmitSnapshot(LoadTestHandle handle)
    {
        var s = handle.LatestSnapshot;
        EmitEnvelope(new Dictionary<string, object?>
        {
            ["type"] = "progress",
            ["elapsed"] = s.Elapsed.TotalSeconds,
            ["phase"] = s.Phase,
            ["activeUsers"] = s.ActiveUsers,
            ["targetUsers"] = s.TargetUsers,
            ["successful"] = s.Successful,
            ["failed"] = s.Failed,
            ["qps"] = Math.Round(s.RollingQps, 2),
            ["inFlight"] = s.InFlight,
            ["durationMsP50"] = Math.Round(s.DurationMsP50, 1),
            ["durationMsP95"] = Math.Round(s.DurationMsP95, 1),
            ["durationMsP99"] = Math.Round(s.DurationMsP99, 1),
            ["durationSamples"] = s.DurationSamples,
        });
    }

    static void EmitResultEnvelope(string fullResultJson, string resultFilePath)
    {
        // Slim the on-stdout envelope: drop `executions[]` (can be
        // very large) but keep summary scalars + duration block.
        using var doc = JsonDocument.Parse(fullResultJson);
        var root = doc.RootElement;
        var summary = new Dictionary<string, object?>();
        foreach (var prop in root.EnumerateObject())
        {
            if (prop.Name == "executions") continue;
            summary[prop.Name] = JsonSerializer.Deserialize<object?>(prop.Value.GetRawText());
        }
        EmitEnvelope(new Dictionary<string, object?>
        {
            ["type"] = "result",
            ["resultFile"] = resultFilePath,
            ["summary"] = summary,
        });
    }

    static void EmitErrorEnvelope(string code, Exception ex)
    {
        EmitEnvelope(new Dictionary<string, object?>
        {
            ["type"] = "error",
            ["code"] = code,
            ["exceptionType"] = ex.GetType().FullName,
            ["message"] = Redact(ex.Message),
            ["exception"] = Redact(ex.ToString()),
        });
    }

    // For pre-StartLoadTest fatal errors (config/file issues): emit a
    // minimal error envelope or print to stderr depending on mode.
    static void EmitError(bool jsonProgress, string code, string message)
    {
        if (jsonProgress)
        {
            EmitEnvelope(new Dictionary<string, object?>
            {
                ["type"] = "error",
                ["code"] = code,
                ["message"] = message,
            });
        }
        else
        {
            Console.Error.WriteLine($"Error: {message}");
        }
    }

    // Single Console.WriteLine call so the line is atomic — important
    // when SIGINT can interleave with the snapshot loop.
    private static readonly object _stdoutLock = new();
    private static readonly JsonSerializerOptions _jsonOpts = new()
    {
        WriteIndented = false,
    };
    static void EmitEnvelope(Dictionary<string, object?> envelope)
    {
        var line = JsonSerializer.Serialize(envelope, _jsonOpts);
        lock (_stdoutLock)
        {
            Console.Out.WriteLine(line);
            Console.Out.Flush();
        }
    }

    // Token must be supplied by the caller (--token / --token-file /
    // $PBI_TOKEN). LoadGen does not mint tokens itself — the notebook
    // gets one via notebookutils.credentials.getToken("pbi"); local
    // smoke gets one via Az CLI in scripts/local_smoke.py. This keeps
    // the Azure.Identity dependency tree out of the staged bundle.
    static string? ResolveToken(string? direct, FileInfo? file, TextWriter info)
    {
        if (!string.IsNullOrWhiteSpace(direct)) return direct.Trim();
        if (file != null && file.Exists) return File.ReadAllText(file.FullName).Trim();
        var envToken = Environment.GetEnvironmentVariable("PBI_TOKEN");
        if (!string.IsNullOrWhiteSpace(envToken)) return envToken.Trim();
        return null;
    }

    // Parse a queries file into a flat list of DAX strings. Accepted shapes
    // (kept in sync with normalize_queries_with_visuals in
    // src/fdlt_runtime/queries.py — the notebook's Python path):
    //   * Power BI Desktop *Performance Analyzer* export
    //     ({"version": ..., "events": [...]}). DAX text is pulled from
    //     metrics.QueryText on "Execute DAX Query" events. Visual metadata
    //     is not retained here (the CLI only needs the DAX); it's used only
    //     to warn about visuals that served from cache.
    //   * Object array: [{"query": "..."}, ...] or [{"Query": "..."}, ...]
    //   * String array: ["EVALUATE ...", ...]
    // Tolerates a leading UTF-8 BOM (Power BI Desktop writes one).
    static string[] ParseQueries(string json)
    {
        using var doc = JsonDocument.Parse(json.TrimStart('\uFEFF'));
        var root = doc.RootElement;

        if (root.ValueKind == JsonValueKind.Object
            && root.TryGetProperty("events", out var events)
            && events.ValueKind == JsonValueKind.Array)
            return ParsePerformanceAnalyzer(events);

        if (root.ValueKind != JsonValueKind.Array)
            throw new InvalidOperationException(
                "Unrecognized queries file shape. Expected a JSON array of query " +
                "strings/objects, or a Power BI Performance Analyzer export " +
                "({\"version\": ..., \"events\": [...]}).");

        return root.EnumerateArray().Select(el =>
        {
            if (el.ValueKind == JsonValueKind.String) return el.GetString()!;
            if (el.TryGetProperty("query", out var q) && q.ValueKind == JsonValueKind.String)
                return q.GetString()!;
            if (el.TryGetProperty("Query", out var q2) && q2.ValueKind == JsonValueKind.String)
                return q2.GetString()!;
            throw new InvalidOperationException(
                "Each query must be a string or an object with a 'query' field.");
        }).ToArray();
    }

    // Extract DAX from a Performance Analyzer events[] array. Mirrors the
    // event walk in normalize_queries_with_visuals (queries.py): each
    // "Execute DAX Query" contributes metrics.QueryText; a "Visual Container
    // Lifecycle" with no following query indicates the visual served from
    // cache, which we surface as a stderr warning so a misleading "no DAX"
    // failure isn't cryptic.
    static string[] ParsePerformanceAnalyzer(JsonElement events)
    {
        var queries = new List<string>();
        int queryNamedEvents = 0;
        int eventCount = 0;
        bool pendingVisual = false;
        string pendingTitle = "", pendingType = "";
        var cachedVisuals = new List<string>();

        foreach (var ev in events.EnumerateArray())
        {
            eventCount++;
            if (ev.ValueKind != JsonValueKind.Object) continue;

            string? name = (ev.TryGetProperty("name", out var n) && n.ValueKind == JsonValueKind.String)
                ? n.GetString() : null;

            if (name == "Visual Container Lifecycle")
            {
                // A prior lifecycle never paired with a query => cache hit.
                if (pendingVisual)
                    cachedVisuals.Add($"{(pendingTitle.Length > 0 ? pendingTitle : "(untitled)")} " +
                                      $"[{(pendingType.Length > 0 ? pendingType : "?")}]");
                pendingVisual = false; pendingTitle = ""; pendingType = "";

                if (ev.TryGetProperty("metrics", out var lm) && lm.ValueKind == JsonValueKind.Object)
                {
                    var vid = FirstString(lm, "visualId", "VisualId");
                    if (!string.IsNullOrEmpty(vid))
                    {
                        pendingVisual = true;
                        pendingTitle = FirstString(lm, "visualTitle", "VisualTitle") ?? "";
                        pendingType  = FirstString(lm, "visualType", "VisualType") ?? "";
                    }
                }
                continue;
            }

            if (name == "Execute DAX Query" || name == "Query")
                queryNamedEvents++;

            string? q = null;
            if (ev.TryGetProperty("metrics", out var m) && m.ValueKind == JsonValueKind.Object)
                q = FirstString(m, "QueryText", "queryText");
            if (string.IsNullOrEmpty(q))
                q = FirstString(ev, "query", "QueryText", "queryText");
            if (string.IsNullOrEmpty(q)
                && ev.TryGetProperty("Query", out var qd) && qd.ValueKind == JsonValueKind.Object)
                q = FirstString(qd, "Query");

            if (!string.IsNullOrWhiteSpace(q))
            {
                queries.Add(q!);
                pendingVisual = false; pendingTitle = ""; pendingType = "";
            }
        }
        if (pendingVisual)
            cachedVisuals.Add($"{(pendingTitle.Length > 0 ? pendingTitle : "(untitled)")} " +
                              $"[{(pendingType.Length > 0 ? pendingType : "?")}]");

        if (cachedVisuals.Count > 0)
        {
            var shown = string.Join(", ", cachedVisuals.Take(5));
            var extra = cachedVisuals.Count <= 5 ? "" : $" (+{cachedVisuals.Count - 5} more)";
            Console.Error.WriteLine(
                $"\u26a0  {cachedVisuals.Count} Visual Container Lifecycle event(s) had no " +
                $"following 'Execute DAX Query' — those visuals likely served from cache: {shown}{extra}.\n" +
                "   To capture DAX for every visual, in Power BI Desktop restart Desktop (or clear " +
                "permissions) to drop the visual cache, then View > Performance Analyzer > Start " +
                "recording > 'Refresh visuals' (not page refresh) > Export. In the Fabric/Service " +
                "portal, hard-reload the report tab (Ctrl+F5) before clicking Refresh visuals.");
        }

        if (queries.Count == 0)
            throw new InvalidOperationException(
                $"Performance Analyzer export contains no DAX query text ({eventCount} events, " +
                $"{queryNamedEvents} Query/Execute DAX Query events, but none had metrics.QueryText). " +
                "Re-record the trace from Power BI Desktop (View > Performance Analyzer > Start " +
                "recording > Refresh visuals > Export), or in the Fabric/Service portal click " +
                "'Refresh visuals' after starting the recording. Some browser/portal modes capture " +
                "only timings and omit the DAX text.");

        return queries.ToArray();
    }

    // First string-valued property among `keys` (in order) on `obj`, else null.
    static string? FirstString(JsonElement obj, params string[] keys)
    {
        foreach (var k in keys)
            if (obj.TryGetProperty(k, out var v) && v.ValueKind == JsonValueKind.String)
            {
                var s = v.GetString();
                if (!string.IsNullOrEmpty(s)) return s;
            }
        return null;
    }

    // Parse users.json. Accepted shapes:
    //   * Object array: [{"effectiveUserName": "...", "customData": "...",
    //                     "roles": "..." | ["..."]}, ...]  (case-insensitive)
    //   * String array: ["alice@..."]  -> EffectiveUserName per slot.
    //   * Empty {} entries are allowed (slot with no impersonation).
    // See docs/impersonation.md.
    static VirtualUser[] ParseUsers(string json)
    {
        using var doc = JsonDocument.Parse(json);
        if (doc.RootElement.ValueKind != JsonValueKind.Array)
            throw new InvalidOperationException("users.json must be a JSON array.");

        return doc.RootElement.EnumerateArray().Select(el =>
        {
            if (el.ValueKind == JsonValueKind.String)
                return new VirtualUser(el.GetString() ?? "", "", "");

            if (el.ValueKind != JsonValueKind.Object)
                throw new InvalidOperationException(
                    $"users.json entries must be strings or objects (got {el.ValueKind}).");

            string get(params string[] keys)
            {
                foreach (var k in keys)
                    foreach (var p in el.EnumerateObject())
                        if (string.Equals(p.Name, k, StringComparison.OrdinalIgnoreCase)
                            && p.Value.ValueKind == JsonValueKind.String)
                            return p.Value.GetString() ?? "";
                return "";
            }

            string roles = get("roles");
            if (string.IsNullOrEmpty(roles))
            {
                // Roles array form: ["R1", "R2"] -> "R1,R2"
                foreach (var p in el.EnumerateObject())
                    if (string.Equals(p.Name, "roles", StringComparison.OrdinalIgnoreCase)
                        && p.Value.ValueKind == JsonValueKind.Array)
                    {
                        roles = string.Join(",",
                            p.Value.EnumerateArray()
                                .Where(x => x.ValueKind == JsonValueKind.String)
                                .Select(x => x.GetString()));
                        break;
                    }
            }

            string eun = get("effectiveUserName", "effectiveusername");
            string cd = get("customData", "customdata");

            return new VirtualUser(eun, cd, roles);
        }).ToArray();
    }

    static void PrintResults(string resultJson, VirtualUser[] users)
    {
        using var doc = JsonDocument.Parse(resultJson);
        var stats = doc.RootElement;

        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════");
        Console.WriteLine("  Load Test Results");
        Console.WriteLine("═══════════════════════════════════════════════");

        var totalMs = stats.GetProperty("totalDurationMs").GetDouble();
        Console.WriteLine($"  Duration:     {totalMs / 1000:F1}s");
        Console.WriteLine($"  Executions:   {stats.GetProperty("totalExecutions").GetInt32()}");
        Console.WriteLine($"  Successful:   {stats.GetProperty("successfulExecutions").GetInt32()}");
        Console.WriteLine($"  Failed:       {stats.GetProperty("failedExecutions").GetInt32()}");
        Console.WriteLine($"  QPS:          {stats.GetProperty("qps").GetDouble()}");
        Console.WriteLine($"  Max iter:     {stats.GetProperty("maxIteration").GetInt32()}");

        if (stats.TryGetProperty("duration", out var dur))
        {
            Console.WriteLine();
            Console.WriteLine("  Duration:");
            Console.WriteLine($"    Min:    {dur.GetProperty("min").GetDouble()}ms");
            Console.WriteLine($"    Median: {dur.GetProperty("median").GetDouble()}ms");
            Console.WriteLine($"    Mean:   {dur.GetProperty("mean").GetDouble()}ms");
            Console.WriteLine($"    P95:    {dur.GetProperty("p95").GetDouble()}ms");
            Console.WriteLine($"    P99:    {dur.GetProperty("p99").GetDouble()}ms");
            Console.WriteLine($"    Max:    {dur.GetProperty("max").GetDouble()}ms");
        }

        Console.WriteLine();
        Console.WriteLine("  Per-user summary:");
        if (stats.TryGetProperty("perUser", out var perUser))
        {
            foreach (var u in perUser.EnumerateArray())
            {
                var idx = u.GetProperty("userIndex").GetInt32();
                var email = idx < users.Length
                    ? (!string.IsNullOrEmpty(users[idx].EffectiveUserName)
                        ? users[idx].EffectiveUserName
                        : !string.IsNullOrEmpty(users[idx].CustomData) ? users[idx].CustomData : $"slot-{idx}")
                    : $"user-{idx}";
                var iters = u.GetProperty("iterations").GetInt32();
                var execs = u.GetProperty("executions").GetInt32();
                var errs = u.GetProperty("errors").GetInt32();
                var avgMs = u.GetProperty("meanDurationMs").GetDouble();
                Console.WriteLine($"    {email,-35} iters={iters,-4} execs={execs,-5} errs={errs,-3} avg={avgMs}ms");
            }
        }

        if (stats.TryGetProperty("sampleErrors", out var errors))
        {
            Console.WriteLine();
            Console.WriteLine("  Sample errors:");
            foreach (var e in errors.EnumerateArray())
            {
                var ui = e.GetProperty("UserIndex").GetInt32();
                var qi = e.GetProperty("QueryIndex").GetInt32();
                var err = e.GetProperty("Error").GetString() ?? "";
                if (err.Length > 100) err = err[..100] + "...";
                Console.WriteLine($"    User {ui}, Q{qi}: {err}");
            }
        }

        if (stats.TryGetProperty("logFile", out var logFileEl))
            Console.WriteLine($"\n  Telemetry log: {logFileEl.GetString()}");

        Console.WriteLine("═══════════════════════════════════════════════");
    }
}
