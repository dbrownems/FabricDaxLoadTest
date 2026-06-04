using System;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using Azure.Core;
using Azure.Identity;
using FabricDaxLoadTest;

namespace FabricDaxLoadTest;

class Program
{
    static int Main(string[] args)
    {
        var xmlaOption = new Option<string>("--xmla", "XMLA endpoint (e.g. powerbi://api.powerbi.com/v1.0/myorg/Workspace)") { IsRequired = true };
        var datasetOption = new Option<string>("--dataset", "Semantic model name") { IsRequired = true };
        var durationOption = new Option<int>("--duration", () => 60, "Test duration in seconds");
        var usersOption = new Option<int>("--users", () => 100, "Number of concurrent simulated users");
        var queriesPerBatchOption = new Option<int>("--queries-per-batch", () => 1, "Concurrent queries per user");
        var pauseIterOption = new Option<int>("--pause-iterations", () => 1000, "Pause between iterations (ms)");
        var pauseQueryOption = new Option<int>("--pause-queries", () => 0, "Pause between queries (ms)");
        var rampOption = new Option<int>("--ramp-time", () => 30, "User ramp-up time (seconds)");
        var replicaOption = new Option<string>("--replica", () => "", "Target replica ('readonly' for scale-out read replica, or '')");
        var queriesFileOption = new Option<FileInfo>("--queries-file", "Path to queries.json") { IsRequired = true };
        var usersFileOption = new Option<FileInfo>("--users-file", "Path to users.json") { IsRequired = true };
        var logDirOption = new Option<string>("--log-dir", () => "./logs", "Directory for telemetry CSV logs");
        var logFileOption = new Option<string>("--log-file", () => "", "Log filename (auto-generated if empty)");
        var tokenOption = new Option<string?>("--token", "Access token (prefer --token-file or --auth)");
        var tokenFileOption = new Option<FileInfo?>("--token-file", "Path to file containing access token");
        var authOption = new Option<string?>("--auth",
            "Azure auth method: 'default', 'browser', 'devicecode', 'cli', 'env', or 'managedidentity'");
        var skipResultsOption = new Option<bool>("--skip-results", () => false, "Drain response without parsing rows");

        var rootCommand = new RootCommand("LoadGen — DAX load test runner for Power BI / Fabric semantic models")
        {
            xmlaOption, datasetOption, durationOption, usersOption,
            queriesPerBatchOption, pauseIterOption, pauseQueryOption,
            rampOption, replicaOption, queriesFileOption, usersFileOption,
            logDirOption, logFileOption, tokenOption, tokenFileOption, authOption,
            skipResultsOption,
        };

        rootCommand.SetHandler((InvocationContext ctx) =>
        {
            var xmla = ctx.ParseResult.GetValueForOption(xmlaOption)!;
            var dataset = ctx.ParseResult.GetValueForOption(datasetOption)!;
            var duration = ctx.ParseResult.GetValueForOption(durationOption);
            var userCount = ctx.ParseResult.GetValueForOption(usersOption);
            var queriesPerBatch = ctx.ParseResult.GetValueForOption(queriesPerBatchOption);
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
            var auth = ctx.ParseResult.GetValueForOption(authOption);
            var skipResults = ctx.ParseResult.GetValueForOption(skipResultsOption);

            ctx.ExitCode = Run(xmla, dataset, duration, userCount, queriesPerBatch,
                pauseIter, pauseQuery, rampTime, replica, queriesFile, usersFile,
                logDir, logFile, tokenDirect, tokenFile, auth, skipResults);
        });

        return rootCommand.Invoke(args);
    }

    static int Run(string xmla, string dataset, int duration, int userCount,
        int queriesPerBatch, int pauseIter, int pauseQuery, int rampTime,
        string replica, FileInfo queriesFile, FileInfo usersFile,
        string logDir, string logFile, string? tokenDirect, FileInfo? tokenFile,
        string? auth, bool skipResults)
    {
        var token = ResolveToken(tokenDirect, tokenFile, auth);
        if (token == null)
        {
            Console.Error.WriteLine("Error: No access token provided.");
            Console.Error.WriteLine("  Use --auth <method>, --token-file <path>, --token <value>, or set PBI_TOKEN env var.");
            Console.Error.WriteLine("  Auth methods: default, browser, devicecode, cli, env, managedidentity");
            return 1;
        }

        if (duration > 3000)
            Console.WriteLine("Warning: Long duration requested. Token may expire during the test.");

        if (!queriesFile.Exists)
        {
            Console.Error.WriteLine($"Error: Queries file not found: {queriesFile.FullName}");
            return 1;
        }

        string[] queries;
        try { queries = ParseQueries(File.ReadAllText(queriesFile.FullName)); }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error parsing queries file: {ex.Message}");
            return 1;
        }

        if (!usersFile.Exists)
        {
            Console.Error.WriteLine($"Error: Users file not found: {usersFile.FullName}");
            return 1;
        }

        (string email, string role)[] allUsers;
        try { allUsers = ParseUsers(File.ReadAllText(usersFile.FullName)); }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error parsing users file: {ex.Message}");
            return 1;
        }

        if (allUsers.Length == 0)
        {
            Console.Error.WriteLine("Error: users.json contains no users.");
            return 1;
        }

        var users = Enumerable.Range(0, userCount)
            .Select(i => allUsers[i % allUsers.Length])
            .ToArray();

        if (userCount > allUsers.Length)
            Console.WriteLine($"Note: Reusing {allUsers.Length} users to fill {userCount} slots.");

        var xmlaEndpoint = !string.IsNullOrEmpty(replica) ? $"{xmla}?{replica}" : xmla;

        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════");
        Console.WriteLine("  LoadGen — Power BI / Fabric DAX Load Test");
        Console.WriteLine("═══════════════════════════════════════════════");
        Console.WriteLine($"  Dataset:     {dataset}");
        Console.WriteLine($"  Endpoint:    {xmlaEndpoint}");
        Console.WriteLine($"  Duration:    {duration}s");
        Console.WriteLine($"  Users:       {userCount} (from {allUsers.Length} in users.json)");
        Console.WriteLine($"  Queries:     {queries.Length}");
        Console.WriteLine($"  Per batch:   {queriesPerBatch}");
        Console.WriteLine($"  Pause iter:  {pauseIter}ms");
        Console.WriteLine($"  Pause query: {pauseQuery}ms");
        Console.WriteLine($"  Ramp time:   {rampTime}s");
        Console.WriteLine($"  Replica:     {(string.IsNullOrEmpty(replica) ? "(default)" : replica)}");
        Console.WriteLine($"  SkipResults: {skipResults}");
        Console.WriteLine($"  Log dir:     {logDir}");
        Console.WriteLine($"  Token:       {token.Length} chars");
        Console.WriteLine("═══════════════════════════════════════════════");
        Console.WriteLine();

        var emailArr = users.Select(u => u.email).ToArray();
        var roleArr = users.Select(u => u.role).ToArray();

        Directory.CreateDirectory(logDir);

        string resultJson;
        try
        {
            resultJson = QueryRunner.RunLoadTest(
                queries, xmlaEndpoint, dataset, token,
                emailArr, roleArr,
                duration, queriesPerBatch,
                pauseIter, pauseQuery,
                logDir, rampTime, logFile,
                skipResults,
                Console.WriteLine);
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

    static string? ResolveToken(string? direct, FileInfo? file, string? auth)
    {
        if (!string.IsNullOrWhiteSpace(direct)) return direct.Trim();
        if (file != null && file.Exists) return File.ReadAllText(file.FullName).Trim();
        var envToken = Environment.GetEnvironmentVariable("PBI_TOKEN");
        if (!string.IsNullOrWhiteSpace(envToken)) return envToken.Trim();
        if (!string.IsNullOrWhiteSpace(auth)) return AcquireToken(auth.Trim().ToLowerInvariant());
        return null;
    }

    static string AcquireToken(string method)
    {
        var scope = "https://analysis.windows.net/powerbi/api/.default";
        Console.WriteLine($"Acquiring token via Azure {method} credential...");

        TokenCredential credential = method switch
        {
            "default" => new DefaultAzureCredential(),
            "browser" => new InteractiveBrowserCredential(),
            "devicecode" => new DeviceCodeCredential(new DeviceCodeCredentialOptions
            {
                DeviceCodeCallback = (info, cancel) =>
                {
                    Console.WriteLine(info.Message);
                    return System.Threading.Tasks.Task.CompletedTask;
                }
            }),
            "cli" => new AzureCliCredential(),
            "env" => new EnvironmentCredential(),
            "managedidentity" => new ManagedIdentityCredential(),
            _ => throw new ArgumentException(
                $"Unknown auth method '{method}'. Use: default, browser, devicecode, cli, env, managedidentity")
        };

        var tokenResult = credential.GetToken(
            new TokenRequestContext(new[] { scope }), CancellationToken.None);
        Console.WriteLine($"Token acquired, expires {tokenResult.ExpiresOn:HH:mm:ss UTC}");
        return tokenResult.Token;
    }

    static string[] ParseQueries(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.EnumerateArray().Select(el =>
        {
            if (el.ValueKind == JsonValueKind.String) return el.GetString()!;
            if (el.TryGetProperty("query", out var q)) return q.GetString()!;
            throw new InvalidOperationException("Each query must be a string or an object with a 'query' field.");
        }).ToArray();
    }

    static (string email, string role)[] ParseUsers(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.EnumerateArray().Select(el =>
        {
            var email = el.GetProperty("email").GetString()!;
            var role = el.GetProperty("role").GetString()!;
            return (email, role);
        }).ToArray();
    }

    static void PrintResults(string resultJson, (string email, string role)[] users)
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

        if (stats.TryGetProperty("latency", out var lat))
        {
            Console.WriteLine();
            Console.WriteLine("  Latency:");
            Console.WriteLine($"    Min:    {lat.GetProperty("min").GetDouble()}ms");
            Console.WriteLine($"    Median: {lat.GetProperty("median").GetDouble()}ms");
            Console.WriteLine($"    Mean:   {lat.GetProperty("mean").GetDouble()}ms");
            Console.WriteLine($"    P95:    {lat.GetProperty("p95").GetDouble()}ms");
            Console.WriteLine($"    P99:    {lat.GetProperty("p99").GetDouble()}ms");
            Console.WriteLine($"    Max:    {lat.GetProperty("max").GetDouble()}ms");
        }

        Console.WriteLine();
        Console.WriteLine("  Per-user summary:");
        if (stats.TryGetProperty("perUser", out var perUser))
        {
            foreach (var u in perUser.EnumerateArray())
            {
                var idx = u.GetProperty("userIndex").GetInt32();
                var email = idx < users.Length ? users[idx].email : $"user-{idx}";
                var iters = u.GetProperty("iterations").GetInt32();
                var execs = u.GetProperty("executions").GetInt32();
                var errs = u.GetProperty("errors").GetInt32();
                var avgMs = u.GetProperty("meanLatencyMs").GetDouble();
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
