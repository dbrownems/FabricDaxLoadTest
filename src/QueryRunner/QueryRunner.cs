using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AnalysisServices.AdomdClient;

namespace FabricDaxLoadTest
{
    /// <summary>
    /// Lock-free logger that enqueues all output to a single background
    /// consumer thread via BlockingCollection. The consumer writes to
    /// the on-disk log file and (optionally) invokes <see cref="OnLogLine"/>
    /// for each line.
    /// <para>
    /// IMPORTANT: this logger MUST NOT write to <c>Console.Out</c>
    /// directly. When QueryRunner is hosted inside a Livy or Jupyter
    /// kernel, .NET writes to fd 1 / stderr leak into the kernel's
    /// JSON-RPC framing and corrupt every subsequent statement with
    /// JSON-parse errors. The caller (e.g. LoadGen Program.cs) opts in
    /// to console echo by setting <see cref="OnLogLine"/> to
    /// <c>Console.WriteLine</c>; embedded callers leave it null and
    /// rely on the file log + the <c>RunLoadTest</c> return string.
    /// </para>
    /// </summary>
    public sealed class QueryRunnerLogger : IDisposable
    {
        private readonly BlockingCollection<string> _queue = new(boundedCapacity: 4096);
        private readonly Task _writerTask;
        private readonly string? _logFilePath;

        /// <summary>
        /// Optional sink invoked for every log line. Set to
        /// <c>Console.WriteLine</c> for an interactive console host;
        /// leave null for embedded callers (Livy, notebooks) so the
        /// kernel's stdout stays clean. Exceptions thrown by the
        /// callback are swallowed so a misbehaving sink can never
        /// stall the writer.
        /// </summary>
        public Action<string>? OnLogLine { get; set; }

        public QueryRunnerLogger(string? logFilePath = null)
        {
            _logFilePath = logFilePath;
            if (!string.IsNullOrEmpty(logFilePath))
            {
                var dir = Path.GetDirectoryName(logFilePath);
                if (!string.IsNullOrEmpty(dir))
                    Directory.CreateDirectory(dir);
            }
            _writerTask = Task.Run(WriteLoop);
        }

        public void Log(string message)
        {
            var line = $"[{DateTime.UtcNow:HH:mm:ss.fff}] {message}";
            _queue.TryAdd(line);
        }

        private async Task WriteLoop()
        {
            StreamWriter? fileWriter = null;
            try
            {
                if (!string.IsNullOrEmpty(_logFilePath))
                {
                    var fs = new FileStream(_logFilePath, FileMode.Create, FileAccess.Write, FileShare.Read);
                    fileWriter = new StreamWriter(fs, Encoding.UTF8);
                }

                var batch = new List<string>();
                while (!_queue.IsCompleted)
                {
                    batch.Clear();
                    try
                    {
                        if (_queue.TryTake(out var first, TimeSpan.FromSeconds(5)))
                            batch.Add(first);
                        else
                            continue;
                    }
                    catch (InvalidOperationException) { break; }

                    while (_queue.TryTake(out var item))
                        batch.Add(item);

                    foreach (var line in batch)
                    {
                        Task? ft = null;
                        try { ft = fileWriter?.WriteLineAsync(line); }
                        catch { }
                        try { OnLogLine?.Invoke(line); } catch { /* never let observer crash logger */ }
                        if (ft != null)
                        {
                            try { await ft; }
                            catch { }
                        }
                    }
                    try { if (fileWriter != null) await fileWriter.FlushAsync(); }
                    catch { }
                }
            }
            finally
            {
                fileWriter?.Dispose();
            }
        }

        public void Dispose()
        {
            _queue.CompleteAdding();
            _writerTask.Wait(TimeSpan.FromSeconds(5));
        }
    }

    public class QueryResult
    {
        public int UserIndex { get; set; }
        public int QueryIndex { get; set; }
        public int Iteration { get; set; }
        public int RowCount { get; set; }
        public double DurationMs { get; set; }
        public double StartTimeMs { get; set; }
        public string? Error { get; set; }
        /// <summary>Bytes drained when running in skip-results mode (-1 = N/A).</summary>
        public long ResponseBytes { get; set; } = -1;
        /// <summary>Snapshot of QueryRunnerStatus.ActiveUsers at the moment this query started executing.</summary>
        public int ActiveUsersAtStart { get; set; }
    }

    internal class TelemetryRecord
    {
        public int QueryNumber { get; set; }
        public string UserEmail { get; set; } = "";
        public DateTime Timestamp { get; set; }
        public double StartTimeMs { get; set; }
        public double DurationMs { get; set; }
        public string Outcome { get; set; } = "";
        public string MessageText { get; set; } = "";
        public int ActiveUsers { get; set; }
    }

    /// <summary>
    /// Thread-safe singleton that accumulates live query execution stats.
    /// All user threads call RecordQuery() after each execution; the periodic
    /// reporter reads the window and resets it via SnapshotAndReset().
    /// </summary>
    public class QueryRunnerStatus
    {
        public static readonly QueryRunnerStatus Instance = new();

        // Cumulative totals
        private long _totalQueries;
        private long _totalErrors;
        private int _activeUsers;
        private int _totalConnections;
        private int _distinctUsers;

        // All results (kept for BuildStats at the end)
        private readonly ConcurrentBag<QueryResult> _allResults = new();

        // Current reporting window — reset every snapshot
        private long _windowQueries;
        private long _windowErrors;
        private long _windowMinTicks = long.MaxValue;
        private long _windowMaxTicks;
        private long _windowSumTicks;
        // Track which user indices ran queries in this window
        private readonly ConcurrentDictionary<int, byte> _windowActiveUsers = new();
        // Track which query indices ran in this window
        private readonly ConcurrentDictionary<int, byte> _windowActiveQueries = new();

        public void Reset()
        {
            _totalQueries = 0;
            _totalErrors = 0;
            _activeUsers = 0;
            _totalConnections = 0;
            _distinctUsers = 0;
            ResetWindow();
            while (_allResults.TryTake(out _)) { }
        }

        private void ResetWindow()
        {
            Interlocked.Exchange(ref _windowQueries, 0);
            Interlocked.Exchange(ref _windowErrors, 0);
            Interlocked.Exchange(ref _windowMinTicks, long.MaxValue);
            Interlocked.Exchange(ref _windowMaxTicks, 0);
            Interlocked.Exchange(ref _windowSumTicks, 0);
            _windowActiveUsers.Clear();
            _windowActiveQueries.Clear();
        }

        public void SetConnectionInfo(int totalConnections, int distinctUsers)
        {
            _totalConnections = totalConnections;
            _distinctUsers = distinctUsers;
        }

        public void IncrementActiveUsers() => Interlocked.Increment(ref _activeUsers);

        public int ActiveUsers => Volatile.Read(ref _activeUsers);

        public void RecordQuery(QueryResult result)
        {
            _allResults.Add(result);
            Interlocked.Increment(ref _totalQueries);

            if (result.Error != null)
            {
                Interlocked.Increment(ref _windowErrors);
                Interlocked.Increment(ref _totalErrors);
            }
            else
            {
                long ticks = (long)(result.DurationMs * TimeSpan.TicksPerMillisecond);
                Interlocked.Increment(ref _windowQueries);
                Interlocked.Add(ref _windowSumTicks, ticks);
                _windowActiveUsers.TryAdd(result.UserIndex, 0);
                _windowActiveQueries.TryAdd(result.QueryIndex, 0);

                // Lock-free min
                long curMin;
                do { curMin = Volatile.Read(ref _windowMinTicks); }
                while (ticks < curMin && Interlocked.CompareExchange(ref _windowMinTicks, ticks, curMin) != curMin);

                // Lock-free max
                long curMax;
                do { curMax = Volatile.Read(ref _windowMaxTicks); }
                while (ticks > curMax && Interlocked.CompareExchange(ref _windowMaxTicks, ticks, curMax) != curMax);
            }
        }

        public List<QueryResult> AllResults => _allResults.ToList();
        public long TotalQueries => Volatile.Read(ref _totalQueries);
        public long TotalErrors => Volatile.Read(ref _totalErrors);

        /// <summary>
        /// Returns a snapshot of the current window stats and resets the window.
        /// </summary>
        public WindowSnapshot SnapshotAndReset()
        {
            long queries = Interlocked.Exchange(ref _windowQueries, 0);
            long errors = Interlocked.Exchange(ref _windowErrors, 0);
            long sumTicks = Interlocked.Exchange(ref _windowSumTicks, 0);
            long minTicks = Interlocked.Exchange(ref _windowMinTicks, long.MaxValue);
            long maxTicks = Interlocked.Exchange(ref _windowMaxTicks, 0);

            var userCount = _windowActiveUsers.Count;
            var queryCount = _windowActiveQueries.Count;
            _windowActiveUsers.Clear();
            _windowActiveQueries.Clear();

            return new WindowSnapshot
            {
                Queries = queries,
                Errors = errors,
                ActiveUserCount = userCount,
                ActiveQueryCount = queryCount,
                MinMs = queries > 0 ? minTicks / (double)TimeSpan.TicksPerMillisecond : 0,
                MaxMs = queries > 0 ? maxTicks / (double)TimeSpan.TicksPerMillisecond : 0,
                AvgMs = queries > 0 ? sumTicks / (double)TimeSpan.TicksPerMillisecond / queries : 0,
                TotalQueries = Volatile.Read(ref _totalQueries),
                TotalErrors = Volatile.Read(ref _totalErrors),
            };
        }

        public class WindowSnapshot
        {
            public long Queries { get; set; }
            public long Errors { get; set; }
            public int ActiveUserCount { get; set; }
            public int ActiveQueryCount { get; set; }
            public double MinMs { get; set; }
            public double MaxMs { get; set; }
            public double AvgMs { get; set; }
            public long TotalQueries { get; set; }
            public long TotalErrors { get; set; }
        }
    }

    public static class QueryRunner
    {
        private static QueryRunnerLogger _logger = new();

        private static void Log(string message) => _logger.Log(message);

        public static string RunLoadTest(
            string[] queries, string xmlaEndpoint, string dataset, string token,
            string[] userEmails, string[] userRoles,
            int durationSeconds = 60, int queriesPerBatch = 4,
            int pauseBetweenIterationsMs = 1000, int pauseBetweenQueriesMs = 0,
            string? logDirectory = null, int userRampTimeSec = 0,
            string? logFileName = null,
            bool skipResults = false,
            Action<string>? logCallback = null)
        {
            var status = QueryRunnerStatus.Instance;
            status.Reset();

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds));
            var testStart = Stopwatch.StartNew();
            var testStartTime = DateTime.UtcNow;

            // Set up text logger — derives .log path from CSV path
            string? textLogPath = null;
            if (!string.IsNullOrEmpty(logDirectory))
            {
                Directory.CreateDirectory(logDirectory);
                var baseName = !string.IsNullOrEmpty(logFileName)
                    ? Path.GetFileNameWithoutExtension(logFileName)
                    : $"LoadTest.{testStartTime:yyyyMMdd-HHmmss}";
                textLogPath = Path.Combine(logDirectory, baseName + ".log");
            }
            _logger = new QueryRunnerLogger(textLogPath) { OnLogLine = logCallback };

            Log($"Starting: {userEmails.Length} users, {queries.Length} queries, {durationSeconds}s, {queriesPerBatch} concurrent/user, pause={pauseBetweenIterationsMs}ms/iter, {pauseBetweenQueriesMs}ms/query, ramp={userRampTimeSec}s, skipResults={skipResults}");
            if (textLogPath != null)
                Log($"Text log: {textLogPath}");

            // Set up telemetry CSV writer
            BlockingCollection<TelemetryRecord>? telemetryQueue = null;
            Task? logWriterTask = null;
            string? logFilePath = null;

            if (!string.IsNullOrEmpty(logDirectory))
            {
                var csvName = !string.IsNullOrEmpty(logFileName)
                    ? logFileName
                    : $"LoadTest.{testStartTime:yyyyMMdd-HHmmss}.csv";
                logFilePath = Path.Combine(logDirectory, csvName);
                Log($"Logging to: {logFilePath}");

                // Write CSV header
                File.WriteAllText(logFilePath, "QueryNumber,UserEmail,Timestamp,StartTimeMs,DurationMs,Outcome,MessageText,ActiveUsers\n");

                telemetryQueue = new BlockingCollection<TelemetryRecord>(boundedCapacity: 10000);
                logWriterTask = Task.Run(() => LogWriterLoop(telemetryQueue, logFilePath, cts.Token));
            }

            Task? periodicReporter = null;
            try
            {

            // Calculate per-user start delays for ramp-up
            int nUsers = userEmails.Length;
            double rampIntervalMs = nUsers > 1 && userRampTimeSec > 0
                ? (userRampTimeSec * 1000.0) / (nUsers - 1)
                : 0;

            // Pre-warm the .NET ThreadPool to avoid starvation during ramp-up.
            // Each user driver uses sync-over-async / blocking ADOMD.NET calls, so each concurrent
            // user×connection occupies one ThreadPool worker. Default min worker count equals
            // CPU count (e.g. 2 on a small Fabric Python notebook host), so 100 concurrent users
            // hit the ThreadPool's slow injection rate (~1 thread per 500ms-1s) and ramp serializes.
            // Setting MinThreads up-front makes the pool eagerly create workers on demand
            // instead of throttling injection.
            int targetWorkers = Math.Max(nUsers * Math.Max(1, queriesPerBatch) + 32, 64);
            ThreadPool.GetMinThreads(out int minWorker0, out int minIo0);
            ThreadPool.GetMaxThreads(out int maxWorker, out int maxIo);
            int newMinWorker = Math.Min(targetWorkers, maxWorker);
            int newMinIo = Math.Min(Math.Max(targetWorkers, minIo0), maxIo);
            bool ok = ThreadPool.SetMinThreads(newMinWorker, newMinIo);
            Log($"ThreadPool: min workers {minWorker0}->{newMinWorker}, min IO {minIo0}->{newMinIo} (max {maxWorker}/{maxIo}, target={targetWorkers}, set={ok})");

            var totalConnectTimeMs = new long[] { 0 };
            int progressStep = Math.Max(1, nUsers / 10);
            var connStrings = new string[nUsers];

            // Build connection strings up front so each user driver task is self-contained.
            for (int i = 0; i < nUsers; i++)
            {
                connStrings[i] = BuildConnectionString(xmlaEndpoint, dataset, token,
                    userEmails[i], userRoles[i]);
            }

            // Pre-warm the model/gateway with a single shared warmup connection BEFORE
            // launching user driver tasks. This pays the one-time gateway/XMLA frontend
            // cold-start (which can be 50-100s on a cold capacity) on the main thread,
            // so subsequent per-user Open() warmups hit a hot front-end (~100ms each)
            // and the ramp curve is smooth. AS engine cold-start is a per-model cost,
            // not per-connection, so paying it once is sufficient. Per-user Open()
            // warmup queries still execute and absorb per-socket TCP/TLS/H2 handshake
            // cost, keeping per-query latency metrics clean.
            try
            {
                var warmupSw = Stopwatch.StartNew();
                using var warmupConn = new AdomdConnection(connStrings[0]);
                warmupConn.Open();
                warmupSw.Stop();
                Log($"Pre-warmup: gateway/model warmed in {warmupSw.ElapsedMilliseconds}ms");
            }
            catch (Exception ex)
            {
                Log($"WARNING: Pre-warmup failed ({ex.Message}); continuing — first user Open() will pay cold-start");
            }

            // Launch one driver task per user. Each task waits for its scheduled ramp slot,
            // opens its own connections (which include the Open() warmup query), increments
            // the active-user counter, then runs the simulation loop. This staggers user
            // arrivals at the exact rampIntervalMs cadence — no batched bursts.
            int connectedUsers = 0;
            int connectFailures = 0;
            var userTasks = new List<Task>(nUsers);
            for (int i = 0; i < nUsers; i++)
            {
                int userIdx = i;
                int delayMs = (int)Math.Round(userIdx * rampIntervalMs);
                userTasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        if (delayMs > 0)
                            await Task.Delay(delayMs, cts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) { return; }

                    IDbConnection[]? connections = null;
                    try
                    {
                        var sw = Stopwatch.StartNew();
                        connections = new IDbConnection[queriesPerBatch];
                        for (int c = 0; c < queriesPerBatch; c++)
                        {
                            connections[c] = new AdomdConnection(connStrings[userIdx]);
                            connections[c].Open();
                        }
                        sw.Stop();
                        Interlocked.Add(ref totalConnectTimeMs[0], (long)sw.Elapsed.TotalMilliseconds);
                        Interlocked.Increment(ref connectedUsers);
                        status.IncrementActiveUsers();
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref connectFailures);
                        Log($"ERROR connecting user {userIdx} ({userEmails[userIdx]}): {ex.Message}");
                        if (connections != null)
                            for (int c = 0; c < connections.Length; c++)
                                try { connections[c]?.Dispose(); } catch { }
                        return;
                    }

                    SimulateUserWithConnections(userIdx, queries, userEmails[userIdx],
                        queriesPerBatch, pauseBetweenIterationsMs, pauseBetweenQueriesMs,
                        connections, connStrings[userIdx], skipResults,
                        testStart, telemetryQueue, cts.Token);
                }, cts.Token));
            }

            // Periodic ramp-progress logging (every progressStep users connected)
            int lastLogged = 0;
            while (true)
            {
                int connected = Volatile.Read(ref connectedUsers);
                int failed = Volatile.Read(ref connectFailures);
                int finished = connected + failed;
                if (finished >= nUsers) break;
                if (cts.Token.IsCancellationRequested) break;

                int nextThreshold = ((lastLogged / progressStep) + 1) * progressStep;
                if (connected >= nextThreshold)
                {
                    double avgMs = connected > 0 ? Volatile.Read(ref totalConnectTimeMs[0]) / (double)connected : 0;
                    Log($"Ramp: {connected}/{nUsers} connected, avg connect {avgMs:F0}ms, t={testStart.Elapsed.TotalSeconds:F0}s");
                    lastLogged = connected;
                }
                try { Task.Delay(500, cts.Token).Wait(cts.Token); }
                catch (OperationCanceledException) { break; }
            }
            // Final ramp log line
            {
                int connected = Volatile.Read(ref connectedUsers);
                double avgMs = connected > 0 ? Volatile.Read(ref totalConnectTimeMs[0]) / (double)connected : 0;
                Log($"Ramp: {connected}/{nUsers} connected, avg connect {avgMs:F0}ms, t={testStart.Elapsed.TotalSeconds:F0}s");
            }

            if (connectedUsers == 0)
                throw new InvalidOperationException("No users connected successfully — cannot run load test");

            if (connectedUsers < nUsers)
                Log($"WARNING: Only {connectedUsers}/{nUsers} users connected successfully");

            // ── Connection summary after ramp-up ──
            int totalConns = connectedUsers * queriesPerBatch;
            var distinctEmails = userEmails.Distinct().Count();
            var distinctRoles = userRoles.Distinct().Count();
            status.SetConnectionInfo(totalConns, distinctEmails);
            double avgConnMs = connectedUsers > 0 ? Volatile.Read(ref totalConnectTimeMs[0]) / (double)connectedUsers : 0;
            Log("Ramp-up complete");
            Log("┌──────────────────────────────────────────┐");
            Log("│         Connection Summary               │");
            Log("├──────────────────────────────────────────┤");
            Log($"│  Users:             {connectedUsers,-20}│");
            Log($"│  Distinct emails:   {distinctEmails,-20}│");
            Log($"│  Distinct roles:    {distinctRoles,-20}│");
            Log($"│  Connections/user:  {queriesPerBatch,-20}│");
            Log($"│  Total connections: {totalConns,-20}│");
            Log($"│  Avg connect time:  {avgConnMs:F0}ms{new string(' ', Math.Max(0, 17 - avgConnMs.ToString("F0").Length))}│");
            Log($"│  Ramp-up time:      {testStart.Elapsed.TotalSeconds:F1}s{new string(' ', Math.Max(0, 17 - testStart.Elapsed.TotalSeconds.ToString("F1").Length))}│");
            Log("└──────────────────────────────────────────┘");

            // ── Periodic stats reporter (every 60s) ──
            periodicReporter = Task.Run(() =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try { Task.Delay(60_000, cts.Token).Wait(cts.Token); }
                    catch (OperationCanceledException) { break; }

                    var snap = status.SnapshotAndReset();
                    Log($"Progress: queries={snap.Queries} users={snap.ActiveUserCount} qIdx={snap.ActiveQueryCount} " +
                        $"min={snap.MinMs:F0}ms avg={snap.AvgMs:F0}ms max={snap.MaxMs:F0}ms " +
                        $"errors={snap.Errors} total={snap.TotalQueries} totalErr={snap.TotalErrors} " +
                        $"t={testStart.Elapsed.TotalSeconds:F0}s");
                }
            });

            Task.WaitAll(userTasks.ToArray());

            } // end try
            catch (Exception ex)
            {
                Log($"FATAL ERROR: {ex}");
                throw;
            }
            finally
            {
            testStart.Stop();

            // Shut down periodic reporter
            if (periodicReporter != null)
                try { periodicReporter.Wait(TimeSpan.FromSeconds(2)); } catch { }

            // Shut down log writer
            if (telemetryQueue != null)
            {
                telemetryQueue.CompleteAdding();
                logWriterTask?.Wait(TimeSpan.FromSeconds(10));
                Log($"Log written: {logFilePath}");
            }
            }

            Log($"Done: {status.TotalQueries} executions in {testStart.Elapsed.TotalSeconds:F1}s");

            var result = BuildStats(status.AllResults, testStart.Elapsed.TotalMilliseconds,
                userEmails.Length, queries.Length, logFilePath);

            _logger.Dispose();
            return result;
        }

        private static void LogWriterLoop(BlockingCollection<TelemetryRecord> queue,
            string logFilePath, CancellationToken ct)
        {
            var batch = new List<TelemetryRecord>();

            while (!queue.IsCompleted)
            {
                batch.Clear();

                // Block up to 10 seconds for the first item
                try
                {
                    if (queue.TryTake(out var first, TimeSpan.FromSeconds(10)))
                        batch.Add(first);
                    else
                        continue;
                }
                catch (InvalidOperationException) { break; } // CompleteAdding was called

                // Drain any remaining items without blocking
                while (queue.TryTake(out var item))
                    batch.Add(item);

                if (batch.Count == 0) continue;

                // Open, append, close — blobfuse doesn't support file sharing
                var sb = new StringBuilder();
                foreach (var r in batch)
                {
                    var msg = SanitizeCsvField(r.MessageText);
                    sb.AppendLine(
                        $"{r.QueryNumber},{r.UserEmail},{r.Timestamp:yyyy-MM-dd HH:mm:ss.fff},{r.StartTimeMs:F0},{r.DurationMs:F1},{r.Outcome},{msg},{r.ActiveUsers}");
                }

                try
                {
                    File.AppendAllText(logFilePath, sb.ToString());
                }
                catch (Exception ex)
                {
                    Log($"[LogWriter] Error writing: {ex.Message}");
                }
            }
        }

        private static string SanitizeCsvField(string value)
        {
            if (string.IsNullOrEmpty(value)) return "";
            // Truncate long messages
            if (value.Length > 500) value = value[..500];
            // Replace problematic characters
            value = value.Replace("\r", " ").Replace("\n", " ");
            // Quote if contains comma, quote, or whitespace
            if (value.Contains(',') || value.Contains('"') || value.Contains(' '))
            {
                value = "\"" + value.Replace("\"", "\"\"") + "\"";
            }
            return value;
        }

        private static void SimulateUserWithConnections(
            int userIndex, string[] queries, string email,
            int queriesPerBatch, int pauseMs, int pauseBetweenQueriesMs,
            IDbConnection[] connections, string connStr,
            bool skipResults,
            Stopwatch testStart,
            BlockingCollection<TelemetryRecord>? telemetryQueue,
            CancellationToken ct)
        {
            try
            {
                int iteration = 0;
                while (!ct.IsCancellationRequested)
                {
                    iteration++;
                    RunIteration(userIndex, email, iteration, queries, connections, connStr,
                        skipResults,
                        pauseBetweenQueriesMs, testStart, telemetryQueue, ct);
                    if (ct.IsCancellationRequested) break;

                    try { Task.Delay(pauseMs, ct).Wait(ct); }
                    catch (OperationCanceledException) { break; }
                }
            }
            finally
            {
                for (int c = 0; c < connections.Length; c++)
                    if (connections[c] != null) { connections[c].Close(); connections[c].Dispose(); }
            }
        }

        private static void RunIteration(
            int userIndex, string email, int iteration, string[] queries,
            IDbConnection[] connections, string connStr,
            bool skipResults,
            int pauseBetweenQueriesMs,
            Stopwatch testStart,
            BlockingCollection<TelemetryRecord>? telemetryQueue,
            CancellationToken ct)
        {
            var status = QueryRunnerStatus.Instance;
            int max = connections.Length;
            using var sem = new SemaphoreSlim(max);
            var connSlots = new ConcurrentQueue<IDbConnection>(connections);
            var tasks = new List<Task>();

            for (int q = 0; q < queries.Length; q++)
            {
                if (ct.IsCancellationRequested) break;
                try { sem.Wait(ct); }
                catch (OperationCanceledException) { break; }

                int qi = q; int iter = iteration;
                tasks.Add(Task.Run(() =>
                {
                    IDbConnection? conn = null;
                    try
                    {
                        if (!connSlots.TryDequeue(out conn))
                            throw new InvalidOperationException("No connection available");

                        var r = ExecuteQuery(userIndex, qi, iter, queries[qi], conn, skipResults, testStart);
                        if (r.Error != null && r.Error.Contains("timed out or was lost"))
                        {
                            status.RecordQuery(r);
                            SubmitTelemetry(telemetryQueue, qi, email, r);

                            Log($"[User {userIndex}] Q{qi} iter {iter} connection lost, reconnecting...");
                            try
                            {
                                conn.Close();
                                conn.Dispose();
                                conn = new AdomdConnection(connStr);
                                conn.Open();
                            }
                            catch (Exception reconEx)
                            {
                                Log($"[User {userIndex}] Reconnect failed: {reconEx.Message}");
                                throw;
                            }

                            // Retry the query once on the new connection
                            r = ExecuteQuery(userIndex, qi, iter, queries[qi], conn, skipResults, testStart);
                        }

                        status.RecordQuery(r);
                        SubmitTelemetry(telemetryQueue, qi, email, r);

                        if (r.Error != null)
                        {
                            Log($"[User {userIndex}] Q{qi} iter {iter} FAILED: {r.Error}");
                            throw new Exception(r.Error);
                        }
                        if (pauseBetweenQueriesMs > 0)
                        {
                            try { Task.Delay(pauseBetweenQueriesMs, ct).Wait(ct); }
                            catch (OperationCanceledException) { }
                        }
                    }
                    finally
                    {
                        if (conn != null) connSlots.Enqueue(conn);
                        sem.Release();
                    }
                }));
            }

            Task.WaitAll(tasks.ToArray());
        }

        private static void SubmitTelemetry(BlockingCollection<TelemetryRecord>? telemetryQueue,
            int qi, string email, QueryResult r)
        {
            if (telemetryQueue != null && !telemetryQueue.IsAddingCompleted)
            {
                string msg = r.Error
                    ?? (r.ResponseBytes >= 0 ? $"{r.ResponseBytes} bytes" : $"{r.RowCount} rows");
                var record = new TelemetryRecord
                {
                    QueryNumber = qi,
                    UserEmail = email,
                    Timestamp = DateTime.UtcNow,
                    StartTimeMs = r.StartTimeMs,
                    DurationMs = r.DurationMs,
                    Outcome = r.Error == null ? "Success" : "Error",
                    MessageText = msg,
                    ActiveUsers = r.ActiveUsersAtStart,
                };
                telemetryQueue.TryAdd(record);
            }
        }

        private static QueryResult ExecuteQuery(int userIndex, int queryIndex,
            int iteration, string query, IDbConnection conn, bool skipResults, Stopwatch testStart)
        {
            var result = new QueryResult {
                UserIndex = userIndex, QueryIndex = queryIndex, Iteration = iteration,
                StartTimeMs = Math.Round(testStart.Elapsed.TotalMilliseconds),
                ActiveUsersAtStart = QueryRunnerStatus.Instance.ActiveUsers };
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = query;
                cmd.CommandTimeout = 0;
                var sw = Stopwatch.StartNew();
                if (skipResults)
                {
                    // SkipResults path: must not load Apache.Arrow.
                    // ExecuteNonQuery drains the response stream into a byte counter.
                    cmd.ExecuteNonQuery();
                    sw.Stop();
                    result.DurationMs = sw.Elapsed.TotalMilliseconds;
                }
                else
                {
                    using var reader = cmd.ExecuteReader();
                    int count = 0;
                    while (reader.Read()) count++;
                    sw.Stop();
                    result.RowCount = count;
                    result.DurationMs = sw.Elapsed.TotalMilliseconds;
                }
            }
            catch (Exception ex)
            {
                result.Error = ex.Message.Length > 500 ? ex.Message[..500] : ex.Message;
            }
            return result;
        }

        private static string BuildConnectionString(
            string xmlaEndpoint, string dataset, string token,
            string userEmail, string userRole)
        {
            var sb = new StringBuilder();
            sb.Append($"Data Source={xmlaEndpoint};Initial Catalog={dataset};");
            sb.Append($"password={token};Timeout=7200;Connect Timeout=300;");
            if (!string.IsNullOrEmpty(userEmail))
                sb.Append($"CustomData={userEmail};");
            if (!string.IsNullOrEmpty(userRole))
                sb.Append($"Roles={userRole};");
            return sb.ToString();
        }

        private static string BuildStats(List<QueryResult> results, double totalMs,
            int nUsers, int nQueries, string? logFilePath = null)
        {
            var ok = results.Where(r => r.Error == null).ToList();
            var fail = results.Where(r => r.Error != null).ToList();
            var durs = ok.Select(r => r.DurationMs).OrderBy(d => d).ToList();
            int maxIter = results.Any() ? results.Max(r => r.Iteration) : 0;

            var stats = new Dictionary<string, object>
            {
                ["totalDurationMs"] = Math.Round(totalMs),
                ["users"] = nUsers,
                ["queriesPerIteration"] = nQueries,
                ["totalExecutions"] = results.Count,
                ["successfulExecutions"] = ok.Count,
                ["failedExecutions"] = fail.Count,
                ["maxIteration"] = maxIter,
                ["qps"] = Math.Round(ok.Count / (totalMs / 1000), 1),
            };

            if (logFilePath != null)
                stats["logFile"] = logFilePath;

            if (durs.Any())
                stats["latency"] = new Dictionary<string, object>
                {
                    ["min"] = Math.Round(durs.First()),
                    ["max"] = Math.Round(durs.Last()),
                    ["mean"] = Math.Round(durs.Average()),
                    ["median"] = Math.Round(Pct(durs, 50)),
                    ["p95"] = Math.Round(Pct(durs, 95)),
                    ["p99"] = Math.Round(Pct(durs, 99)),
                };

            var perUser = results.GroupBy(r => r.UserIndex).OrderBy(g => g.Key)
                .Select(g => new Dictionary<string, object>
                {
                    ["userIndex"] = g.Key,
                    ["iterations"] = g.Max(r => r.Iteration),
                    ["executions"] = g.Count(),
                    ["errors"] = g.Count(r => r.Error != null),
                    ["meanLatencyMs"] = g.Where(r => r.Error == null).Select(r => r.DurationMs)
                        .DefaultIfEmpty(0).Average() is var avg ? Math.Round(avg) : 0,
                }).ToList();
            stats["perUser"] = perUser;

            if (fail.Any())
                stats["sampleErrors"] = fail.Take(5)
                    .Select(r => new { r.UserIndex, r.QueryIndex, r.Iteration, r.Error }).ToList();

            // Time-series: per-second buckets for glitch detection
            var timeline = ok.GroupBy(r => (int)(r.StartTimeMs / 1000))
                .OrderBy(g => g.Key)
                .Select(g =>
                {
                    var d = g.Select(r => r.DurationMs).OrderBy(x => x).ToList();
                    return new Dictionary<string, object>
                    {
                        ["second"] = g.Key,
                        ["count"] = g.Count(),
                        ["meanMs"] = Math.Round(d.Average()),
                        ["p50Ms"] = Math.Round(Pct(d, 50)),
                        ["p95Ms"] = Math.Round(Pct(d, 95)),
                        ["maxMs"] = Math.Round(d.Last()),
                    };
                }).ToList();
            stats["timeline"] = timeline;

            // Raw executions sorted by start time (for detailed analysis)
            stats["executions"] = ok.OrderBy(r => r.StartTimeMs)
                .Select(r => new Dictionary<string, object>
                {
                    ["t"] = r.StartTimeMs,
                    ["ms"] = Math.Round(r.DurationMs),
                    ["u"] = r.UserIndex,
                    ["q"] = r.QueryIndex,
                }).ToList();

            return JsonSerializer.Serialize(stats, new JsonSerializerOptions { WriteIndented = true });
        }

        private static double Pct(List<double> s, double p) =>
            !s.Any() ? 0 : s[Math.Clamp((int)Math.Ceiling(p / 100.0 * s.Count) - 1, 0, s.Count - 1)];
    }
}
