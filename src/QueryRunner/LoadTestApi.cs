using System;
using System.Threading;
using System.Threading.Tasks;

namespace FabricDaxLoadTest
{
    /// <summary>
    /// Behavior when a query returns an error (DAX semantic error, query
    /// timeout reported by AS, etc.). Infrastructure failures
    /// (unrecoverable connection loss) always abort regardless of policy.
    /// </summary>
    public enum ErrorPolicy
    {
        /// <summary>Record the error and keep running. Default.</summary>
        Continue = 0,
        /// <summary>Throw on the first per-query error and fail the run.</summary>
        Abort = 1,
    }

    /// <summary>
    /// All inputs to <see cref="QueryRunner.StartLoadTest(LoadTestConfig)"/>.
    /// Plain class with mutable properties so pythonnet callers can populate
    /// it field-by-field without dealing with positional record constructors.
    /// </summary>
    public sealed class LoadTestConfig
    {
        public string[] Queries { get; set; } = Array.Empty<string>();
        public string XmlaEndpoint { get; set; } = "";
        public string Dataset { get; set; } = "";
        public string Token { get; set; } = "";
        public string[] UserEmails { get; set; } = Array.Empty<string>();
        public string[] UserRoles { get; set; } = Array.Empty<string>();
        public int DurationSeconds { get; set; } = 60;
        public int QueriesPerBatch { get; set; } = 4;
        public int PauseBetweenIterationsMs { get; set; } = 1000;
        public int PauseBetweenQueriesMs { get; set; }
        public string? LogDirectory { get; set; }
        public int UserRampTimeSec { get; set; }
        public string? LogFileName { get; set; }
        public bool SkipResults { get; set; }

        /// <summary>
        /// How to handle per-query errors. Default <see cref="ErrorPolicy.Continue"/>
        /// so the run completes and the per-execution telemetry shows the
        /// failure rate under load. Infrastructure failures (connect-then-fail,
        /// reconnect-then-fail) still abort the run regardless of this setting.
        /// </summary>
        public ErrorPolicy ErrorPolicy { get; set; } = ErrorPolicy.Continue;

        /// <summary>
        /// When true (default), subscribe to the dataset's XMLA trace for
        /// the duration of the run and capture engine-side events
        /// (QueryEnd, ExecutionMetrics, VertiPaqSEQuery*, etc.) to
        /// <c>{LogFileName}.trace.csv</c> alongside the executions CSV.
        /// Best-effort — if trace setup fails (insufficient permissions,
        /// dataset doesn't allow database-scoped traces, etc.) the run
        /// proceeds without engine telemetry and a warning is logged.
        /// </summary>
        public bool EnableTracing { get; set; } = true;

        /// <summary>
        /// Optional sink invoked from the .NET logger thread for every log
        /// line. Embedded callers (Livy / Jupyter notebooks) should leave
        /// this null and read the file log + <see cref="LoadTestHandle.LatestSnapshot"/>;
        /// the LoadGen CLI sets it to <c>Console.WriteLine</c>. Callbacks
        /// must be lightweight and must not call back into the .NET layer
        /// (pythonnet GIL re-entrancy hazard).
        /// </summary>
        public Action<string>? LogCallback { get; set; }
    }

    /// <summary>
    /// Immutable point-in-time view of a running load test. Updated ~1 Hz
    /// by the driver thread and exposed through
    /// <see cref="LoadTestHandle.LatestSnapshot"/>. Safe to read from any
    /// thread (including pythonnet); reads are lock-free.
    /// </summary>
    public sealed class LoadTestProgressSnapshot
    {
        public DateTime UtcNow { get; init; }
        public TimeSpan Elapsed { get; init; }

        /// <summary>
        /// One of: "Pending", "Connecting", "Steady", "Cancelling",
        /// "Done", "Cancelled", "Failed".
        /// </summary>
        public string Phase { get; init; } = "Pending";

        /// <summary>
        /// Count of user drivers that have successfully connected so far.
        /// Note: there is currently no decrement on user-task completion,
        /// so after the run finishes this value reflects "users that
        /// connected during the run", not currently active users.
        /// </summary>
        public int ActiveUsers { get; init; }

        public int TargetUsers { get; init; }
        public long Successful { get; init; }
        public long Failed { get; init; }

        /// <summary>
        /// Queries-per-second over the last ~5 seconds. Computed by
        /// the snapshot loop from the cumulative success counter; 0 until
        /// at least one full sample interval has elapsed.
        /// </summary>
        public double RollingQps { get; init; }
    }

    /// <summary>
    /// Mutable single-slot box for the latest snapshot. Volatile read/write
    /// keeps the publication race-free without a lock. Internal because the
    /// only legitimate writer is <see cref="QueryRunner"/>.
    /// </summary>
    internal sealed class SnapshotBox
    {
        private LoadTestProgressSnapshot _value;
        public SnapshotBox(LoadTestProgressSnapshot initial) { _value = initial; }
        public LoadTestProgressSnapshot Value => Volatile.Read(ref _value);
        public void Set(LoadTestProgressSnapshot v) => Volatile.Write(ref _value, v);
    }

    /// <summary>
    /// Handle returned by <see cref="QueryRunner.StartLoadTest"/>. The
    /// load test runs on a background <see cref="Task"/>; callers poll
    /// <see cref="LatestSnapshot"/>, request graceful stop via
    /// <see cref="Cancel"/>, and join via <see cref="Wait"/>.
    /// <para>
    /// <b>Cancellation is cooperative.</b> <see cref="Cancel"/> only
    /// triggers the .NET cancellation token; in-flight ADOMD calls
    /// (<c>Open()</c>, <c>ExecuteReader()</c>) cannot be interrupted and
    /// will finish on their own. After <see cref="Cancel"/>, expect a
    /// drain delay equal to the slowest in-flight query.
    /// </para>
    /// </summary>
    public sealed class LoadTestHandle : IDisposable
    {
        private readonly Task<string> _task;
        private readonly CancellationTokenSource _externalCts;
        private readonly SnapshotBox _box;
        private int _disposed;

        public Guid RunId { get; }
        public bool IsCompleted => _task.IsCompleted;
        public LoadTestProgressSnapshot LatestSnapshot => _box.Value;

        internal LoadTestHandle(
            Guid runId,
            Task<string> task,
            CancellationTokenSource externalCts,
            SnapshotBox box)
        {
            RunId = runId;
            _task = task;
            _externalCts = externalCts;
            _box = box;
        }

        /// <summary>
        /// Request a graceful stop. Returns immediately; the background
        /// task drains in-flight queries and finalizes counters before
        /// completing. Idempotent. Safe after <see cref="Dispose"/>.
        /// </summary>
        public void Cancel()
        {
            try { _externalCts.Cancel(); }
            catch (ObjectDisposedException) { /* already cleaned up */ }
        }

        /// <summary>
        /// Block until the load test completes and return the JSON stats
        /// string. Re-throws the original exception if the run faulted.
        /// </summary>
        public string Wait()
        {
            // GetAwaiter().GetResult() preserves the original exception's
            // stack trace better than catching AggregateException.
            return _task.GetAwaiter().GetResult();
        }

        /// <summary>
        /// Block up to <paramref name="timeoutMs"/> milliseconds for
        /// completion. Throws <see cref="TimeoutException"/> if the run
        /// has not finished. Does NOT request cancellation — the caller
        /// must invoke <see cref="Cancel"/> separately if desired.
        /// </summary>
        public string WaitOrThrow(int timeoutMs)
        {
            if (!_task.Wait(timeoutMs))
                throw new TimeoutException(
                    $"Load test {RunId} did not complete within {timeoutMs} ms.");
            return _task.GetAwaiter().GetResult();
        }

        /// <summary>
        /// Cancels the run (idempotent) and schedules the underlying
        /// <see cref="CancellationTokenSource"/> for disposal once the
        /// background task completes. Disposing the CTS while the
        /// background task is still creating linked tokens would race;
        /// deferring via a continuation avoids it.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            try { _externalCts.Cancel(); }
            catch (ObjectDisposedException) { }
            _task.ContinueWith(
                _ => { try { _externalCts.Dispose(); } catch { } },
                TaskScheduler.Default);
        }
    }
}
