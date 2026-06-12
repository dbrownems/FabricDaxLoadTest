using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace FabricDaxLoadTest
{
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
        private int _inFlightQueries;
        private int _totalConnections;
        private int _distinctUsers;

        // Ring of (timestampTicks, durationMs) for successful queries within
        // the last DURATION_WINDOW_SECONDS. Single-thread (snapshot loop)
        // trims the head; producers (user threads) only enqueue. Lock-free.
        private const int DURATION_WINDOW_SECONDS = 5;
        private readonly ConcurrentQueue<(long Ticks, double DurationMs)> _recentDurations = new();

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
            _inFlightQueries = 0;
            _totalConnections = 0;
            _distinctUsers = 0;
            ResetWindow();
            while (_allResults.TryTake(out _)) { }
            while (_recentDurations.TryDequeue(out _)) { }
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

        public int InFlight => Volatile.Read(ref _inFlightQueries);
        public void IncrementInFlight() => Interlocked.Increment(ref _inFlightQueries);
        public void DecrementInFlight() => Interlocked.Decrement(ref _inFlightQueries);

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

                // Push successful duration into the rolling-percentile ring.
                // Bounded implicitly by the snapshot-loop trimming to ~5s.
                _recentDurations.Enqueue((DateTime.UtcNow.Ticks, result.DurationMs));

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

        /// <summary>
        /// Drain entries older than <see cref="DURATION_WINDOW_SECONDS"/> from
        /// the rolling-duration ring and return p50/p95/p99 over what remains.
        /// Called once per second from the snapshot loop. Returns
        /// (0,0,0,0) when the window is empty.
        /// </summary>
        public (double P50, double P95, double P99, int Count) ComputeDurationPercentiles()
        {
            long cutoff = DateTime.UtcNow.Ticks - (DURATION_WINDOW_SECONDS * TimeSpan.TicksPerSecond);
            while (_recentDurations.TryPeek(out var head) && head.Ticks < cutoff)
            {
                _recentDurations.TryDequeue(out _);
            }
            var snapshot = _recentDurations.ToArray();
            if (snapshot.Length == 0) return (0, 0, 0, 0);
            var arr = new double[snapshot.Length];
            for (int i = 0; i < snapshot.Length; i++) arr[i] = snapshot[i].DurationMs;
            Array.Sort(arr);
            double Pct(double p)
            {
                // Nearest-rank percentile; matches what humans expect from
                // "p95 over the last few seconds" in a load-test dashboard.
                int idx = (int)Math.Ceiling(p / 100.0 * arr.Length) - 1;
                if (idx < 0) idx = 0;
                if (idx >= arr.Length) idx = arr.Length - 1;
                return arr[idx];
            }
            return (Pct(50), Pct(95), Pct(99), arr.Length);
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
}
