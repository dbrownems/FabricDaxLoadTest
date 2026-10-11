namespace FabricDaxLoadTest
{
    /// <summary>
    /// Per-iteration outcome captured by <c>ExecuteQuery</c> and consumed
    /// by <see cref="QueryRunnerStatus"/> + the executions CSV writer.
    /// </summary>
    internal sealed class QueryResult
    {
        public int UserIndex { get; set; }
        public int QueryIndex { get; set; }
        public int Iteration { get; set; }
        public int QuerySeq { get; set; }
        public int RowCount { get; set; }
        public double DurationMs { get; set; }
        public double StartTimeMs { get; set; }
        public string? Error { get; set; }
        /// <summary>Snapshot of QueryRunnerStatus.ActiveUsers at the moment this query started executing.</summary>
        public int ActiveUsersAtStart { get; set; }
    }
}
