// Trace event models for the FabricDaxLoadTest XMLA-over-ADOMD subscriber.
//
// Two record types:
//   AsTraceEvent — narrow shape consumed by the load-test correlator
//   (downstream code pairs QueryEnd + ExecutionMetrics rows by RequestID
//   to produce per-query telemetry). Adapted from PbiLoadTester's record
//   of the same name; payload-bearing event-specific data lives in
//   TextData (e.g. ExecutionMetrics carries CPU/memory JSON there).
//
// Plus minimal local enums TraceColumn and TraceEventClass so we don't
// have to drag in the full Microsoft.AnalysisServices (AMO) NuGet
// package — that package is ~6 MB across multiple DLLs and we'd be
// using exactly two enum types from it. The integer values used here
// are documented constants of the Analysis Services trace schema and
// have been stable since SQL 2005.

using System;

namespace FabricDaxLoadTest.Tracing;

/// <summary>
/// One trace row emitted by the AS engine, normalized to the narrow
/// shape used by load-test correlation. Untyped fields (TextData) carry
/// event-specific payloads — e.g. for ExecutionMetrics, TextData is a
/// JSON document with vertipaqJobCpuTimeMs / queryProcessingCpuTimeMs.
/// </summary>
public sealed record AsTraceEvent(
    DateTime UtcTimestamp,
    string EventClass,
    long DurationMs,
    long CpuMs,
    string? TextData,
    string? UserName,
    string? DatabaseName,
    string? ApplicationName,
    string? SessionId,
    string? RequestId);

/// <summary>
/// Subset of AMO's TraceColumn enum we actually consume. Values match
/// the AS trace ColumnID schema. Keep additions ordered by ColumnID for
/// easy reconciliation against the Analysis Services trace
/// documentation.
/// </summary>
internal enum TraceColumn
{
    EventClass = 0,
    EventSubclass = 1,
    CurrentTime = 2,
    StartTime = 3,
    EndTime = 4,
    Duration = 5,
    CpuTime = 6,
    IntegerData = 10,
    ObjectID = 11,
    ObjectName = 13,
    Error = 24,
    ConnectionID = 25,
    DatabaseName = 28,
    NTUserName = 32,
    NTDomainName = 33,
    ClientProcessID = 36,
    ApplicationName = 37,
    SessionID = 39,
    Spid = 41,
    TextData = 42,
    RequestID = 47,
    Identity = 60,
}

/// <summary>
/// Subset of AMO's TraceEventClass enum we actually subscribe to.
/// Values match the AS trace EventID schema.
/// </summary>
internal enum TraceEventClass
{
    ProgressReportBegin = 5,
    ProgressReportEnd = 6,
    QueryBegin = 9,
    QueryEnd = 10,
    CommandBegin = 15,
    CommandEnd = 16,
    Error = 17,
    VertiPaqSEQueryBegin = 82,
    VertiPaqSEQueryEnd = 83,
    VertiPaqSEQueryCacheMatch = 85,
    DirectQueryBegin = 98,
    DirectQueryEnd = 99,
    AggregateTableRewriteQuery = 112,
    JobGraph = 134,
    ExecutionMetrics = 136,
}

