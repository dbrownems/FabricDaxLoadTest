// XMLA-over-ADOMD trace subscriber for FabricDaxLoadTest.
//
// Adapted from PbiLoadTester's TraceSubscriber + LiveTraceCapture.
// Power BI / Fabric workspaces reject AMO server-scoped traces with
// "Permission to create a trace is denied. Only members of the server
//  administrator role can create or subscribe to traces without a filter
//  tree." We therefore use the database-scoped XMLA pattern (per
// dbrownems/XmlaMonitorSample): open an AdomdConnection with
// `Initial Catalog=<db>`, set the `Catalog` command property on every
// Create / Subscribe / Delete, and stream events back as rows from an
// AdomdDataReader.
//
// The trace event/column definition lives in the embedded resource
// `FabricDaxLoadTest.Tracing.FilteredTrace.xmla`. We rewrite the
// <ID>/<Name> nodes per-run so each load test gets its own trace
// (no cross-run interference). The embedded XMLA ships WITHOUT a
// <Filter> clause — PBI Service rejects most filter forms on
// subscription traces (see .copilot/tracing-notes.md §3). Filtering by
// ApplicationName=FabricDaxLoadTest/<RunId> happens client-side in
// ProcessRow.
//
// Self-healing on per-event column rejection: the server may reject
// individual columns on individual events with
//   "The event Id=N does not contain the column Id=M".
// We parse the message, drop the offending <ColumnID> from the request,
// and retry. Capped at 32 attempts.
//
// IMPORTANT: keep the subscribe reader on a background thread —
// ExecuteReader on a Subscribe XMLA blocks until the FIRST event row
// arrives, which can be seconds. Doing it on the caller thread freezes
// the load test setup.

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Xml;
using Microsoft.AnalysisServices.AdomdClient;

namespace FabricDaxLoadTest.Tracing;

public sealed class TraceSubscriber : IAsyncDisposable
{
    private const string Ns = "http://schemas.microsoft.com/analysisservices/2003/engine";
    private const string EmbeddedXmlaResourceName = "FabricDaxLoadTest.Tracing.FilteredTrace.xmla";

    private readonly string _xmlaEndpoint;
    private readonly string? _token;
    private readonly string _database;
    private readonly string? _applicationFilter;
    private readonly Channel<AsTraceEvent> _channel;
    private readonly Action<string> _log;

    private AdomdConnection? _connection;
    private AdomdCommand? _subscribeCommand;
    private string? _traceId;
    private Task? _readerTask;
    private volatile bool _stopping;

    private readonly Dictionary<int, TraceColumn?> _columnIndexMap = new();

    public ChannelReader<AsTraceEvent> Events => _channel.Reader;
    public bool Connected { get; private set; }
    public string? LastError { get; private set; }
    public DateTime ConnectedAtUtc { get; private set; }
    public long EventsSeen { get; private set; }
    public long EventsDroppedByFilter { get; private set; }
    public long EventsDroppedByBackpressure { get; private set; }

    /// <summary>
    /// Fires for every event that survives the application filter, BEFORE
    /// it is written to the channel. Used for side-effect-free observation
    /// (CSV recording, perf-data file). Exceptions are caught.
    /// </summary>
    public event Action<AsTraceEvent>? OnRawEvent;

    /// <param name="applicationFilter">If non-null, drop rows whose
    /// ApplicationName column does not match exactly. The load test
    /// driver sets ApplicationName = "FabricDaxLoadTest/&lt;RunId&gt;" on
    /// its query connections; the trace is unfiltered server-side and
    /// this is the only way to isolate the run's events.</param>
    public TraceSubscriber(
        string xmlaEndpoint,
        string? token,
        string database,
        string? applicationFilter = null,
        Action<string>? log = null)
    {
        if (string.IsNullOrWhiteSpace(xmlaEndpoint))
            throw new ArgumentException("XMLA endpoint is required.", nameof(xmlaEndpoint));
        if (string.IsNullOrWhiteSpace(database))
            throw new ArgumentException(
                "Database (Initial Catalog / Dataset) is required for trace subscription. " +
                "Power BI / Fabric workspaces only allow database-scoped traces.",
                nameof(database));

        _xmlaEndpoint = xmlaEndpoint;
        _token = token;
        _database = database;
        _applicationFilter = string.IsNullOrWhiteSpace(applicationFilter) ? null : applicationFilter;
        _log = log ?? (_ => { });
        _channel = Channel.CreateBounded<AsTraceEvent>(new BoundedChannelOptions(10_000)
        {
            // Wait would back-pressure the row pump and let ADOMD time out;
            // DropOldest would silently lose QueryEnd/ExecutionMetrics rows
            // and break correlation. DropWrite drops the NEWEST event when
            // the consumer is too slow, exposed via EventsDroppedByBackpressure
            // so analysis can flag suspect runs. The 10k buffer is ample for
            // a fast Delta-table sink.
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = true,
        });
    }

    public async Task StartAsync(CancellationToken ct)
    {
        // Open + Delete + Create on the CALLER thread. Doing these on a
        // background Task.Run thread caused PBI to return a non-rowset
        // XMLA response for the subsequent Subscribe (AdomdUnknownResponse­
        // Exception "The result set returned by the server is not a
        // rowset.").
        try
        {
            _log($"Opening AdomdConnection to {_xmlaEndpoint} (catalog={_database})...");
            _connection = new AdomdConnection { ConnectionString = BuildConnectionString() };
            _connection.Open();
            _log("Connection opened.");

            _traceId = "FDLT-" + Guid.NewGuid().ToString("N").Substring(0, 12);
            _log($"Trace id: {_traceId}");

            // Pre-emptively delete any existing trace with this id (left
            // over from a crashed run). Server returns "does not exist"
            // when there's nothing to delete — swallow that.
            try
            {
                using var del = (AdomdCommand)_connection.CreateCommand();
                del.Properties.Add(new AdomdProperty("Catalog", _database));
                del.CommandText = BuildDeleteXmla(_traceId);
                del.ExecuteNonQuery();
            }
            catch (Exception ex) when (ex.Message.Contains("does not exist", StringComparison.OrdinalIgnoreCase))
            {
                // expected
            }
            catch (Exception ex)
            {
                _log("Pre-delete returned: " + Flatten(ex));
            }

            // Load the XMLA template from the embedded resource and
            // substitute the per-run trace ID/Name. Then issue Create
            // with self-healing on per-event column rejection.
            try
            {
                CreateTraceWithAutoRetry(_traceId);
                _log("Trace created.");
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    "Server rejected the trace creation. Underlying error: " + Flatten(ex), ex);
            }
        }
        catch (Exception ex)
        {
            LastError = Flatten(ex);
            Connected = false;
            _log("StartAsync FAILED: " + LastError);
            try { _connection?.Close(); } catch { }
            throw new InvalidOperationException(
                "Trace subscription failed and the load test cannot continue: " + LastError, ex);
        }

        // Subscribe runs on a background thread — ExecuteReader on a
        // Subscribe XMLA blocks until the FIRST event row arrives, which
        // can be several seconds depending on server load.
        _subscribeCommand = (AdomdCommand)_connection!.CreateCommand();
        _subscribeCommand.Properties.Add(new AdomdProperty("Catalog", _database));
        _subscribeCommand.CommandText = BuildSubscribeXmla(_traceId!);

        _readerTask = Task.Run(ReaderLoop, ct);

        Connected = true;
        ConnectedAtUtc = DateTime.UtcNow;
        _log("Trace subscription running (reader on background thread).");
        await Task.CompletedTask;
    }

    private void ReaderLoop()
    {
        try
        {
            _log("Subscribe ExecuteReader: opening (will block until first event)...");
            using var rdr = _subscribeCommand!.ExecuteReader();
            var colNames = new string[rdr.FieldCount];
            for (int i = 0; i < rdr.FieldCount; i++) colNames[i] = rdr.GetName(i);
            _log($"Subscribe reader opened ({rdr.FieldCount} columns: {string.Join(", ", colNames)}).");
            for (int i = 0; i < rdr.FieldCount; i++)
                _columnIndexMap[i] = MapColumnName(rdr.GetName(i));

            while (!_stopping && rdr.Read())
            {
                ProcessRow(rdr);
            }

            _log("Subscribe reader: stream ended.");
            _channel.Writer.TryComplete();
        }
        catch (Exception) when (_stopping)
        {
            _channel.Writer.TryComplete();
        }
        catch (Exception ex)
        {
            var detail = Flatten(ex);
            // Hint for the well-known PBI-Service-only BinaryXml issue so
            // future occurrences are easy to recognize and fix in source.
            if (detail.Contains("BinaryXml", StringComparison.OrdinalIgnoreCase))
            {
                detail += "  (HINT: a column in one of the enabled events returns " +
                          "list-shaped BinaryXml that ADOMD can't parse. Known " +
                          "offenders on PBI Service: RequestParameters, " +
                          "RequestProperties. Drop the offending <ColumnID> " +
                          "from FilteredTrace.xmla.)";
            }
            _log("Subscribe reader FAILED: " + detail);
            LastError = detail;
            Connected = false;
            _channel.Writer.TryComplete(ex);
        }
    }

    private void ProcessRow(AdomdDataReader rdr)
    {
        try
        {
            var dict = new Dictionary<TraceColumn, string?>(rdr.FieldCount);
            for (int i = 0; i < rdr.FieldCount; i++)
            {
                if (!_columnIndexMap.TryGetValue(i, out var col) || col == null) continue;
                if (rdr.IsDBNull(i)) continue;
                dict[col.Value] = rdr.GetValue(i)?.ToString();
            }

            // Application-name filter (client-side: PBI Service rejects
            // server-side <Filter> clauses on subscription traces).
            //
            // IMPORTANT: when ApplicationName is NULL on the row, the
            // event class either doesn't emit ApplicationName (e.g.
            // Event 85 VertiPaqSEQueryCacheMatch — which is restricted
            // to a small column set per .copilot/tracing-notes.md §4)
            // or AS chose not to populate it. We admit those rows
            // unconditionally and rely on downstream RequestID
            // correlation to drop non-load-test cache events. Without
            // this carve-out, every Event 85 would be filtered away —
            // a real measurement loss because cache hit-rate is one of
            // the load-test signals.
            dict.TryGetValue(TraceColumn.ApplicationName, out var appName);
            if (_applicationFilter != null
                && appName != null
                && !string.Equals(appName, _applicationFilter, StringComparison.Ordinal))
            {
                EventsDroppedByFilter++;
                return;
            }

            // Determine event class label from EventClass column (numeric or name).
            string ecLabel;
            if (dict.TryGetValue(TraceColumn.EventClass, out var ecRaw) && !string.IsNullOrEmpty(ecRaw))
            {
                if (int.TryParse(ecRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ecNum)
                    && Enum.IsDefined(typeof(TraceEventClass), ecNum))
                {
                    ecLabel = ((TraceEventClass)ecNum).ToString();
                }
                else
                {
                    ecLabel = ecRaw;
                }
            }
            else
            {
                return;
            }

            // Prefer EndTime > CurrentTime > StartTime > now. CurrentTime
            // is when AS emitted the row, which equals the event's true
            // wall clock for End-style events when EndTime isn't requested.
            var ts = ParseDate(dict.GetValueOrDefault(TraceColumn.EndTime))
                  ?? ParseDate(dict.GetValueOrDefault(TraceColumn.CurrentTime))
                  ?? ParseDate(dict.GetValueOrDefault(TraceColumn.StartTime))
                  ?? DateTime.UtcNow;

            var ev = new AsTraceEvent(
                UtcTimestamp: ts,
                EventClass: ecLabel,
                DurationMs: ParseLong(dict.GetValueOrDefault(TraceColumn.Duration)),
                CpuMs: ParseLong(dict.GetValueOrDefault(TraceColumn.CpuTime)),
                TextData: dict.GetValueOrDefault(TraceColumn.TextData),
                UserName: dict.GetValueOrDefault(TraceColumn.NTUserName),
                DatabaseName: dict.GetValueOrDefault(TraceColumn.DatabaseName),
                ApplicationName: appName,
                SessionId: dict.GetValueOrDefault(TraceColumn.SessionID),
                RequestId: dict.GetValueOrDefault(TraceColumn.RequestID),
                ActivityID: dict.GetValueOrDefault(TraceColumn.ActivityID));

            // Channel write FIRST so a slow OnRawEvent observer can't
            // drop events from the live aggregator pipeline. TryWrite
            // returns false when the bounded channel is full under
            // DropWrite mode (consumer slower than producer); we count
            // those as backpressure drops rather than letting them
            // silently corrupt the QueryEnd/ExecutionMetrics correlator.
            if (_channel.Writer.TryWrite(ev))
            {
                EventsSeen++;
            }
            else
            {
                EventsDroppedByBackpressure++;
            }

            try { OnRawEvent?.Invoke(ev); } catch { /* never let observer crash trace */ }
        }
        catch
        {
            // Never let row-handler exceptions kill the subscription.
        }
    }

    private static TraceColumn? MapColumnName(string name)
    {
        if (string.IsNullOrEmpty(name)) return null;
        if (Enum.TryParse<TraceColumn>(name, ignoreCase: true, out var col)) return col;
        return name.ToUpperInvariant() switch
        {
            "CPUTIME" => TraceColumn.CpuTime,
            "SESSIONID" => TraceColumn.SessionID,
            "REQUESTID" => TraceColumn.RequestID,
            "NTUSERNAME" => TraceColumn.NTUserName,
            "SPID" => TraceColumn.Spid,
            "CONNECTIONID" => TraceColumn.ConnectionID,
            "OBJECTID" => TraceColumn.ObjectID,
            _ => null,
        };
    }

    private static DateTime? ParseDate(string? s)
        => DateTime.TryParse(s, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out var d) ? d : null;

    private static long ParseLong(string? s) => long.TryParse(s, out var v) ? v : 0L;

    // -- XMLA construction ---------------------------------------------

    /// <summary>Loads the embedded trace template and substitutes the
    /// per-run trace ID/Name. Returns the parsed XmlDocument so callers
    /// can mutate it (e.g. to drop a rejected ColumnID and retry).</summary>
    private static XmlDocument LoadCreateTemplate(string traceId)
    {
        var asm = typeof(TraceSubscriber).Assembly;
        using var stream = asm.GetManifestResourceStream(EmbeddedXmlaResourceName)
            ?? throw new InvalidOperationException(
                $"Embedded resource '{EmbeddedXmlaResourceName}' not found. " +
                "Verify <EmbeddedResource> in QueryRunner.csproj.");
        using var sr = new StreamReader(stream, Encoding.UTF8);
        var doc = new XmlDocument { PreserveWhitespace = false };
        doc.LoadXml(sr.ReadToEnd());

        var nsm = new XmlNamespaceManager(doc.NameTable);
        nsm.AddNamespace("e", Ns);

        // Replace <ID> and <Name> under <Trace>.
        var idNode = doc.SelectSingleNode("//e:Trace/e:ID", nsm)
            ?? throw new InvalidOperationException("Trace XMLA missing <ID> element.");
        var nameNode = doc.SelectSingleNode("//e:Trace/e:Name", nsm)
            ?? throw new InvalidOperationException("Trace XMLA missing <Name> element.");
        idNode.InnerText = traceId;
        nameNode.InnerText = traceId;

        return doc;
    }

    // Server may reject individual columns on individual events with
    // "The event Id=N does not contain the column Id=M". Self-heal by
    // removing the offending <ColumnID> from the request and retrying.
    // Capped at 32 attempts. See .copilot/tracing-notes.md §4.
    private static readonly Regex ColumnRejectRegex =
        new(@"event\s+Id=(\d+)\s+does not contain the column\s+Id=(\d+)",
            RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private void CreateTraceWithAutoRetry(string traceId)
    {
        const int maxAttempts = 32;
        var doc = LoadCreateTemplate(traceId);

        for (int attempt = 0; attempt < maxAttempts; attempt++)
        {
            try
            {
                using var cmd = (AdomdCommand)_connection!.CreateCommand();
                cmd.Properties.Add(new AdomdProperty("Catalog", _database));
                cmd.CommandText = doc.OuterXml;
                cmd.ExecuteNonQuery();
                return;
            }
            catch (Exception ex)
            {
                var msg = ex.Message ?? "";
                var inner = ex.InnerException?.Message ?? "";
                var both = msg + " | " + inner;
                var m = ColumnRejectRegex.Match(both);
                if (!m.Success) throw;
                if (!int.TryParse(m.Groups[1].Value, out var eventId) ||
                    !int.TryParse(m.Groups[2].Value, out var columnId)) throw;

                if (!RemoveColumnFromTemplate(doc, eventId, columnId))
                {
                    // Couldn't locate the rejected column in our plan —
                    // give up rather than retry the same payload.
                    throw;
                }
                _log($"Server rejected ColumnID={columnId} on EventID={eventId}; dropping and retrying (attempt {attempt + 1}/{maxAttempts})");

                // Best-effort: AS may have left a half-created trace behind.
                try { TryDeleteTrace(traceId); } catch { }
            }
        }
        throw new InvalidOperationException(
            $"Trace Create rejected too many columns ({maxAttempts}); event/column matrix may be unsupported on this server.");
    }

    /// <summary>Removes &lt;ColumnID&gt;<paramref name="columnId"/>&lt;/ColumnID&gt;
    /// from &lt;Event&gt;&lt;EventID&gt;<paramref name="eventId"/>&lt;/EventID&gt;.
    /// If the event has no remaining columns, removes the &lt;Event&gt; entirely.
    /// Returns false if the column wasn't found (caller should give up).</summary>
    private static bool RemoveColumnFromTemplate(XmlDocument doc, int eventId, int columnId)
    {
        var nsm = new XmlNamespaceManager(doc.NameTable);
        nsm.AddNamespace("e", Ns);

        var eventNode = doc.SelectSingleNode(
            $"//e:Event[e:EventID='{eventId.ToString(CultureInfo.InvariantCulture)}']", nsm);
        if (eventNode == null) return false;

        var colNode = eventNode.SelectSingleNode(
            $"e:Columns/e:ColumnID[text()='{columnId.ToString(CultureInfo.InvariantCulture)}']", nsm);
        if (colNode == null) return false;

        var columnsNode = colNode.ParentNode!;
        columnsNode.RemoveChild(colNode);

        if (columnsNode.ChildNodes.Count == 0)
        {
            // No remaining columns — remove the whole <Event>.
            eventNode.ParentNode!.RemoveChild(eventNode);
        }
        return true;
    }

    private static string BuildSubscribeXmla(string traceId)
    {
        var sb = new StringBuilder();
        using (var w = XmlWriter.Create(sb, new XmlWriterSettings { OmitXmlDeclaration = true, Indent = false }))
        {
            w.WriteStartElement("Subscribe", Ns);
            w.WriteStartElement("Object", Ns);
            w.WriteElementString("TraceID", Ns, traceId);
            w.WriteEndElement();
            w.WriteEndElement();
        }
        return sb.ToString();
    }

    private static string BuildDeleteXmla(string traceId)
    {
        var sb = new StringBuilder();
        using (var w = XmlWriter.Create(sb, new XmlWriterSettings { OmitXmlDeclaration = true, Indent = false }))
        {
            w.WriteStartElement("Delete", Ns);
            w.WriteStartElement("Object", Ns);
            w.WriteElementString("TraceID", Ns, traceId);
            w.WriteEndElement();
            w.WriteEndElement();
        }
        return sb.ToString();
    }

    private void TryDeleteTrace(string traceId)
    {
        try
        {
            if (_connection == null || _connection.State != ConnectionState.Open) return;
            using var cmd = (AdomdCommand)_connection.CreateCommand();
            cmd.Properties.Add(new AdomdProperty("Catalog", _database));
            cmd.CommandText = BuildDeleteXmla(traceId);
            cmd.ExecuteNonQuery();
        }
        catch { /* trace will be GC'd */ }
    }

    private string BuildConnectionString()
    {
        var sb = new StringBuilder();
        AppendKv(sb, "Data Source", _xmlaEndpoint);
        AppendKv(sb, "Initial Catalog", _database);
        AppendKv(sb, "Connect Timeout", "60");
        // The trace connection's ApplicationName is intentionally
        // distinct from the load driver's ("FabricDaxLoadTest/<RunId>"),
        // so trace events generated by this connection itself don't get
        // matched by the client-side filter. (We don't expect any, but
        // belt-and-braces.)
        AppendKv(sb, "Application Name", "FabricDaxLoadTest.Trace");
        if (!string.IsNullOrWhiteSpace(_token))
        {
            AppendKv(sb, "Password", _token);
            AppendKv(sb, "User ID", "");
        }
        return sb.ToString();
    }

    private static void AppendKv(StringBuilder sb, string key, string value)
    {
        sb.Append(key).Append('=');
        bool needsQuote = value.IndexOfAny(new[] { ';', '=', '"', ' ', '\t' }) >= 0;
        if (needsQuote)
            sb.Append('"').Append(value.Replace("\"", "\"\"")).Append('"');
        else
            sb.Append(value);
        sb.Append(';');
    }

    private static string Flatten(Exception ex)
    {
        var msgs = new List<string>();
        for (var e = ex; e != null; e = e.InnerException)
            if (!string.IsNullOrWhiteSpace(e.Message)) msgs.Add(e.Message.Trim());
        var deduped = new List<string>();
        foreach (var m in msgs) if (deduped.Count == 0 || deduped[^1] != m) deduped.Add(m);
        return string.Join(" → ", deduped);
    }

    public async ValueTask DisposeAsync()
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        _stopping = true;
        try { _subscribeCommand?.Cancel(); } catch { }
        try { _log($"DisposeAsync: stopping=true, command.Cancel issued at {sw.Elapsed.TotalMilliseconds:F0}ms"); } catch { }

        if (_readerTask != null)
        {
            try { await Task.WhenAny(_readerTask, Task.Delay(TimeSpan.FromSeconds(5))).ConfigureAwait(false); } catch { }
            try { _log($"DisposeAsync: reader task wait complete at {sw.Elapsed.TotalSeconds:F1}s (status={_readerTask.Status})"); } catch { }
        }

        // Issue Delete on a fresh connection — the subscribe connection
        // may still have an active reader and reject new commands.
        // Bound with a hard timeout so a hung server can't keep the run
        // "Stopping" forever.
        if (_traceId != null)
        {
            var deleteTask = Task.Run(() =>
            {
                try
                {
                    using var cleanup = new AdomdConnection { ConnectionString = BuildConnectionString() };
                    cleanup.Open();
                    using var cmd = (AdomdCommand)cleanup.CreateCommand();
                    cmd.Properties.Add(new AdomdProperty("Catalog", _database));
                    cmd.CommandText = BuildDeleteXmla(_traceId);
                    cmd.ExecuteNonQuery();
                }
                catch { /* trace will be GC'd */ }
            });
            var deleteStart = sw.Elapsed;
            var winner = await Task.WhenAny(deleteTask, Task.Delay(TimeSpan.FromSeconds(15))).ConfigureAwait(false);
            try { _log($"DisposeAsync: cleanup Delete {(winner == deleteTask ? "completed" : "TIMED OUT")} after {(sw.Elapsed - deleteStart).TotalSeconds:F1}s"); } catch { }
        }

        try { _subscribeCommand?.Dispose(); } catch { }
        try { _connection?.Close(); } catch { }
        try { _connection?.Dispose(); } catch { }

        _channel.Writer.TryComplete();
        try { _log($"DisposeAsync: complete in {sw.Elapsed.TotalSeconds:F1}s"); } catch { }
    }
}
