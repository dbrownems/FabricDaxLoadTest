using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

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
}
