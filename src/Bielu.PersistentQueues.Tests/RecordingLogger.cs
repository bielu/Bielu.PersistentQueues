using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Logging;

namespace Bielu.PersistentQueues.Logging;

// Queues log from their background loops while the test thread reads the recorded messages, so
// writes are serialized and the message properties return snapshots rather than the live lists.
public class RecordingLogger(TextWriter? console = null, LogLevel logLevel = LogLevel.Debug) : ILogger
{
    private readonly List<string> _debug = new();
    private readonly List<string> _error = new();
    private readonly List<string> _info = new();
    private readonly Lock _lock = new();

    public IEnumerable<string> DebugMessages => Snapshot(_debug);
    public IEnumerable<string> InfoMessages => Snapshot(_info);

    public IEnumerable<string> ErrorMessages => Snapshot(_error);

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        var list = logLevel switch
        {
            LogLevel.Trace => _debug,
            LogLevel.Debug => _debug,
            LogLevel.Information => _info,
            LogLevel.Warning => _error,
            LogLevel.Error => _error,
            LogLevel.Critical => _error,
            _ => null // LogLevel.None - ignore
        };
        if (list == null)
            return;
        var message = formatter(state, exception);
        lock (_lock)
        {
            list.Add(message);
        }
        console?.WriteLine(message + exception);
    }

    private string[] Snapshot(List<string> list)
    {
        lock (_lock)
        {
            return list.ToArray();
        }
    }

    public bool IsEnabled(LogLevel level) => level switch
    {
        LogLevel.Debug when logLevel == LogLevel.Debug => true,
        LogLevel.Information when logLevel is LogLevel.Debug or LogLevel.Information => true,
        LogLevel.Error when logLevel is LogLevel.Debug 
            or LogLevel.Information 
            or LogLevel.Error => true,
        _ => false
    };

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        return null;
    }
}