using System.IO;
using System.Text;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests;

// Queues log from their background send/receive loops while the test thread logs too (e.g. from
// Dispose), so every access to the shared line buffer is serialized. Without the lock, concurrent
// StringBuilder mutation threw ArgumentOutOfRangeException ('chunkLength') and failed tests at random.
public class TestOutputHelperWriter(ITestOutputHelper output) : TextWriter
{
    private readonly StringBuilder _buffer = new();
    private readonly Lock _lock = new();

    public override Encoding Encoding => Encoding.UTF8;

    public override void Write(char value)
    {
        lock (_lock)
        {
            WriteUnlocked(value);
        }
    }

    public override void Write(string? value)
    {
        if (value == null) return;

        lock (_lock)
        {
            foreach (var ch in value)
            {
                WriteUnlocked(ch);
            }
        }
    }

    public override void WriteLine(string? value)
    {
        lock (_lock)
        {
            if (value != null)
            {
                foreach (var ch in value)
                {
                    WriteUnlocked(ch);
                }
            }
            FlushLine();
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            lock (_lock)
            {
                if (_buffer.Length > 0)
                {
                    FlushLine();
                }
            }
        }
        base.Dispose(disposing);
    }

    private void WriteUnlocked(char value)
    {
        if (value == '\n')
        {
            FlushLine();
        }
        else if (value != '\r')
        {
            _buffer.Append(value);
        }
    }

    private void FlushLine()
    {
        output.WriteLine(_buffer.ToString());
        _buffer.Clear();
    }
}
