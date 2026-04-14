using System.IO;
using System.Text;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests;

public class TestOutputHelperWriter(ITestOutputHelper output) : TextWriter
{
    private readonly ITestOutputHelper _output = output;
    private readonly StringBuilder _buffer = new();

    public override Encoding Encoding => Encoding.UTF8;

    public override void Write(char value)
    {
        if (value == '\n')
        {
            _output.WriteLine(_buffer.ToString());
            _buffer.Clear();
        }
        else if (value != '\r')
        {
            _buffer.Append(value);
        }
    }

    public override void Write(string? value)
    {
        if (value == null) return;

        foreach (var ch in value)
        {
            Write(ch);
        }
    }

    public override void WriteLine(string? value)
    {
        Write(value);
        _output.WriteLine(_buffer.ToString());
        _buffer.Clear();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && _buffer.Length > 0)
        {
            _output.WriteLine(_buffer.ToString());
            _buffer.Clear();
        }
        base.Dispose(disposing);
    }
}
