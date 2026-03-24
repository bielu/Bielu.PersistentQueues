using System.IO;
using System.Text;
using Xunit.Abstractions;

namespace LightningQueues.Tests;

public class TestOutputHelperWriter : TextWriter
{
    private readonly ITestOutputHelper _output;
    private readonly StringBuilder _buffer = new();

    public TestOutputHelperWriter(ITestOutputHelper output)
    {
        _output = output;
    }

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
