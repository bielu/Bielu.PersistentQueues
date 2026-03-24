using System.IO;
using System.Text;
using Xunit.Abstractions;

namespace LightningQueues.Tests;

public class TestOutputHelperWriter : TextWriter
{
    private readonly ITestOutputHelper _output;

    public TestOutputHelperWriter(ITestOutputHelper output)
    {
        _output = output;
    }

    public override Encoding Encoding => Encoding.UTF8;

    public override void WriteLine(string? value)
    {
        _output.WriteLine(value ?? string.Empty);
    }

    public override void Write(string? value)
    {
        if (value != null)
            _output.WriteLine(value);
    }
}
