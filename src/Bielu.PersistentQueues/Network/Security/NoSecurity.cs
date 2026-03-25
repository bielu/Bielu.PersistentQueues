using System;
using System.IO;
using System.Threading.Tasks;

namespace Bielu.PersistentQueues.Network.Security;

public class NoSecurity : IStreamSecurity
{
    public ValueTask<Stream> Apply(Uri endpoint, Stream stream)
    {
        return new ValueTask<Stream>(stream);
    }
}