using System.Collections.Generic;

namespace Bielu.PersistentQueues.Net;

public class OutgoingMessageFailure
{
    public bool ShouldRetry { get; init; }
    public required IList<Message> Messages { get; init; }
}