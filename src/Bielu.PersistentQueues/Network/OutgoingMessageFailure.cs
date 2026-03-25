using System.Collections.Generic;

namespace Bielu.PersistentQueues.Network;

public class OutgoingMessageFailure
{
    public bool ShouldRetry { get; init; }
    public required IList<Message> Messages { get; init; }
}