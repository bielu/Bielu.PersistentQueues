using System;
using System.Linq;
using System.Net;
using Bielu.PersistentQueues.Network;
using Bielu.PersistentQueues.Network.Protocol;
using Bielu.PersistentQueues.Network.Protocol.V1;
using Bielu.PersistentQueues.Network.Security;
using Bielu.PersistentQueues.Network.Tcp;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Bielu.PersistentQueues;

/// <summary>
/// Extension methods for registering Bielu.PersistentQueues services with Microsoft DI.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds Bielu.PersistentQueues services to the specified <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configure">A delegate to configure the <see cref="PersistentQueuesBuilder"/>.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddPersistentQueues(
        this IServiceCollection services,
        Action<PersistentQueuesBuilder> configure)
    {
        var builder = new PersistentQueuesBuilder(services);
        configure(builder);
        builder.Build();
        return services;
    }
}

/// <summary>
/// Builder for configuring Bielu.PersistentQueues services.
/// </summary>
public class PersistentQueuesBuilder(IServiceCollection services)
{
    private IPEndPoint? _endpoint;
    private TimeSpan _networkBatchTimeout = TimeSpan.FromSeconds(5);
    private IStreamSecurity _sendingSecurity = new NoSecurity();
    private IStreamSecurity _receivingSecurity = new NoSecurity();
    private string[] _queueNames = [];
    private bool _autoStart { get; set; }
    private Func<IServiceProvider, IMessageStore>? _storeFactory;
    private readonly DeadLetterOptions _deadLetterOptions = new();

    /// <summary>
    /// Initializes a new instance of <see cref="PersistentQueuesBuilder"/>.
    /// </summary>
    /// <param name="services">The service collection being configured.</param>

    /// <summary>
    /// Gets the service collection being configured.
    /// </summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Configures the endpoint to listen on for incoming messages.
    /// </summary>
    /// <param name="endpoint">The IP endpoint to listen on.</param>
    /// <returns>The builder for chaining.</returns>
    public PersistentQueuesBuilder ListenOn(IPEndPoint endpoint)
    {
        _endpoint = endpoint;
        return this;
    }

    /// <summary>
    /// Configures the queue to use an automatically assigned endpoint on the local machine.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public PersistentQueuesBuilder AutomaticEndpoint()
    {
        _endpoint = new IPEndPoint(IPAddress.Loopback, PortFinder.FindPort());
        return this;
    }

    /// <summary>
    /// Sets the timeout for batching network messages.
    /// </summary>
    /// <param name="timeout">The batch timeout duration.</param>
    /// <returns>The builder for chaining.</returns>
    public PersistentQueuesBuilder NetworkBatchTimeout(TimeSpan timeout)
    {
        _networkBatchTimeout = timeout;
        return this;
    }

    /// <summary>
    /// Configures the security mechanisms for sending and receiving messages.
    /// </summary>
    /// <param name="sending">The security configuration for outgoing messages.</param>
    /// <param name="receiving">The security configuration for incoming messages.</param>
    /// <returns>The builder for chaining.</returns>
    public PersistentQueuesBuilder SecureTransportWith(IStreamSecurity sending, IStreamSecurity receiving)
    {
        _sendingSecurity = sending;
        _receivingSecurity = receiving;
        return this;
    }

    /// <summary>
    /// Specifies queue names to create when the queue starts.
    /// </summary>
    /// <param name="queueNames">The names of the queues to create.</param>
    /// <returns>The builder for chaining.</returns>
    public PersistentQueuesBuilder CreateQueues(params string[] queueNames)
    {
        _queueNames = _queueNames.Concat(queueNames).ToArray();
        return this;
    }

    /// <summary>
    /// Configures the message store implementation to use.
    /// </summary>
    /// <param name="storeFactory">A factory that creates the message store from the service provider.</param>
    /// <returns>The builder for chaining.</returns>
    public PersistentQueuesBuilder UseStorage(Func<IServiceProvider, IMessageStore> storeFactory)
    {
        _storeFactory = storeFactory;
        return this;
    }
    public PersistentQueuesBuilder AutoStart()
    {
        _autoStart = true;
        return this;
    }

    /// <summary>
    /// Enables the dead letter queue.
    /// Messages that exceed their <see cref="Message.MaxAttempts"/> or fail all send
    /// retries are automatically moved to the shared <c>dead-letter</c> queue.
    /// The DLQ is disabled by default.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public PersistentQueuesBuilder WithDeadLetterQueue()
    {
        _deadLetterOptions.Enabled = true;
        return this;
    }

    internal void Build()
    {
        var endpoint = _endpoint ?? new IPEndPoint(IPAddress.Loopback, PortFinder.FindPort());
        var sendingSecurity = _sendingSecurity;
        var receivingSecurity = _receivingSecurity;
        var networkBatchTimeout = _networkBatchTimeout;
        var queueNames = _queueNames;
        var storeFactory = _storeFactory;

        Services.TryAddSingleton<IMessageSerializer, MessageSerializer>();
        Services.TryAddSingleton<IContentSerializer>(JsonContentSerializer.Default);

        if (storeFactory != null)
        {
            Services.TryAddSingleton(storeFactory);
        }

        Services.TryAddSingleton<IReceivingProtocol>(sp =>
        {
            var store = sp.GetRequiredService<IMessageStore>();
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var logger = sp.GetRequiredService<ILogger<ReceivingProtocol>>();
            return new ReceivingProtocol(store, receivingSecurity, serializer, new Uri($"lq.tcp://{endpoint}"), logger);
        });

        Services.TryAddSingleton<ISendingProtocol>(sp =>
        {
            var store = sp.GetRequiredService<IMessageStore>();
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var logger = sp.GetRequiredService<ILogger<SendingProtocol>>();
            return new SendingProtocol(store, sendingSecurity, serializer, logger);
        });

        Services.TryAddSingleton(sp =>
        {
            var protocol = sp.GetRequiredService<IReceivingProtocol>();
            var logger = sp.GetRequiredService<ILogger<Receiver>>();
            return new Receiver(endpoint, protocol, logger);
        });

        Services.TryAddSingleton(sp =>
        {
            var protocol = sp.GetRequiredService<ISendingProtocol>();
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var logger = sp.GetRequiredService<ILogger<Sender>>();
            return new Sender(protocol, serializer, logger, networkBatchTimeout);
        });

        Services.TryAddSingleton<IQueue>(sp =>
        {
            var receiver = sp.GetRequiredService<Receiver>();
            var sender = sp.GetRequiredService<Sender>();
            var store = sp.GetRequiredService<IMessageStore>();
            var logger = sp.GetRequiredService<ILogger<Queue>>();
            var contentSerializer = sp.GetRequiredService<IContentSerializer>();
            var queue = new Queue(receiver, sender, store, logger, contentSerializer, _deadLetterOptions);
            if (queueNames != null)
            {
                foreach (var name in queueNames)
                {
                    queue.CreateQueue(name);
                }
            }

            if (_autoStart)
            {
                queue.Start();
            }
            return queue;
        });
    }
}