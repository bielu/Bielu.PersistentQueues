using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LightningDB;
using Bielu.PersistentQueues.Serialization;
using Bielu.PersistentQueues.Storage.LMDB;
using Xunit.Abstractions;

namespace Bielu.PersistentQueues.Tests;

public class TestBase(ITestOutputHelper? output = null)
{
   private static readonly string _tempPath = Path.Combine(Path.GetTempPath(), $"lightningqueuestests-{Environment.Version.ToString()}");
   protected ITestOutputHelper? Output { get; set; } = output;
   protected TextWriter? OutputWriter => Output != null ? new TestOutputHelperWriter(Output) : null;

   protected static Task DeterministicDelayAsync(int delayMs, CancellationToken token)
   {
       if (token.IsCancellationRequested)
           return Task.FromCanceled(token);

       int actualDelay = Math.Max(delayMs, 10);
            
       var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
       
       var timer = new Timer(_ => tcs.TrySetResult(), null, actualDelay, Timeout.Infinite);
       
       token.Register(() => 
       {
           timer.Dispose();
           tcs.TrySetCanceled(token);
       }, useSynchronizationContext: false);
       
       tcs.Task.ContinueWith(_ => timer.Dispose(), TaskContinuationOptions.ExecuteSynchronously);
       
       return tcs.Task;
   }
   
   protected static Task DeterministicDelayAsync(TimeSpan delay, CancellationToken token)
   {
       return DeterministicDelayAsync((int)delay.TotalMilliseconds, token);
   }

   protected async Task QueueScenarioAsync(Action<QueueConfiguration> queueBuilder,
      Func<IQueue, CancellationToken, Task> scenario, TimeSpan timeout, string queueName = "test")
   {
      using var cancellation = new CancellationTokenSource(timeout);
      var serializer = new MessageSerializer();
      using var env = LightningEnvironment();
      var queueConfiguration = new QueueConfiguration()
         .WithDefaultsForTest(Output)
         .SerializeWith(serializer)
         .StoreWithLmdb(() => env);
      queueBuilder(queueConfiguration);
      using var queue = queueConfiguration.BuildAndStartQueue(queueName);
      await scenario(queue, cancellation.Token);
      await cancellation.CancelAsync();
   }
   
   protected Task QueueScenarioAsync(Action<QueueConfiguration> queueBuilder,
      Func<IQueue, CancellationToken, Task> scenario, string queueName = "test")
   {
      return QueueScenarioAsync(queueBuilder, scenario, TimeSpan.FromSeconds(1), queueName);
   }

   protected Task QueueScenarioAsync(Func<IQueue, CancellationToken, Task> scenario, TimeSpan timeout,
      string queueName = "test")
   {
      return QueueScenarioAsync(config => { }, scenario, timeout, queueName);
   }

   protected Task QueueScenarioAsync(Func<IQueue, CancellationToken, Task> scenario, string queueName = "test")
   {
      return QueueScenarioAsync(scenario, TimeSpan.FromSeconds(1), queueName);
   }
   
   protected void StorageScenario(Action<LmdbMessageStore> action)
   {
      using var env = LightningEnvironment();
      using var store = new LmdbMessageStore(env, new MessageSerializer());
      store.CreateQueue("test");
      action(store);
   }

   protected LightningEnvironment LightningEnvironment(string? path = null)
   {
      return new LightningEnvironment(path ?? TempPath(), new EnvironmentConfiguration { MaxDatabases = 20, MapSize = 1024 * 1024 * 100 });
   }

   protected static Message NewMessage(string queueName = "test", string payload = "hello")
   {
      return Message.Create(
         data: Encoding.UTF8.GetBytes(payload),
         queue: queueName
      );
   }

   public static string TempPath()
   {
      var path = Path.Combine(_tempPath, Guid.NewGuid().ToString());
      Directory.CreateDirectory(path);
      return path;
   }
   
   public static void CleanupSession()
   {
      if(Directory.Exists(_tempPath))
         Directory.Delete(_tempPath, true);
   }
}