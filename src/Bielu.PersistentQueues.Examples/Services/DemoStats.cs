using System.Threading;

namespace Bielu.PersistentQueues.Examples.Services;

internal sealed class DemoStats
{
    private int _ordersSent;
    private int _prioritySent;
    private int _ordersProcessed;
    private int _priorityProcessed;

    public int OrdersSent => _ordersSent;
    public int PrioritySent => _prioritySent;
    public int OrdersProcessed => _ordersProcessed;
    public int PriorityProcessed => _priorityProcessed;

    public void IncrementOrdersSent() => Interlocked.Increment(ref _ordersSent);
    public void IncrementPrioritySent() => Interlocked.Increment(ref _prioritySent);
    public void AddOrdersProcessed(int count) => Interlocked.Add(ref _ordersProcessed, count);
    public void AddPriorityProcessed(int count) => Interlocked.Add(ref _priorityProcessed, count);
}
