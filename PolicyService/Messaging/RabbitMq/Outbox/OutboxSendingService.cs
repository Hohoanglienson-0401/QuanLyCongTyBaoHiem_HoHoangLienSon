using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace PolicyService.Messaging.RabbitMq.Outbox;

public class OutboxSendingService : IHostedService
{
    private static readonly SemaphoreSlim semaphore = new SemaphoreSlim(1);
    private readonly Outbox outbox;
    private Timer timer;

    public OutboxSendingService(Outbox outbox)
    {
        this.outbox = outbox;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        timer = new Timer(
            PushMessages,
            null,
            TimeSpan.Zero,
            TimeSpan.FromSeconds(1)
        );

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        timer?.Change(Timeout.Infinite, 0);
        timer?.Dispose();
        return Task.CompletedTask;
    }
    private async void PushMessages(object state)
    {
        if (!await semaphore.WaitAsync(0))
            return;

        try
        {
            await outbox.PushPendingMessages();
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
        }
        finally
        {
            semaphore.Release();
        }
    }
}