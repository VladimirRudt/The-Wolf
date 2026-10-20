using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;

namespace LeakyOrderService;

/// <summary>Publishes a price change every second, so leaked handlers keep running.</summary>
public sealed class PriceTicker : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            PriceFeed.Publish($"SKU-{Random.Shared.Next(1000):D4}", Random.Shared.Next(100, 10_000) / 100m);
        }
    }
}

/// <summary>Sends POST /orders to this app at Load:RequestsPerSecond, so it leaks without an external load tool.</summary>
public sealed class LoadGenerator(
    IServer server,
    IHostApplicationLifetime lifetime,
    IHttpClientFactory httpClientFactory,
    IConfiguration configuration,
    ILogger<LoadGenerator> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var requestsPerSecond = configuration.GetValue("Load:RequestsPerSecond", 20);
        if (requestsPerSecond <= 0)
        {
            return;
        }

        var started = new TaskCompletionSource();
        using (lifetime.ApplicationStarted.Register(() => started.TrySetResult()))
        {
            await started.Task.WaitAsync(stoppingToken);
        }

        var address = server.Features.Get<IServerAddressesFeature>()?.Addresses.FirstOrDefault()
            ?? throw new InvalidOperationException("Server has no listening address.");
        var client = httpClientFactory.CreateClient();
        client.BaseAddress = new Uri(address);
        logger.LogInformation("Sending {Rate} requests/s to {Address}/orders", requestsPerSecond, address);

        // Ticks of 100 ms: PeriodicTimer is too coarse on Windows (~15 ms) for one request per tick.
        var perTick = Math.Max(1, requestsPerSecond / 10);
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            var requests = Enumerable.Range(0, perTick)
                .Select(_ => client.PostAsync("/orders", content: null, stoppingToken));
            try
            {
                foreach (var response in await Task.WhenAll(requests))
                {
                    response.Dispose();
                }
            }
            catch (HttpRequestException ex)
            {
                logger.LogWarning("Load request failed: {Message}", ex.Message);
            }
        }
    }
}
