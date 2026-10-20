namespace LeakyOrderService;

public sealed class PriceChangedEventArgs(string sku, decimal price) : EventArgs
{
    public string Sku { get; } = sku;
    public decimal Price { get; } = price;
}

public static class PriceFeed
{
    public static event EventHandler<PriceChangedEventArgs>? PriceChanged;

    public static int SubscriberCount => PriceChanged?.GetInvocationList().Length ?? 0;

    public static void Publish(string sku, decimal price) =>
        PriceChanged?.Invoke(null, new PriceChangedEventArgs(sku, price));
}

public sealed class OrderLine(string sku, int quantity, decimal price)
{
    public string Sku { get; } = sku;
    public int Quantity { get; } = quantity;
    public decimal Price { get; set; } = price;
}

public sealed class Order(Guid id, List<OrderLine> lines)
{
    public Guid Id { get; } = id;
    public List<OrderLine> Lines { get; } = lines;

    public static Order CreateRandom()
    {
        var lines = new List<OrderLine>();
        for (var i = 0; i < 20; i++)
        {
            lines.Add(new OrderLine($"SKU-{Random.Shared.Next(1000):D4}", Random.Shared.Next(1, 10), 9.99m));
        }

        return new Order(Guid.NewGuid(), lines);
    }
}

/// <summary>A per-request view model that keeps its lines' prices live. The leak: it never unsubscribes.</summary>
public sealed class OrderView : IDisposable
{
    private readonly List<OrderLine> _lines;

    // Pre-rendered page fragment: ~32 KB per leaked view (below the 85 KB LOH threshold),
    // so growth is obvious in dotnet-counters within a minute even with Server GC's large gen0 budget.
    private readonly byte[] _renderCache = new byte[32 * 1024];

    public OrderView(Order order)
    {
        _lines = order.Lines;
        PriceFeed.PriceChanged += OnPriceChanged;
    }

    public decimal Total => _lines.Sum(l => l.Price * l.Quantity);

    public int RenderedBytes => _renderCache.Length;

    public void Dispose() => PriceFeed.PriceChanged -= OnPriceChanged;

    private void OnPriceChanged(object? sender, PriceChangedEventArgs e)
    {
        foreach (var line in _lines)
        {
            if (line.Sku == e.Sku)
            {
                line.Price = e.Price;
            }
        }
    }
}
