// Reference scenario (docs/architecture.md §8): every request creates an OrderView that
// subscribes to the static PriceFeed.PriceChanged event and never unsubscribes.
// Path to root: static event → handler list → OrderView → List<OrderLine>.
// Run with --Leak:Fixed=true to dispose views (unsubscribe) and see memory stay flat.
using LeakyOrderService;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHttpClient();
builder.Services.AddHostedService<PriceTicker>();
builder.Services.AddHostedService<LoadGenerator>();

var app = builder.Build();
var fixedMode = app.Configuration.GetValue<bool>("Leak:Fixed");

app.MapPost("/orders", () =>
{
    var order = Order.CreateRandom();
    var view = new OrderView(order);
    var total = view.Total;
    if (fixedMode)
    {
        view.Dispose();
    }

    return Results.Ok(new { order.Id, total });
});

app.MapGet("/stats", () => new
{
    pid = Environment.ProcessId,
    fixedMode,
    subscribers = PriceFeed.SubscriberCount,
    gcHeapBytes = GC.GetTotalMemory(false),
});

Console.WriteLine($"LeakyOrderService PID {Environment.ProcessId} (fixed: {fixedMode})");
app.Run();
