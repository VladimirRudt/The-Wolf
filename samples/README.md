# Sample apps

.NET 10 apps with known memory behaviour, used to develop and test Mr. Wolf. Each prints its PID on start and generates its own load — no external tools needed.

| App | Behaviour | What to expect in `dotnet-counters` |
|---|---|---|
| `LeakyOrderService` | **Reference scenario** (`docs/architecture.md` §8): each `POST /orders` creates an `OrderView` that subscribes to static `PriceFeed.PriceChanged` and never unsubscribes. A built-in load generator sends 20 req/s. | Working set and gen1→gen2 grow steadily (~40 MB/min) |
| `SteadyAllocator` | Healthy baseline: 50 MB/s allocations, bounded 100 MB live set. Rules must stay quiet here. | Saw-tooth; gen2 bottoms stay flat |
| `LohChurn` | 200 MB/s of short-lived 85 KB–2 MB arrays. Not a leak. | Many gen2 GCs (~8/s); LOH size jumps up and down |
| `NativeLeak` | Leaks 5 MB/s via `NativeMemory.Alloc`. | Working set grows ~5 MB/s; GC heap flat |

## Run

```sh
dotnet run --project samples/LeakyOrderService
dotnet run --project samples/LeakyOrderService -- --Leak:Fixed=true      # views unsubscribe: flat memory
dotnet run --project samples/LeakyOrderService -- --Load:RequestsPerSecond=0  # no load
dotnet run --project samples/SteadyAllocator -- --mb-per-sec 50 --live-set-mb 100
dotnet run --project samples/LohChurn -- --mb-per-sec 200 --keep 16
dotnet run --project samples/NativeLeak -- --mb-per-sec 5
```

`LeakyOrderService` listens on `http://localhost:5080`; `GET /stats` shows the live subscriber count.

## Observe

```sh
dotnet tool install -g dotnet-counters
dotnet-counters monitor -p <pid>
```

Use the PID the app prints, not the one from `dotnet run` (that is the SDK's launcher process).
