// Healthy baseline: allocates at a steady rate with a bounded live set.
// The heap saw-tooths but its bottoms stay flat; rules must stay quiet on this app.
// Options: --mb-per-sec <n> (default 50), --live-set-mb <n> (default 100).
const int ItemBytes = 1024;
const int TicksPerSecond = 10;

var mbPerSec = Arg("--mb-per-sec", 50);
var liveSetMb = Arg("--live-set-mb", 100);

// Long-lived slots: survivors get promoted to gen2 and later replaced, so gen2 GCs also reclaim memory.
var slots = new byte[liveSetMb * 1024 * 1024 / ItemBytes][];
var next = 0;
var itemsPerTick = mbPerSec * 1024 * 1024 / ItemBytes / TicksPerSecond;

Console.WriteLine($"SteadyAllocator PID {Environment.ProcessId}: {mbPerSec} MB/s, live set {liveSetMb} MB");
using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(1000 / TicksPerSecond));
var tick = 0L;
while (await timer.WaitForNextTickAsync())
{
    for (var i = 0; i < itemsPerTick; i++)
    {
        var item = new byte[ItemBytes];
        // 1 in 10 items survives in the live set; the rest are garbage immediately.
        if (i % 10 == 0)
        {
            slots[next] = item;
            next = (next + 1) % slots.Length;
        }
    }

    if (++tick % (5 * TicksPerSecond) == 0)
    {
        PrintStatus();
    }
}

int Arg(string name, int fallback)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? int.Parse(args[i + 1]) : fallback;
}

static void PrintStatus() =>
    Console.WriteLine(
        $"{DateTime.UtcNow:HH:mm:ss} heap {GC.GetTotalMemory(false) / 1048576} MB, " +
        $"GCs gen0/1/2 {GC.CollectionCount(0)}/{GC.CollectionCount(1)}/{GC.CollectionCount(2)}");
