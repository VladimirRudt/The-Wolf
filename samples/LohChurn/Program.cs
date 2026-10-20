// LOH churn: allocates short-lived large arrays (85 KB – 2 MB). Each one lands on the
// Large Object Heap, which is only collected by gen2 GCs, so gen2 GCs become frequent.
// Not a leak: total memory stays bounded. Options: --mb-per-sec <n> (default 200), --keep <n> (default 16).
const int TicksPerSecond = 10;
const int MinLargeBytes = 85_000;
const int MaxLargeBytes = 2 * 1024 * 1024;

var mbPerSec = Arg("--mb-per-sec", 200);
var keep = Arg("--keep", 16);

// The last few buffers stay alive briefly, like in-flight request buffers.
var recent = new byte[keep][];
var next = 0;
var bytesPerTick = (long)mbPerSec * 1024 * 1024 / TicksPerSecond;

Console.WriteLine($"LohChurn PID {Environment.ProcessId}: {mbPerSec} MB/s of large arrays, keeping {keep}");
using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(1000 / TicksPerSecond));
var tick = 0L;
while (await timer.WaitForNextTickAsync())
{
    for (long allocated = 0; allocated < bytesPerTick;)
    {
        var buffer = new byte[Random.Shared.Next(MinLargeBytes, MaxLargeBytes)];
        recent[next] = buffer;
        next = (next + 1) % recent.Length;
        allocated += buffer.Length;
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
