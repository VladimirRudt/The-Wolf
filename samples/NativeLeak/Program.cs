// Native leak: allocates unmanaged memory with NativeMemory.Alloc and never frees it.
// Process memory (working set / private bytes) grows while the GC heap stays flat —
// the "native vs managed" signal from docs/architecture.md §5.
// Options: --mb-per-sec <n> (default 5).
using System.Runtime.InteropServices;

const int TicksPerSecond = 10;

var mbPerSec = Arg("--mb-per-sec", 5);
var bytesPerTick = (nuint)(mbPerSec * 1024 * 1024 / TicksPerSecond);
long leakedBytes = 0;

Console.WriteLine($"NativeLeak PID {Environment.ProcessId}: leaking {mbPerSec} MB/s of native memory");
using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(1000 / TicksPerSecond));
var tick = 0L;
while (await timer.WaitForNextTickAsync())
{
    LeakChunk(bytesPerTick);
    leakedBytes += (long)bytesPerTick;

    // A little managed garbage, so the GC heap is alive but flat.
    // (KeepAlive makes it escape, so the JIT can't stack-allocate it away.)
    GC.KeepAlive(new byte[64 * 1024]);

    if (++tick % (5 * TicksPerSecond) == 0)
    {
        Console.WriteLine(
            $"{DateTime.UtcNow:HH:mm:ss} native leaked {leakedBytes / 1048576} MB, " +
            $"GC heap {GC.GetTotalMemory(false) / 1048576} MB");
    }
}

int Arg(string name, int fallback)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? int.Parse(args[i + 1]) : fallback;
}

static unsafe void LeakChunk(nuint bytes)
{
    var p = NativeMemory.Alloc(bytes);
    // Touch every page, otherwise the OS only reserves the memory and RSS doesn't grow.
    NativeMemory.Fill(p, bytes, 0xAB);
}
