using System.Globalization;
using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.Analysis;
using Microsoft.Diagnostics.Tracing.Parsers.Clr;

// Writes the expected output of the golden tests, as computed by TraceEvent (the reference
// parser behind PerfView and dotnet-trace).
//
// events mode (wolf-nettrace):
//   events <count> lost <count>
//   <timestamp_qpc> <thread_id> <provider> <event_id> <version> <payload_hex>   (sorted)
// TraceEvent reorders events by time while wolf-nettrace keeps file order, so lines are sorted.
//
// gc mode (wolf-dotnet), from TraceEvent's GC model (PerfView's GCStats), one line per
// completed GC, by GC number:
//   <number> <generation> <type> <reason> <pause_ms> <duration_ms> <compacting> <gen0> <gen1> <gen2> <loh> <poh>
// type and reason are the raw runtime values; sizes are bytes after the GC.
const string Usage = "usage: dotnet run --project tools/TraceCheck -- [gc] <in.nettrace> <out.expected.txt>";

var (mode, input, output) = args switch
{
    [var i, var o] => ("events", i, o),
    ["gc", var i, var o] => ("gc", i, o),
    _ => (null, null, null),
};
if (mode is null || input is null || output is null)
{
    Console.Error.WriteLine(Usage);
    return 2;
}

var lines = mode == "gc" ? GcLines(input) : EventLines(input);
File.WriteAllText(output, string.Join('\n', lines) + "\n");
Console.WriteLine($"{lines[0]} -> {output}");
return 0;

static List<string> EventLines(string path)
{
    var lines = new List<string>();
    using var source = new EventPipeEventSource(path);
    // The dynamic parser names events from the stream's own metadata, so provider names are
    // set and ids stay raw. (The Clr parser would remap some ids, e.g. 39 → 65524.)
    source.Dynamic.All += _ => { };
    source.AllEvents += e => lines.Add(string.Join(' ',
#pragma warning disable CS0618 // "discouraged": the raw clock value is exactly what we compare
        e.TimeStampQPC,
#pragma warning restore CS0618
        e.ThreadID,
        e.ProviderName,
        (int)e.ID,
        e.Version,
        Convert.ToHexStringLower(e.EventData())));
    source.Process();
    lines.Sort(StringComparer.Ordinal);
    lines.Insert(0, $"events {lines.Count} lost {source.EventsLost}");
    return lines;
}

static List<string> GcLines(string path)
{
    using var source = new EventPipeEventSource(path);
    source.NeedLoadedDotNetRuntimes();
    source.Process();

    var gcs = source.Processes()
        .Select(process => process.LoadedDotNetRuntime())
        .Where(runtime => runtime is not null)
        .SelectMany(runtime => runtime!.GC.GCs)
        .Where(gc => gc.IsComplete)
        .OrderBy(gc => gc.Number)
        .ToList();

    var lines = gcs.Select(gc =>
    {
        var stats = gc.HeapStats;
        var mechanisms = gc.GlobalHeapHistory?.GlobalMechanisms;
        var compacting = mechanisms is null ? "-"
            : (mechanisms.Value & GCGlobalMechanisms.Compaction) != 0 ? "yes" : "no";
        return string.Join(' ',
            gc.Number,
            gc.Generation,
            (int)gc.Type,
            (int)gc.Reason,
            gc.PauseDurationMSec.ToString("F6", CultureInfo.InvariantCulture),
            gc.DurationMSec.ToString("F6", CultureInfo.InvariantCulture),
            compacting,
            stats?.GenerationSize0, stats?.GenerationSize1, stats?.GenerationSize2,
            stats?.GenerationSize3, stats?.GenerationSize4);
    }).ToList();
    lines.Insert(0, $"gcs {lines.Count}");
    return lines;
}
