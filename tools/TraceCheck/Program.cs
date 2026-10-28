using Microsoft.Diagnostics.Tracing;

// Writes the expected output of a wolf-nettrace golden test, as parsed by TraceEvent (the
// reference parser behind PerfView and dotnet-trace):
//
//   events <count> lost <count>
//   <timestamp_qpc> <thread_id> <provider> <event_id> <version> <payload_hex>   (sorted)
//
// TraceEvent reorders events by time while wolf-nettrace keeps file order, so lines are sorted.
if (args.Length != 2)
{
    Console.Error.WriteLine("usage: dotnet run --project tools/TraceCheck -- <in.nettrace> <out.expected.txt>");
    return 2;
}

var lines = new List<string>();
using (var source = new EventPipeEventSource(args[0]))
{
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
}

File.WriteAllText(args[1], string.Join('\n', lines) + "\n");
Console.WriteLine($"{lines[0]} -> {args[1]}");
return 0;
