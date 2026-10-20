# `.briefcase` — heap snapshot file format (v1)

A `.briefcase` file holds one heap snapshot: every live object, its type and size, references between objects, and GC roots. It is designed to be **memory-mapped**: opened instantly, used without parsing, and analyzed even when larger than RAM.

---

## 1. Design principles

1. **Flat arrays of fixed-size numbers**, laid out exactly as used in memory. No JSON (except the small metadata section), no variable-length records to walk, no compression in the working copy.
2. **Objects are renumbered** `0..N-1`. References are `u32` indexes, not addresses. Lookup = array index; no hash maps.
3. **Struct of arrays (columns):** one array per field, so queries read only the columns they need.
4. **Graph as CSR** (compressed sparse row): `edge_offsets[N+1]` + `edge_targets[E]`.
5. **Section table** so new sections can be added without breaking readers.
6. **Little-endian**, every section **8-byte aligned**.
7. **Write-once:** capture writes the file; analysis adds results as new sections in a new file or a sidecar, never modifying a mapped file in place.
8. **Privacy:** no string contents or field values in v1. If ever added, they live in a separate optional section.

---

## 2. Layout

```
┌──────────────────────────────┐
│ Header (fixed size)          │
├──────────────────────────────┤
│ Section table                │  section_count × SectionEntry
├──────────────────────────────┤
│ Sections (each 8-aligned)    │
├──────────────────────────────┤
│ Footer: checksum             │
└──────────────────────────────┘
```

### 2.1 Header
```rust
#[repr(C)]
struct Header {
    magic: [u8; 8],        // b"BRIEFCSE"
    version: u32,          // format version, starts at 1
    flags: u32,            // reserved, 0
    object_count: u64,     // N
    edge_count: u64,       // E
    root_count: u64,
    type_count: u64,
    section_count: u64,
}
```

### 2.2 Section entry
```rust
#[repr(C)]
struct SectionEntry {
    kind: u32,             // see table below
    elem_size: u32,        // bytes per element (0 for blobs)
    offset: u64,           // from file start, multiple of 8
    length: u64,           // bytes
}
```

### 2.3 Sections (v1)

| kind | Name | Element | Count | Notes |
|---|---|---|---|---|
| 1 | `metadata` | blob (UTF-8 JSON) | — | target id, runtime version, host, capture time (UTC ns), reason, command id, GC number, pause_ms, collector version |
| 2 | `string_bytes` | u8 | — | concatenated UTF-8 type names |
| 3 | `string_offsets` | u64 | type_count + 1 | type i name = `string_bytes[o[i]..o[i+1]]` |
| 4 | `type_flags` | u32 | type_count | bit 0 array, bit 1 string, bit 2 finalizable *(if known)*, others reserved |
| 5 | `obj_type` | u32 | N | type index |
| 6 | `obj_size` | u32 | N | shallow size; `u32::MAX` → look up in `size_overflow` |
| 7 | `obj_generation` | u8 | N | 0,1,2,3=LOH,4=POH,255=unknown (padded to 8) |
| 8 | `obj_address` | u64 | N | original address, sorted ascending |
| 9 | `edge_offsets` | u64 | N + 1 | CSR offsets |
| 10 | `edge_targets` | u32 | E | target object index |
| 11 | `roots` | RootEntry | root_count | see below |
| 12 | `size_overflow` | (u32 index, u32 pad, u64 size) | — | objects ≥ 4 GB (rare) |
| 100+ | analysis sections | | | added later (§5) |

```rust
#[repr(C)]
struct RootEntry {
    object: u32,           // object index
    kind: u8,              // 0 stack, 1 handle, 2 finalizer queue, 3 static, 4 other
    flags: u8,             // bit 0 pinned, bit 1 weak, bit 2 interior, bit 3 ref-counted
    _pad: u16,
    root_id: u64,          // source-specific id (e.g. handle id), 0 if none
}
```
Mapping from EventPipe root kinds/flags to this enum is defined in `wolf-briefcase` and must be documented there *(verify source kinds)*.

**Generation per object:** EventPipe heap-dump events may not carry generation per object. If not available, derive it from generation address ranges if the session provides them, otherwise store 255 *(verify)*.

### 2.4 Footer
`xxh3_64` (or CRC32C) checksum over all bytes before the footer, written last.

---

## 3. Size estimate

Per object: 4 (type) + 4 (size) + 1 (gen) + 8 (address) + 8 (edge offset) ≈ 25 bytes, plus 4 bytes per reference.
Example: 4M objects × 2 refs avg ≈ 132 MB. Real heaps vary.

---

## 4. Writing (two-pass build)

Input: raw spooled heap-dump events (see `collector-dotnet.md` §6.2).

**Pass 1 — capture (during GC, app paused): no processing.**
Append raw records to a temp spool file: nodes (address, size, type id, edge count), edges (target address), roots, types.

**Pass 2 — build (after the GC, app running):**
1. Read nodes; sort by address; assign index 0..N-1.
2. Remap type ids → dense type indexes; build string table.
3. Convert each edge target address → index via binary search on the sorted `obj_address`. Edges pointing outside the snapshot (should be rare) are dropped and counted in metadata.
4. Build CSR: `edge_offsets` from per-node edge counts in sorted order; `edge_targets` in the same order.
5. Convert roots (address → index), sort by object.
6. Write header, section table, sections (8-aligned), checksum.
7. Write to `<name>.briefcase.tmp`, fsync, then **atomic rename** to `<name>.briefcase`.

Memory use during build should stay bounded (external sort or chunked processing for very large heaps — phase 2+ optimization).

File naming: `<host>_<pid>_<UTC timestamp YYYY-MM-DDTHH-MM-SS>.briefcase`.

---

## 5. Analysis sections (added by the analysis engine)

Written into a **sidecar** `<name>.briefcase.analysis` (same container format, same header magic with a different `flags` bit) or into a new copy of the file:

| kind | Name | Element | Count |
|---|---|---|---|
| 100 | `idom` | u32 | N (`u32::MAX` = virtual root / unreachable) |
| 101 | `retained_size` | u64 | N |
| 102 | `rev_edge_offsets` | u64 | N + 1 |
| 103 | `rev_edge_targets` | u32 | E |
| 104 | `bfs_parent` | u32 | N (shortest path to root) |

Per-type aggregates and diffs are stored as small tables in DuckDB, not in the file.

---

## 6. Reading

- Map with `memmap2::Mmap`; view sections as typed slices with `bytemuck::cast_slice` (or `zerocopy`).
- **Validate before use:** magic, version, section bounds within file, 8-byte alignment, element counts consistent with header, checksum. Never trust offsets from a file blindly.
- Readers ignore unknown section kinds (forward compatibility). Readers reject unknown major versions.

```rust
pub fn refs(&self, i: usize) -> &[u32] {
    let o = self.edge_offsets();
    &self.edge_targets()[o[i] as usize..o[i + 1] as usize]
}
```

---

## 7. Transport and archive

Working copy: uncompressed (required for mmap). For copying/archiving: `zstd`, decompress before use.

## 8. Versioning

- `version` = 1 for this spec. Additive changes (new sections) don't bump the major version. Layout changes to existing sections do.
- Keep a test corpus of small briefcases per version; readers must open all of them.
