# `ocpm_engine.event_batches`

Native summarization of factorized pg_ocpm 0.9 event batches
(`ocpm.event_log_batches` / `ocpm.event_log_window_batches` result rows)
into exact per-window summaries, without expanding event rows in Python.

## Functions

```python
summarize_event_batch_rows(rows: Iterable[Sequence]) -> EventLogSummary
```
Validate and summarize compact single-window result rows in Rust. Each row
is `(activity_path, activity_count, case_count, case_id_payloads,
event_timestamp_payloads)` as returned by `ocpm.event_log_batches(...)`.

```python
summarize_event_window_batch_rows(rows: Iterable[Sequence], *,
                                  window_count: int)
    -> tuple[EventLogSummary, ...]
```
Summarize one compact multi-window result without expanding events. Rows
carry a leading window number; the result is one summary per window in
order. `window_count` must be positive.

Rows stream to the native builder in bounded chunks, so peak Python memory
stays independent of result size. The exact 0.8 event-row fallback is
reached through
[`OcpmEngine.execute_event_log_summary`](engine.md#event-log-summaries),
which selects the right path from negotiated capabilities.

## Result types

| Type | Fields |
|---|---|
| `EventLogSummary` | `case_count`, `event_count`, `payload_bytes`, `variants`, `dfg`, `activities` |
| `EventVariantCount` | `activity_path`, `frequency` |
| `EventDfgEdge` | `source`, `target`, `frequency`, `mean_duration_seconds` |
| `EventActivityCount` | `activity`, `case_frequency`, `occurrence_frequency`, `start_frequency`, `end_frequency` |
| `EventLogExecution` | `strategy`, `database_rows`, `expanded_event_rows`, `summaries` |

## Example

```python
from ocpm_engine import summarize_event_batch_rows

cursor.execute(
    "SELECT activity_path, activity_count, case_count,"
    "       case_id_payloads, event_timestamp_payloads"
    "  FROM ocpm.event_log_batches(%s, %s, %s, %s, %s)",
    (dataset_id, tenant_id, "Order", window_start, window_end),
)
summary = summarize_event_batch_rows(cursor.fetchall())

print(summary.case_count, summary.event_count)
for edge in summary.dfg[:5]:
    print(edge.source, edge.target, edge.frequency, edge.mean_duration_seconds)
```
