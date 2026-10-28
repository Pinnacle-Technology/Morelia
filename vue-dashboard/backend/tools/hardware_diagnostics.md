# Inspect a hardware recovery failure

Use the session log to inspect the inventory and connection operations performed
by the failing backend process. These events are enabled at the default INFO log
level. Start a new backend and acquisition runtime after installing code changes.

## Find the session events

Run these commands from the repository root. Replace `73` with the session ID.
If `DIAGNOSTIC_LOG_DIR` is customized, use that directory instead.

```powershell
$sessionId = 73
$logPath = ".\vue-dashboard\backend\instance\diagnostics\session-$sessionId\watchdog-driver.jsonl"
$events = Get-Content -LiteralPath $logPath -Tail 5000 |
    ForEach-Object { $_ | ConvertFrom-Json }
$hardwareEvents = $events | Where-Object component -eq 'hardware_diagnostics'
$hardwareEvents |
    Select-Object timestamp, pid, device_id, action, outcome, port, error_code, suppressed_count
```

Inspect the latest presence error and its evidence:

```powershell
$failure = $hardwareEvents |
    Where-Object { $_.action -eq 'native_presence' -and $_.outcome -eq 'failed' } |
    Select-Object -Last 1
$failure | ConvertTo-Json -Depth 12
```

Find other events emitted by the same operation:

```powershell
$hardwareEvents |
    Where-Object operation_id -eq $failure.operation_id |
    ConvertTo-Json -Depth 12
```

If the failure occurred earlier, increase the tail length or read the whole file.
Unchanged events can be suppressed, so also inspect preceding inventory events
for the same process and target.

## Read the evidence

| Action | Evidence |
| --- | --- |
| `native_inventory` | Raw serial and description representations and hex bytes, driver flags, open flag, device type, VID/PID integer, USB location, expected and returned counts, failing index, Python executable, and driver module path. |
| `inventory_cache` | Whether the observation was cached, its age when emitted, cache lifetime, and normalized endpoint rows. Covers D2XX, VCP, and Windows USB. |
| `windows_usb_inventory` | Currently attached FTDI USB parent instance IDs, VID/PID, and native serial selectors. |
| `native_presence` | Requested physical selector and logical hardware identity, exact match count, unidentified row count, evaluated rows, and presence result. `presence_source` identifies D2XX or Windows PnP; `windows_rows` records fallback evidence. |
| `connection_presence` | Device and stream identity at the runtime boundary, with the presence result or exception. Covers D2XX and VCP. |
| `native_open` | Exact serial, connection settings, identity returned by the handle, failure stage, and retained cleanup ownership. |
| `native_close` | Close stage and whether the handle or cleanup obligation remains. |

Each emitted operation includes an operation ID, duration, outcome, and error
type and code when available. Failures include the exception chain and traceback.
The session file adds session, runtime, watchdog, process, and timestamp context.
The existing `watchdog stream status` events also include hardware presence and
worker exit code alongside heartbeat, source-read, and recovery state.

A blank descriptor is evidence of an incomplete observation. It does not identify
which physical device is absent or prove why the driver withheld the identity.
An enumeration failure preserves the rows already read and the failing index.

On Windows, if D2XX has no target match and contains blank identities, recovery
queries the Windows Configuration Manager for currently present USB devices.
It matches the frozen native serial against FTDI USB parents, including channel
suffixes for supported multi-interface devices. Disconnected historical devices
are excluded. The inventory is shared across watchers for one second and does
not open or close D2XX connections.

When the target is absent, recovery reports `hardware_present=false` and
`waiting_for_port`; it waits for replug without consuming the 30-second access
timeout. When the target returns, recovery resumes opening and verifying the
stream. Windows query failures or unsupported FTDI identities remain errors,
because those observations cannot establish absence. The supported FTDI product
IDs are `6001`, `6010`, `6011`, and `6014` under vendor `0403`.

## Account for logging limits

The recorder emits the first result, changed evidence or outcome, and one repeat
after 60 seconds of unchanged observations. `suppressed_count` reports skipped
observations since the preceding emitted event. A successful operation after a
failure is emitted immediately. Cache-hit status and cache age do not themselves
trigger an event.

Evidence contains at most 64 rows and 256 characters per text field. Truncated
lists include an omitted count. The recorder retains at most 256 operation keys
per process. Evicted keys emit again on their next observation. Existing periodic
watchdog status logging is unchanged, and these limits do not rotate log files.

Evidence is restricted to hardware metadata. Handles, acquisition samples,
command payloads, credentials, and arbitrary object representations are excluded.
The recorder does not enumerate again, open a device, or reset hardware to obtain
extra evidence. Logging failures do not replace the operation's result or error.

## Instrument another operation

Use the shared context manager around a hardware or recovery boundary. Populate
evidence as the operation advances, so a failure records the last reached stage.

```python
from app.diagnostic_events import diagnostic_operation

with diagnostic_operation("device_setup", port=serial, transport="d2xx") as evidence:
    evidence["stage"] = "configure_handle"
    configure_device()
    evidence["stage"] = "complete"
```

Add new evidence names to the explicit allowlist in `app/diagnostic_events.py`.
Use bounded operational values. Add an output-contract test that reads the actual
session JSONL file and verifies the evidence survives logging filters. Exercise
failure, recovery, unchanged repeats, and payload exclusion. Keep per-sample reads
out of this context manager; use the existing source-read counters for those.

For a separate read-only inventory sample, use [the FTDI diagnostic](diagnose_ftdi.py).
That sample comes from another process and cannot substitute for the inventory
captured inside a failing backend operation.
