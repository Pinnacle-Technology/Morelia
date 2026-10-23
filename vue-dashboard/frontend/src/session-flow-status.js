// Collapses the independent signals the detail page receives into the one
// question an operator asks while spectating a run: is data moving, and should I
// worry? The inputs disagree often enough that the merge rule has to be explicit
// rather than incidental:
//
//   lifecycle       — what the control plane intends (Preparing..Completed)
//   health          — the backend's session-level rollup over the streams
//   streams         — the RAW per-device stream states from the newest report
//   outboxHealth    — whether the runtime is still reporting at all
//   activityState   — whether OUR event stream is connected (session-events.js)
//
// Two of these are subtle:
//
// `activityState` — a dead SSE connection means we cannot SEE the session, not
// that the session stopped. Reporting that as red would train operators to
// ignore red. It caps at amber and says so in `reason`.
//
// `streams` vs `health` — `health` is the backend's cross-axis rollup
// (health_state.derive), and it is lossy on purpose: a single enum cannot show
// two axes at once. Relying on it alone is what made this bar report "Streaming"
// while a pod sat unplugged. The per-device states are carried alongside it, NOT
// folded into it, so the rail can show which stream and why.

import { isRunningLifecycle } from "./session-utils";
import { timestampMs } from "./datetime";

export const FlowTone = Object.freeze({
  IDLE: "idle",
  GOOD: "good",
  WARN: "warn",
  BAD: "bad",
});

// Severity ordering, so merging signals is "worst wins" instead of last-wins.
const TONE_RANK = { idle: 0, good: 1, warn: 2, bad: 3 };

export function worstTone(a, b) {
  return TONE_RANK[b] > TONE_RANK[a] ? b : a;
}

// Lifecycles where nothing is expected to move. These never animate and never
// go red — a Preparing run isn't broken; dispatch just has not completed. "Unknown" is the
// deep-link case: the page mounted from a URL and /status has not answered yet,
// so we say so rather than guessing a lifecycle.
const RESTING_HEADLINE = { Unknown: "Loading status…" };

const RESTING_REASON = {
  Unknown: "Fetching this session's current status…",
  Preparing: "Preparing this run for dispatch.",
  Scheduled: "Scheduled. Waiting for its start time.",
  Completed: "Completed. This run is closed and read-only; start another run from its source template if needed.",
  Cancelled: "Cancelled before dispatch. No compatible device was available at start time.",
};

// `health` values are the operator labels, not the raw enum — keep these in
// sync with `SessionHealth` / `resolveSessionHealth` in session-utils.js.
const HEALTH_TONE = {
  Healthy: FlowTone.GOOD,
  Suspect: FlowTone.WARN,
  Recovering: FlowTone.WARN,
  Delayed: FlowTone.WARN,
  Unreachable: FlowTone.BAD,
  Failed: FlowTone.BAD,
  "Not streaming": FlowTone.BAD,
  // We have lost sight of a running session. Capped at WARN for the same reason
  // ACTIVITY_TONE is (see the header note): not seeing it is not the same as it
  // being broken, and red that turns out to be fine is red operators learn to
  // ignore. A genuinely failed stream row still outranks this in deriveFlowStatus.
  "Not reporting": FlowTone.WARN,
  Unknown: FlowTone.WARN,
  "Needs action": FlowTone.BAD,
  "Not running": FlowTone.IDLE,
  Stopped: FlowTone.IDLE,
};

// SessionEventState -> tone. Deliberately tops out at WARN: see the header note.
const ACTIVITY_TONE = {
  live: FlowTone.GOOD,
  connecting: FlowTone.WARN,
  reconnecting: FlowTone.WARN,
  stale: FlowTone.WARN,
  unavailable: FlowTone.WARN,
};

const ACTIVITY_NOTE = {
  connecting: "Connecting to the activity stream…",
  reconnecting: "Reconnecting to the activity stream — status below may lag.",
  stale: "Activity stream is stale; showing the last proven events.",
  unavailable: "Activity stream is unavailable — status below may be out of date.",
};

const RUNNING_HEADLINE = {
  Starting: "Starting up",
  Stopping: "Stopping",
};

// ---------------------------------------------------------------------------
// Per-stream axis
// ---------------------------------------------------------------------------

// Raw StreamStatus (backend domain/enums.py) -> tone. `suspect` is deliberately
// NOT folded into `healthy` here. The backend used to hide it (`_hide_suspect`)
// and that fold is exactly what this rail exists to undo — re-applying it in the
// client would reopen the bug. See docs/frontend-backend-contract-gap-register.md.
const STREAM_TONE = {
  healthy: FlowTone.GOOD,
  suspect: FlowTone.WARN,
  unhealthy: FlowTone.BAD,
};

// Raw SinkHealth (backend runtime_child/driver.py) -> tone.
const SINK_TONE = {
  healthy: FlowTone.GOOD,
  degraded: FlowTone.WARN,
  failed: FlowTone.BAD,
};

// Operator copy for the watchdog's `action` / `recovery_stage`. The two
// port-wait cases are deliberately kept apart: "the port is gone" and "the port
// is here but something else holds it" call for different operator actions, and
// the gap register lists conflating them as a forbidden result.
const RECOVERY_COPY = {
  waiting_for_port: "Port not connected — waiting for it to return",
  waiting_for_port_release: "Port present but held — waiting for release",
  retry_wait: "Retrying after a failed attempt",
};

const RECOVERY_ERROR_COPY = {
  cleanup_failed: "Stream cleanup failed. Automatic recovery stopped. Stop the session before trying again.",
  port_release_timeout: "Port recovery timed out after 15 seconds. Automatic recovery stopped. Check the device connection and close other programs using it before retrying.",
};

const STREAM_LABEL = {
  healthy: "Healthy",
  suspect: "Suspect",
  unhealthy: "Unhealthy",
};

/**
 * Tone + copy for one sink group, as its own axis.
 *
 * Kept separate from the stream tone rather than merged into it, because the
 * two failures need different responses: a dead stream means the device stopped
 * producing, while a failed sink means the device is fine and the write path is
 * losing it. Merging them would make those look identical on the rail.
 *
 * ABSENCE IS NOT A FAULT on this axis. The runtime populates a report's `sinks`
 * list from the sink-ERROR queue alone (`Morelia._drain_sink_errors`), so a
 * healthy sink is never mentioned and `_sink_status_view` necessarily stamps it
 * `stale` — "known only from durable evidence". Reading that as unproven pinned
 * every healthy row to amber under a green session and printed
 * "N not in latest report" forever. Under the contract the runtime actually
 * implements, silence about a sink is the good news.
 *
 * Only evidence moves this axis off neutral:
 *   - a live `health` of degraded/failed,
 *   - `status: "unknown"`, which is the backend's parse-FAILURE marker (a live
 *     snapshot arrived and could not be read) and not the same as absence,
 *   - durable `sample_loss`, which is monotonic and outlives the report.
 */
function summarizeSinks(sinks) {
  if (!sinks.length) return { tone: FlowTone.IDLE, note: "No sinks reported" };

  let tone = FlowTone.GOOD;
  let lostSamples = 0;
  let troubled = 0;
  let unreadable = 0;
  for (const sink of sinks) {
    // No `health` means no live snapshot for this sink, which under the
    // errors-only contract means it has not failed. It gets no vote.
    if (sink.health) {
      const sinkTone = SINK_TONE[sink.health] ?? FlowTone.IDLE;
      if (sinkTone !== FlowTone.GOOD) troubled += 1;
      tone = worstTone(tone, sinkTone);
    }
    if (sink.status === "unknown") {
      tone = worstTone(tone, FlowTone.WARN);
      unreadable += 1;
    }
    lostSamples += sink.sample_loss ?? 0;
  }

  // Loss is monotonic and durable — a sink that reports `healthy` while having
  // already dropped samples is still something the operator must know about.
  if (lostSamples > 0) tone = worstTone(tone, FlowTone.WARN);

  const notes = [];
  if (lostSamples > 0) notes.push(`${lostSamples.toLocaleString()} samples lost`);
  if (troubled) notes.push(`${troubled}/${sinks.length} sinks reporting errors`);
  if (unreadable) notes.push(`${unreadable} unreadable`);
  // Says exactly what we know, and no more: the runtime has not complained
  // about these sinks. It deliberately does not claim they are verified healthy.
  if (!notes.length) notes.push("No sink errors reported");
  return { tone, note: notes.join(" · ") };
}

/**
 * Decide what ONE rail row shows, from its stream axis and its sink axis.
 *
 * This is the whole severity policy in one place. Two rules are load-bearing:
 *
 * 1. The track reflects the STREAM only. A stream that is producing normally
 *    keeps marching even when its sink is lossy, and the sink problem shows as
 *    its own note. Rolling the sink into the track would make "the pod is
 *    unplugged" and "the disk is dropping writes" render identically.
 *
 * 2. `flowing` is `tone === GOOD` — anything less than healthy holds still.
 *    An amber track that keeps marching reads as "recovering, don't worry",
 *    which is the failure mode that hid the unplugged pod in the first place.
 *
 * The row's overall `tone` is still worst-of both axes, so a failed sink colours
 * the row (and rolls up into the headline) without unfreezing its track.
 */
function rollRow(streamTone, sinkTone) {
  return {
    tone: worstTone(streamTone, sinkTone),
    flowing: streamTone === FlowTone.GOOD,
  };
}

/**
 * Resolve the configured flow that belongs to a reported device.
 *
 * The runtime emits device reports in manifest order, and the manifest is built
 * from this same `device_flows` list, so positional correspondence holds. It is
 * still only an invariant, not an identity — so prefer a real id match when the
 * flow carries one, and fall back to position.
 */
function matchConfiguredFlow(deviceId, configuredFlows, index) {
  const byId = configuredFlows.find(
    (flow) =>
      flow &&
      (flow.device_id === deviceId ||
        flow.nickname === deviceId ||
        String(flow.device_config_id ?? "") === String(deviceId ?? "")),
  );
  return byId ?? configuredFlows[index] ?? {};
}

function deviceIdentity(deviceId) {
  const value = String(deviceId ?? "").trim();
  const separator = value.indexOf(":");
  if (separator < 1 || separator === value.length - 1) {
    return { deviceType: value || null, hardwareId: null };
  }
  return {
    deviceType: value.slice(0, separator),
    hardwareId: value.slice(separator + 1),
  };
}
function workerFaultCopy(fault, { recovered = false } = {}) {
  if (!fault) return null;
  if (fault.reason === "protocol_violation") {
    return recovered
      ? "Recovered after interrupted shutdown"
      : "Shutdown interrupted during recovery";
  }
  const target = fault.sink_id ? `Sink ${fault.sink_id}` : "Stream worker";
  const type = fault.error_type ? ` (${fault.error_type})` : "";
  const reason = fault.reason ? `: ${fault.reason}` : "";
  return `${target} failed${type}${reason}`;
}

/**
 * Build one rail row per reported device.
 *
 * @param {object[]} devices  `latest_report.devices` — raw per-device states.
 * @param {object[]} sinks    the session's `sinks` array, keyed by `source_id`.
 * @param {object[]} configuredFlows  `session.device_flows`, for hardware ids.
 * @param {boolean}  unproven  true when the runtime has gone quiet, in which
 *   case every row is last-known rather than current and none of them animate.
 */
export function deriveStreamRows({
  devices = [],
  sinks = [],
  configuredFlows = [],
  unproven = false,
} = {}) {
  return devices.map((device, index) => {
    const deviceId = device?.device_id ?? null;
    const configured = matchConfiguredFlow(deviceId, configuredFlows, index);
    const identity = deviceIdentity(deviceId);
    const nickname = String(configured.nickname ?? "").trim();
    const deviceType = configured.device_type ?? configured.deviceType ?? configured.type ?? identity.deviceType;
    const hardwareId = configured.hardware_id ?? configured.hardwareId ?? identity.hardwareId;
    const ownSinks = sinks.filter((sink) => sink.source_id === deviceId);
    const sinkSummary = summarizeSinks(ownSinks);
    const streamTone = STREAM_TONE[device?.stream_status] ?? FlowTone.IDLE;
    const rolled = rollRow(streamTone, sinkSummary.tone);

    // `pending_recovery` collapses both port-wait cases into one boolean, so the
    // copy is resolved from `action`/`recovery_stage` — the fields that still
    // tell the two apart.
    const recoveryCopy =
      RECOVERY_COPY[device?.action] ?? RECOVERY_COPY[device?.recovery_stage] ?? null;
    const faultCopy = workerFaultCopy(device?.worker_fault, {
      recovered: device?.stream_status === "healthy" && !device?.pending_recovery,
    });
    const attempt = device?.recovery_attempt;

    return {
      id: deviceId ?? `device-${index}`,
      label: nickname || deviceType || `Device ${index + 1}`,
      hardwareId: nickname ? null : hardwareId,
      // Unproven rows never animate and never claim green: the values are the
      // last ones we were told, not the ones that are true now.
      tone: unproven ? worstTone(rolled.tone, FlowTone.WARN) : rolled.tone,
      flowing: unproven ? false : rolled.flowing,
      unproven,
      status: STREAM_LABEL[device?.stream_status] ?? "Unknown",
      reason: RECOVERY_ERROR_COPY[device?.reason] ?? faultCopy ?? recoveryCopy ?? device?.reason ?? null,
      workerFault: device?.worker_fault ?? null,
      attempt: recoveryCopy && attempt ? attempt : null,
      pendingRecovery: Boolean(device?.pending_recovery),
      sinkTone: sinkSummary.tone,
      sinkNote: sinkSummary.note,
      sinkCount: ownSinks.length,
    };
  });
}

// Runtime telemetry freshness (`outbox_health` from session_status._telemetry_view).
// `stale`/`overflow` mean we are no longer hearing from the runtime, so every
// per-stream value on screen is last-known rather than current.
const UNPROVEN_OUTBOX = new Set(["stale", "overflow"]);

export function isOutboxUnproven(outboxHealth) {
  return UNPROVEN_OUTBOX.has(outboxHealth);
}

/** "2s ago" / "4m ago", or null when the runtime has never reported. */
export function formatReportAge(lastReportAt, now = Date.now()) {
  if (!lastReportAt) return null;
  const then = timestampMs(lastReportAt);
  if (then === null) return null;
  const seconds = Math.max(0, Math.round((now - then) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  return `${Math.floor(seconds / 3600)}h ago`;
}

/**
 * Reduce the live signals to what the flow bar renders.
 *
 * @param {object[]} streams  rows from `deriveStreamRows`. When present they
 *   OUTRANK the session-level `health` rollup, because they are the unfolded
 *   version of the same facts. When absent (nothing reported yet, or a resting
 *   session) the bar falls back to the single-track behaviour.
 * @param {string} outboxHealth  `outbox_health` from /status.
 *
 * @returns {{tone, flowing, headline, reason, unproven}}
 *   `flowing` drives the single fallback track and is intentionally NOT just
 *   `tone === GOOD`: a Starting session has work in flight and should animate,
 *   while a red one has nothing moving and must hold still. Per-row animation on
 *   the rail is stricter — see `rollRow`.
 */
export function deriveFlowStatus({
  lifecycle = "Unknown",
  health = "Unknown",
  phase = null,
  activityState = "idle",
  detailAvailable = true,
  streams = [],
  outboxHealth = null,
} = {}) {
  if (!isRunningLifecycle(lifecycle)) {
    return {
      lifecycle,
      tone: FlowTone.IDLE,
      toneReasons: [`lifecycle:${lifecycle}`],
      flowing: false,
      unproven: false,
      headline: RESTING_HEADLINE[lifecycle] ?? lifecycle ?? "Unknown",
      reason: RESTING_REASON[lifecycle] ?? "This session is not currently running.",
    };
  }

  const unproven = isOutboxUnproven(outboxHealth);

  // Starting/Stopping are transitions: health hasn't stabilized yet, so it doesn't
  // get a vote until the session reaches Active.
  let tone = lifecycle === "Active" ? (HEALTH_TONE[health] ?? FlowTone.WARN) : FlowTone.WARN;
  const toneSources = [{ tone, reason: lifecycle === "Active" ? `health:${health}` : `lifecycle:${lifecycle}` }];
  const includeTone = (value, reason) => {
    toneSources.push({ tone: value, reason });
    tone = worstTone(tone, value);
  };
  includeTone(ACTIVITY_TONE[activityState] ?? FlowTone.IDLE, `activity:${activityState}`);
  if (!detailAvailable) includeTone(FlowTone.WARN, "detail:unavailable");
  // Stream evidence becomes authoritative only after startup. During lifecycle
  // transitions, old rows describe the previous stable state and must not paint
  // Starting or Stopping as healthy, degraded, or failed.
  if (lifecycle === "Active") {
    for (const row of streams) includeTone(row.tone, `stream:${row.id}:${row.status}`);
  }
  // A quiet runtime is a floor, not a ceiling: it can never read green, but a
  // genuinely failed stream still outranks it.
  if (unproven) includeTone(FlowTone.WARN, `outbox:${outboxHealth}`);

  const notes = [];
  if (phase) notes.push(`Phase: ${phase}.`);
  if (unproven) notes.push("The runtime has stopped reporting; stream states below are last-known.");
  else if (lifecycle === "Active" && !streams.length) notes.push(`Stream health: ${health}.`);
  if (!detailAvailable) notes.push("Session detail could not be refreshed.");
  if (ACTIVITY_NOTE[activityState]) notes.push(ACTIVITY_NOTE[activityState]);
  // The fallback has to know whether anything has reported. It used to be a bare
  // `|| "Waiting for the first status report…"`, which fired on the HEALTHIEST
  // case there is — an Active session with a full rail and nothing wrong leaves
  // every branch above unpushed — and printed "waiting for the first report"
  // directly beneath a "report 0s ago" clock.
  if (!notes.length) notes.push(railReason(streams));

  return {
    lifecycle,
    tone,
    toneReasons: toneSources.filter((source) => source.tone === tone).map((source) => source.reason),
    flowing: lifecycle !== "Stopping" && tone !== FlowTone.BAD,
    unproven,
    headline: streamHeadline({ lifecycle, tone, streams, unproven }),
    reason: notes.join(" "),
  };
}

/**
 * The default reason line, resolved from the rail rather than from its absence.
 *
 * With no rail there genuinely is nothing to describe yet. With one, this names
 * the single troubled stream where it can — the reason line is the only place
 * with room for "which one and why", and the headline's count cannot say it.
 */
function railReason(streams) {
  if (!streams.length) return "Waiting for the first status report…";
  const troubled = streams.filter((row) => row.tone !== FlowTone.GOOD);
  if (!troubled.length) return "All streams are reporting healthy.";
  if (troubled.length === 1) {
    const [row] = troubled;
    return `${row.label}: ${row.reason ?? row.sinkNote ?? row.status}.`;
  }
  return `${troubled.length} of ${streams.length} streams need attention.`;
}

/**
 * Motion state for the session rat, from an already-derived flow status.
 *
 * Lives here rather than in whichever component renders the rat, because the rat
 * and the flow bar are no longer in the same card — the rat sits with Session
 * Summary and the rail sits in Stream Health. Two components reading the same
 * verdict must not each re-invent the mapping, or the rat ends up running beside
 * a red rail.
 *
 * @param {object} status  the return of `deriveFlowStatus`.
 * @param {object[]} streams  rail rows, for the Suspect gait (its own frame set).
 */
export function deriveRatState(status, streams = []) {
  if (status.lifecycle === "Stopping") return "stopping";
  if (status.tone === FlowTone.IDLE) return "paused";
  if (status.tone === FlowTone.BAD) return "stopped";
  if (streams.some((row) => row.status === "Suspect")) return "suspect";
  if (!status.flowing) return "stopped";
  if (status.tone === FlowTone.WARN) return "recovering";
  return "running";
}

function streamHeadline({ lifecycle, tone, streams, unproven }) {
  if (RUNNING_HEADLINE[lifecycle]) return RUNNING_HEADLINE[lifecycle];
  if (unproven) return "No recent report — stream states unproven";
  // The count is the headline once we have a rail: "1 of 3 streams flowing" is
  // the answer to "is my data moving", where a single word never was.
  if (streams.length) {
    const flowing = streams.filter((row) => row.flowing).length;
    return `${flowing} of ${streams.length} streams flowing`;
  }
  if (tone === FlowTone.GOOD) return "Streaming";
  if (tone === FlowTone.BAD) return "Stalled — needs action";
  return "Streaming — degraded";
}
