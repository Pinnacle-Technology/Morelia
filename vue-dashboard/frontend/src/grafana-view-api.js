import { requestJson } from "./api-client";

const VIEW_STATES = new Set([
  "ready",
  "unavailable",
  "not_configured",
  "not_applicable",
]);
const CH_OPTIONS = ["CH0", "CH1", "CH2"];

function safeHttpUrl(value, legacyCh = null) {
  if (typeof value !== "string" || !value.trim()) return null;
  try {
    const parsed = new URL(value);
    const sensitiveQuery = [...parsed.searchParams.keys()].some((key) => /token|secret|credential|api[_-]?key/i.test(key));
    const safe = (parsed.protocol === "http:" || parsed.protocol === "https:")
      && !parsed.username
      && !parsed.password
      && !sensitiveQuery;
    if (!safe || !/^\/d(?:-solo)?\/morelia-m4-comparison\//.test(parsed.pathname)) return null;
    // Keep the M4 view at its fixed cadence even with an older running backend.
    parsed.searchParams.set("refresh", "500ms");
    parsed.searchParams.delete("var-channel");
    if (legacyCh !== null) {
      parsed.searchParams.delete("var-show_ch");
      (legacyCh.length ? legacyCh : ["__none__"])
        .forEach((channel) => parsed.searchParams.append("var-show_ch", channel));
    }
    return parsed.toString();
  } catch {
    return null;
  }
}

function normalizeChannels(channels) {
  if (!Array.isArray(channels)) return [];
  return [...new Set(channels.filter((channel) => typeof channel === "string" && channel.trim()).map((channel) => channel.trim()))];
}

function normalizeOptions(values) {
  if (!Array.isArray(values)) return [];
  return [...new Set(values.filter((value) => typeof value === "string" && value.trim()).map((value) => value.trim()))];
}

function normalizeTargets(targets) {
  if (!Array.isArray(targets)) return [];
  return targets.flatMap((target) => {
    if (!target || typeof target !== "object" || typeof target.id !== "string" || !target.id.trim()) return [];
    return [{
      id: target.id.trim(),
      label: typeof target.label === "string" && target.label.trim() ? target.label.trim() : target.id.trim(),
      channels: normalizeChannels(target.channels),
    }];
  });
}

export function normalizeGrafanaView(payload, { showCh = null } = {}) {
  if (!payload || typeof payload !== "object" || !VIEW_STATES.has(payload.state)) {
    throw new TypeError("The Grafana view API returned an unusable response shape.");
  }

  const targets = normalizeTargets(payload.targets);
  const selectedTarget = targets.find((target) => target.id === payload.selected_target_id) ?? targets[0] ?? null;
  const dataDetailOptions = normalizeOptions(payload.data_detail_options);
  const selectedDataDetail = dataDetailOptions.includes(payload.selected_data_detail)
    ? payload.selected_data_detail
    : dataDetailOptions[0] ?? null;
  const ttlOptions = normalizeOptions(payload.ttl_options);
  const selectedTtl = normalizeOptions(payload.selected_ttl).filter((value) => ttlOptions.includes(value));
  // A backend running an active recording may predate the analog controls.
  // Only that older response needs client-side URL wiring until it restarts.
  const hasChControls = Object.hasOwn(payload, "ch_options") || Object.hasOwn(payload, "selected_ch");
  const chOptions = hasChControls
    ? normalizeOptions(payload.ch_options).filter((value) => CH_OPTIONS.includes(value))
    : [...CH_OPTIONS];
  const selectedCh = hasChControls
    ? normalizeOptions(payload.selected_ch).filter((value) => chOptions.includes(value))
    : Array.isArray(showCh)
      ? CH_OPTIONS.filter((value) => showCh.includes(value))
      : [...CH_OPTIONS];
  const legacyCh = hasChControls ? null : selectedCh;
  const embedUrl = safeHttpUrl(payload.embed_url, legacyCh);
  const openUrl = safeHttpUrl(payload.open_url, legacyCh);
  const state = payload.state === "ready" && !embedUrl ? "unavailable" : payload.state;

  return {
    state,
    scope: "device_filters",
    notice: typeof payload.notice === "string" ? payload.notice : "",
    targets,
    selectedTargetId: selectedTarget?.id ?? null,
    dataDetailOptions,
    selectedDataDetail,
    ttlOptions,
    selectedTtl,
    chOptions,
    selectedCh,
    embedUrl,
    openUrl,
    retryAfterSeconds: Number.isFinite(payload.retry_after_seconds)
      ? Math.min(120, Math.max(5, Number(payload.retry_after_seconds)))
      : 15,
    message: state === "unavailable" && payload.state === "ready" && !embedUrl
      ? "Grafana returned an unsafe or missing graph URL."
      : typeof payload.message === "string" ? payload.message : "",
  };
}

export async function loadGrafanaView(
  sessionId,
  { targetId = null, dataDetail = null, showTtl = null, showCh = null } = {},
) {
  if (typeof sessionId !== "string" && typeof sessionId !== "number") {
    throw new TypeError("A numeric or string session id is required.");
  }

  const query = new URLSearchParams();
  if (targetId) query.set("target_id", targetId);
  if (dataDetail) query.set("data_detail", dataDetail);
  if (Array.isArray(showTtl)) {
    const ttlValues = showTtl.length ? showTtl : ["__none__"];
    ttlValues.forEach((value) => query.append("show_ttl", value));
  }
  if (showCh !== null) {
    if (!Array.isArray(showCh) || showCh.some((value) => !CH_OPTIONS.includes(value))) {
      throw new TypeError("Analog channels must be selected from CH0, CH1, and CH2.");
    }
    const chValues = CH_OPTIONS.filter((value) => showCh.includes(value));
    (chValues.length ? chValues : ["__none__"])
      .forEach((value) => query.append("show_ch", value));
  }
  const suffix = query.size ? `?${query.toString()}` : "";
  const payload = await requestJson(
    `/api/v1/sessions/${encodeURIComponent(String(sessionId))}/grafana-view${suffix}`,
  );
  return normalizeGrafanaView(payload, { showCh });
}
