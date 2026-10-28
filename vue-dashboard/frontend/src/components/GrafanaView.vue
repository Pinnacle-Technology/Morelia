<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { AlertTriangle, BarChart3, ExternalLink, RefreshCw } from "@lucide/vue";
import BaseButton from "./BaseButton.vue";
import { loadGrafanaView } from "../grafana-view-api";

const props = defineProps({
  sessionId: { type: [String, Number], required: true },
});

const descriptor = ref(null);
const lastReadyDescriptor = ref(null);
const viewState = ref("loading");
const selectedTargetId = ref(null);
const selectedDataDetail = ref(null);
const selectedTtl = ref(null);
const selectedCh = ref(null);
const message = ref("");
const retrying = ref(false);

let mounted = false;
let pollTimer = null;
let requestSequence = 0;
let consecutiveFailures = 0;

const selectedTarget = computed(() =>
  descriptor.value?.targets.find((target) => target.id === selectedTargetId.value)
    ?? descriptor.value?.targets[0]
    ?? null,
);
const displayedGraph = computed(() =>
  descriptor.value?.state === "ready" ? descriptor.value : lastReadyDescriptor.value,
);
const hasRetainedGraph = computed(() =>
  viewState.value === "unavailable" && Boolean(lastReadyDescriptor.value?.embedUrl),
);
const frameTitle = computed(() =>
  `M4 Grafana comparison for ${selectedTarget.value?.label || "selected device"}`,
);
const statusCopy = computed(() => {
  if (viewState.value === "loading") return "Checking the M4 comparison dashboard…";
  if (viewState.value === "not_configured") return message.value || "Grafana has not been configured for this Morelia deployment.";
  if (viewState.value === "not_applicable") return message.value || "This session has no Influx destination that matches the M4 dashboard.";
  return message.value || "The Grafana dashboard is currently unavailable. Acquisition and Influx writing are unaffected.";
});

function clearPoll() {
  if (pollTimer !== null) {
    globalThis.clearTimeout(pollTimer);
    pollTimer = null;
  }
}

function schedulePoll(seconds) {
  clearPoll();
  if (!mounted || viewState.value === "not_applicable") return;
  const fallback = viewState.value === "not_configured" ? 30 : 15;
  const base = Number.isFinite(seconds) ? seconds : fallback;
  const delaySeconds = Math.min(120, Math.max(5, base * (2 ** consecutiveFailures)));
  pollTimer = globalThis.setTimeout(() => refresh({ silent: true }), delaySeconds * 1000);
}

function applyDescriptor(next) {
  descriptor.value = next;
  viewState.value = next.state;
  message.value = next.message;
  selectedTargetId.value = next.selectedTargetId;
  selectedDataDetail.value = next.selectedDataDetail;
  selectedTtl.value = next.selectedTtl;
  selectedCh.value = next.selectedCh;

  if (next.state === "ready") {
    lastReadyDescriptor.value = next;
    consecutiveFailures = 0;
  } else if (next.state === "unavailable") {
    consecutiveFailures += 1;
  } else if (next.state === "not_configured" || next.state === "not_applicable") {
    lastReadyDescriptor.value = null;
  }
  schedulePoll(next.retryAfterSeconds);
}

async function refresh({ silent = false, resetBackoff = false } = {}) {
  clearPoll();
  const sequence = ++requestSequence;
  if (resetBackoff) consecutiveFailures = 0;
  if (!silent && !descriptor.value) viewState.value = "loading";
  retrying.value = !silent;

  try {
    const next = await loadGrafanaView(props.sessionId, {
      targetId: selectedTargetId.value,
      dataDetail: selectedDataDetail.value,
      showTtl: selectedTtl.value,
      showCh: selectedCh.value,
    });
    if (!mounted || sequence !== requestSequence) return;
    applyDescriptor(next);
  } catch (error) {
    if (!mounted || sequence !== requestSequence) return;
    consecutiveFailures += 1;
    viewState.value = "unavailable";
    message.value = error?.message || "The Grafana dashboard could not be reached.";
    schedulePoll(5);
  } finally {
    if (mounted && sequence === requestSequence) retrying.value = false;
  }
}

function selectTarget(event) {
  selectedTargetId.value = event.target.value;
  refresh({ resetBackoff: true });
}

function selectDataDetail(event) {
  selectedDataDetail.value = event.target.value;
  refresh({ resetBackoff: true });
}

function toggleTtl(event) {
  const next = new Set(selectedTtl.value ?? []);
  if (event.target.checked) next.add(event.target.value);
  else next.delete(event.target.value);
  selectedTtl.value = (descriptor.value?.ttlOptions ?? []).filter((value) => next.has(value));
  refresh({ resetBackoff: true });
}

function toggleCh(event) {
  const next = new Set(selectedCh.value ?? []);
  if (event.target.checked) next.add(event.target.value);
  else next.delete(event.target.value);
  selectedCh.value = (descriptor.value?.chOptions ?? []).filter((value) => next.has(value));
  refresh({ resetBackoff: true });
}

function dataDetailLabel(value) {
  if (value === "raw") return "Raw — all samples";
  if (value === "auto") return "Auto";
  return value;
}

watch(() => props.sessionId, () => {
  descriptor.value = null;
  lastReadyDescriptor.value = null;
  selectedTargetId.value = null;
  selectedDataDetail.value = null;
  selectedTtl.value = null;
  selectedCh.value = null;
  refresh({ resetBackoff: true });
});

onMounted(() => {
  mounted = true;
  refresh({ resetBackoff: true });
});

onBeforeUnmount(() => {
  mounted = false;
  requestSequence += 1;
  clearPoll();
});
</script>

<template>
  <section class="grafana-view" :data-state="viewState" aria-labelledby="grafana-view-title">
    <header class="grafana-view__header">
      <div>
        <p class="grafana-view__eyebrow"><BarChart3 :size="15" aria-hidden="true" /> Grafana · 500 ms refresh</p>
        <h3 id="grafana-view-title">Morelia M4 comparison</h3>
      </div>
      <div class="grafana-view__actions">
        <a
          v-if="displayedGraph?.openUrl"
          class="button button--secondary button--small"
          :href="displayedGraph.openUrl"
          target="_blank"
          rel="noopener noreferrer"
        >
          Open in Grafana <ExternalLink :size="15" aria-hidden="true" />
        </a>
        <BaseButton
          variant="quiet"
          size="small"
          :disabled="retrying"
          @click="refresh({ resetBackoff: true })"
        >
          <RefreshCw :size="15" aria-hidden="true" /> {{ retrying ? "Checking…" : "Retry" }}
        </BaseButton>
      </div>
    </header>

    <p class="grafana-view__trust-notice">
      <AlertTriangle :size="17" aria-hidden="true" />
      <span><strong>Device view:</strong> this graph is filtered by device and display settings, not isolated to this session. It may include samples written by another run using the same device tag.</span>
    </p>

    <div v-if="descriptor?.targets.length" class="grafana-view__controls" aria-label="Grafana graph filters">
      <label class="grafana-view__control">
        Device
        <select :value="selectedTargetId" @change="selectTarget">
          <option v-for="target in descriptor.targets" :key="target.id" :value="target.id">{{ target.label }}</option>
        </select>
      </label>
      <label class="grafana-view__control">
        Data detail
        <select
          :value="selectedDataDetail"
          :disabled="!descriptor.dataDetailOptions.length"
          @change="selectDataDetail"
        >
          <option v-for="detail in descriptor.dataDetailOptions" :key="detail" :value="detail">
            {{ dataDetailLabel(detail) }}
          </option>
        </select>
      </label>
      <fieldset class="grafana-view__ttl">
        <legend>Analog channels</legend>
        <div class="grafana-view__ttl-options">
          <label v-for="channel in descriptor.chOptions" :key="channel" class="grafana-view__ttl-option">
            <input
              type="checkbox"
              :value="channel"
              :checked="selectedCh?.includes(channel)"
              @change="toggleCh"
            />
            {{ channel }}
          </label>
        </div>
      </fieldset>
      <fieldset class="grafana-view__ttl">
        <legend>TTL channels</legend>
        <div class="grafana-view__ttl-options">
          <label v-for="ttl in descriptor.ttlOptions" :key="ttl" class="grafana-view__ttl-option">
            <input
              type="checkbox"
              :value="ttl"
              :checked="selectedTtl?.includes(ttl)"
              @change="toggleTtl"
            />
            {{ ttl }}
          </label>
        </div>
      </fieldset>
    </div>

    <div
      v-if="viewState !== 'ready'"
      class="grafana-view__status"
      :class="{ 'grafana-view__status--over-graph': hasRetainedGraph }"
      role="status"
      aria-live="polite"
    >
      <RefreshCw v-if="viewState === 'loading'" class="grafana-view__spin" :size="22" aria-hidden="true" />
      <AlertTriangle v-else :size="22" aria-hidden="true" />
      <div>
        <strong>{{ viewState === "unavailable" ? "Grafana unavailable" : viewState === "not_configured" ? "Grafana not configured" : viewState === "not_applicable" ? "No matching M4 graph" : "Loading Grafana" }}</strong>
        <p>{{ statusCopy }}</p>
        <p v-if="hasRetainedGraph">The last successfully loaded graph remains below for reference.</p>
      </div>
    </div>

    <iframe
      v-if="displayedGraph?.embedUrl"
      class="grafana-view__frame"
      :src="displayedGraph.embedUrl"
      :title="frameTitle"
      loading="lazy"
      referrerpolicy="no-referrer"
    />
  </section>
</template>

<style scoped>
.grafana-view { display: grid; gap: var(--space-4); }
.grafana-view__header { display: flex; align-items: flex-start; justify-content: space-between; gap: var(--space-4); }
.grafana-view__header h3 { margin: 0.2rem 0 0; }
.grafana-view__eyebrow { display: flex; align-items: center; gap: var(--space-2); margin: 0; color: var(--text-accent); font-size: var(--fs-xs); font-weight: var(--fw-bold); letter-spacing: var(--ls-wide); text-transform: uppercase; }
.grafana-view__actions { display: flex; flex-wrap: wrap; gap: var(--space-2); }
.grafana-view__actions a { text-decoration: none; }
.grafana-view__trust-notice { display: flex; align-items: flex-start; gap: var(--space-3); margin: 0; padding: var(--space-3) var(--space-4); color: #765006; border: 1px solid #e6c27a; border-radius: var(--radius-md); background: #fff9e8; font-size: var(--fs-xs); line-height: var(--lh-body); }
.grafana-view__trust-notice svg { flex: 0 0 auto; margin-top: 0.1rem; }
.grafana-view__controls { display: flex; flex-wrap: wrap; gap: var(--space-3); }
.grafana-view__control { display: grid; min-width: min(240px, 100%); gap: var(--space-2); color: var(--text-muted); font-size: var(--fs-xs); font-weight: var(--fw-semibold); }
.grafana-view__controls select { min-height: 40px; padding: 0 var(--space-3); color: var(--ink); border: 1px solid var(--border-card); border-radius: var(--radius-md); background: var(--surface-card); }
.grafana-view__ttl { min-width: min(320px, 100%); margin: 0; padding: 0; border: 0; color: var(--text-muted); font-size: var(--fs-xs); font-weight: var(--fw-semibold); }
.grafana-view__ttl legend { margin-bottom: var(--space-2); padding: 0; }
.grafana-view__ttl-options { display: flex; min-height: 40px; align-items: center; flex-wrap: wrap; gap: var(--space-2) var(--space-4); }
.grafana-view__ttl-option { display: inline-flex; align-items: center; gap: var(--space-2); color: var(--ink); font-weight: var(--fw-medium); }
.grafana-view__ttl-option input { width: 16px; height: 16px; margin: 0; accent-color: var(--text-accent); }
.grafana-view__status { display: flex; min-height: 190px; align-items: center; justify-content: center; gap: var(--space-3); padding: var(--space-5); color: var(--text-muted); border: 1px dashed var(--border-card); border-radius: var(--radius-md); background: var(--surface-muted); text-align: left; }
.grafana-view__status strong { color: var(--ink); }
.grafana-view__status p { max-width: 620px; margin: var(--space-1) 0 0; font-size: var(--fs-sm); line-height: var(--lh-body); }
.grafana-view__status--over-graph { min-height: auto; color: #9f5c08; border-style: solid; border-color: #e6c27a; background: #fff9e8; }
.grafana-view__frame { width: 100%; min-height: 560px; border: 1px solid var(--border-card); border-radius: var(--radius-md); background: #111217; }
.grafana-view__spin { animation: grafana-spin 1s linear infinite; }
@keyframes grafana-spin { to { transform: rotate(360deg); } }
@media (max-width: 720px) {
  .grafana-view__header { align-items: stretch; flex-direction: column; }
  .grafana-view__actions > * { flex: 1; }
  .grafana-view__controls { display: grid; }
  .grafana-view__control, .grafana-view__ttl { min-width: 0; }
  .grafana-view__frame { min-height: 420px; }
}
</style>
