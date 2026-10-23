import { afterEach, expect, it, vi } from "vitest";
import { createRenderer, nextTick, ssrContextKey } from "vue";
import SessionDetailPage from "./SessionDetailPage.vue";

const fixture = vi.hoisted(() => ({ snapshot: null }));
vi.mock("../session-detail-api", () => ({ loadSessionDetail: async () => fixture.snapshot }));
vi.mock("../session-activity-api", () => ({ loadSessionActivity: async () => ({ items: [] }) }));
vi.mock("../session-notes-api", () => ({
  loadSessionNotes: async () => ({ items: [] }), createSessionNote: vi.fn(), updateSessionNote: vi.fn(),
}));
vi.mock("../session-events", async (original) => ({
  ...await original(),
  createSessionEventStream: ({ onChange }) => ({
    start: () => onChange({ state: "live", events: [], error: null }), stop() {},
  }),
}));

// The SSR-compiled page runs its real setup and lifecycle against a headless host.
const renderer = createRenderer({
  createComment: () => ({}), insert() {}, remove() {}, parentNode: () => null,
  nextSibling: () => null,
});
let app;
afterEach(() => { app?.unmount(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

function reportedSession() {
  const hardwareIds = ["AB0MZ1ACA", "AB0MZ1ADA", "AB0MZ1AEA", "AB0MZ1AFA", "AB0MZ1ANA",
    "AB0MZ1AVA", "AB0MZ1AYA", "AB0MZ1AZA", "AB0MZ1B2A", "AB0MZ1B3A", "AB0MZ4ZYA", "AB0MZ501A", "AB0MZBJBA"];
  return {
    session: { id: 63, name: "Recording", status: "active",
      device_flows: hardwareIds.map((hardware_id) => ({ device_type: "pod8401hr", hardware_id, sinks: [] })) },
    health: null, phase: "running", outbox_health: "current", watchdog_state: "running",
    latest_report: { event_id: 1030771, received_at: "2026-10-23T20:32:50.896604",
      devices: hardwareIds.map((id) => ({ device_id: `pod8401hr:${id}`, stream_status: "healthy" })) },
    sinks: [], operations: [], incidents: [], gaps: [],
  };
}

async function mount(snapshot) {
  fixture.snapshot = snapshot;
  vi.stubGlobal("window", {});
  vi.spyOn(console, "info").mockImplementation(() => {});
  app = renderer.createApp({ ...SessionDetailPage, render: () => null }, { sessionId: "63" });
  app.provide(ssrContextKey, {});
  const vm = app.mount({});
  await nextTick();
  await nextTick();
  return vm.$.setupState;
}

it("keeps the reported session 63 green when only the poller health is absent", async () => {
  const page = await mount(reportedSession());
  expect(page.detailState).toBe("live");
  expect(page.view.health).toBe("Healthy");
  expect(page.ratState).toBe("running");
  expect(page.ratCaption).toBe("Session streaming");
  expect(page.flowInputs.health).toBe("Healthy");
  expect(page.streamRows).toHaveLength(13);
});

it.each(["stale", "overflow", null])("does not infer healthy from %s telemetry", async (outbox_health) => {
  const page = await mount({ ...reportedSession(), outbox_health });
  expect(page.view.health).toBe("Not reporting");
  expect(page.ratState).toBe("recovering");
});

it.each(["recovering", "delayed", "unreachable", "failed", "unknown"])("preserves explicit backend health %s", async (health) => {
  const page = await mount({ ...reportedSession(), health });
  expect(page.view.health).not.toBe("Healthy");
  expect(page.ratState).not.toBe("running");
});

it.each(["missing", "empty", "duplicate", "suspect", "unhealthy", "unknown", "recovering", "sink-failed", "sample-loss"])(
  "does not infer healthy when report evidence is %s", async (problem) => {
    const snapshot = reportedSession();
    const devices = snapshot.latest_report.devices;
    if (problem === "missing") devices.pop();
    else if (problem === "empty") devices.length = 0;
    else if (problem === "duplicate") devices[1].device_id = devices[0].device_id;
    else if (problem === "recovering") devices[0].pending_recovery = true;
    else if (problem === "sink-failed" || problem === "sample-loss") snapshot.sinks.push({
      source_id: devices[0].device_id, sink_id: "csv",
      health: problem === "sink-failed" ? "failed" : null,
      sample_loss: problem === "sample-loss" ? 12 : 0,
    });
    else devices[0].stream_status = problem;
    const page = await mount(snapshot);
    expect(page.view.health).not.toBe("Healthy");
    expect(page.ratState).not.toBe("running");
  },
);

it("restores the warning if a subsequent detail refresh fails", async () => {
  const page = await mount(reportedSession());
  expect(page.view.health).toBe("Healthy");
  page.detailState = "unavailable";
  await nextTick();
  expect(page.view.health).toBe("Not reporting");
  expect(page.ratState).toBe("recovering");
});

it.each(["starting", "stopping", "completed"])("preserves the %s lifecycle", async (status) => {
  const snapshot = reportedSession();
  snapshot.session.status = status;
  const page = await mount(snapshot);
  expect(page.view.health).not.toBe("Healthy");
  expect(page.ratState).not.toBe("running");
});
