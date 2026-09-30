import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const source = readFileSync(new URL("./GrafanaView.vue", import.meta.url), "utf8");

describe("M4 Grafana view", () => {
  it("always identifies the graph as device scoped rather than session isolated", () => {
    expect(source).toContain("Device view:");
    expect(source).toContain("not isolated to this session");
    expect(source).toContain("same device tag");
  });

  it("loads only while mounted and cancels its polling lifecycle on unmount", () => {
    expect(source).toContain("onMounted(() =>");
    expect(source).toContain("onBeforeUnmount(() =>");
    expect(source).toContain("clearPoll()");
    expect(source).toContain("if (!mounted");
    expect(source).toContain("2 ** consecutiveFailures");
  });

  it("uses a guarded interactive iframe without propagating referrer data", () => {
    expect(source).toContain('referrerpolicy="no-referrer"');
    expect(source).toContain('loading="lazy"');
    expect(source).toContain('rel="noopener noreferrer"');
    expect(source).not.toContain("javascript:");
    expect(source).not.toMatch(/api[_-]?token|credential|secret/i);
  });

  it("keeps acquisition independent in its outage message", () => {
    expect(source).toContain("Acquisition and Influx writing are unaffected");
    expect(source).toContain("last successfully loaded graph");
  });

  // The Vue surface exposes M4 detail and accessible analog/TTL controls.
  it("lets operators control the M4 detail and analog/TTL variables", () => {
    expect(source).toContain("Data detail");
    expect(source).toContain("Analog channels");
    expect(source).toContain("TTL channels");
    expect(source).toContain('type="checkbox"');
    expect(source).toContain("selectedDataDetail");
    expect(source).toContain("selectedTtl");
    expect(source).toContain("selectedCh");
  });
});
