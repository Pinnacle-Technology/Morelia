import { afterEach, describe, expect, it, vi } from "vitest";
import { loadGrafanaView, normalizeGrafanaView } from "./grafana-view-api";

afterEach(() => vi.restoreAllMocks());

const readyPayload = {
  state: "ready",
  scope: "device_filters",
  notice: "M4 device view",
  targets: [{ id: "0:0", label: "POD 8206", channels: ["CH0", "CH1", "CH0"] }],
  selected_target_id: "0:0",
  data_detail_options: ["raw", "auto", "50ms", "250ms"],
  selected_data_detail: "250ms",
  ttl_options: ["TTL1", "TTL2", "TTL3", "TTL4"],
  selected_ttl: ["TTL1", "TTL3"],
  embed_url: "http://127.0.0.1:3100/d-solo/morelia-m4-comparison/morelia-raw-vs-m4?refresh=500ms",
  open_url: "http://127.0.0.1:3100/d/morelia-m4-comparison/morelia-raw-vs-m4?refresh=500ms",
  retry_after_seconds: 2,
  message: null,
};

describe("loadGrafanaView", () => {
  it("defaults to all analog channels with an older backend", () => {
    const result = normalizeGrafanaView(readyPayload);
    expect(result.chOptions).toEqual(["CH0", "CH1", "CH2"]);
    expect(result.selectedCh).toEqual(["CH0", "CH1", "CH2"]);
    for (const url of [result.embedUrl, result.openUrl]) {
      expect(new URL(url).searchParams.getAll("var-show_ch")).toEqual(["CH0", "CH1", "CH2"]);
    }
  });

  it("retains a requested analog subset in both older-backend URLs without changing TTL", async () => {
    const payload = {
      ...readyPayload,
      embed_url: readyPayload.embed_url + "&var-show_ch=CH0&var-show_ttl=TTL1&var-show_ttl=TTL3",
      open_url: readyPayload.open_url + "&var-show_ch=CH0&var-show_ttl=TTL1&var-show_ttl=TTL3",
    };
    const fetchMock = vi.fn(async () => ({ ok: true, json: async () => payload }));
    vi.stubGlobal("fetch", fetchMock);
    const result = await loadGrafanaView(3, { showCh: ["CH2", "CH1", "CH1"], showTtl: ["TTL1", "TTL3"] });
    const query = new URL(fetchMock.mock.calls[0][0], "http://localhost").searchParams;
    expect(query.getAll("show_ch")).toEqual(["CH1", "CH2"]);
    expect(query.getAll("show_ttl")).toEqual(["TTL1", "TTL3"]);
    expect(result.selectedCh).toEqual(["CH1", "CH2"]);
    expect(result.selectedTtl).toEqual(["TTL1", "TTL3"]);
    for (const url of [result.embedUrl, result.openUrl]) {
      const params = new URL(url).searchParams;
      expect(params.getAll("var-show_ch")).toEqual(["CH1", "CH2"]);
      expect(params.getAll("var-show_ttl")).toEqual(["TTL1", "TTL3"]);
      expect(params.get("refresh")).toBe("500ms");
    }
  });

  it("keeps an empty analog selection empty across repeated older-backend loads", async () => {
    const fetchMock = vi.fn(async () => ({ ok: true, json: async () => readyPayload }));
    vi.stubGlobal("fetch", fetchMock);
    const first = await loadGrafanaView(3, { showCh: [], showTtl: [] });
    const polled = await loadGrafanaView(3, { showCh: first.selectedCh, dataDetail: "50ms", targetId: "0:0" });
    expect(fetchMock.mock.calls[0][0]).toContain("show_ttl=__none__&show_ch=__none__");
    expect(first.selectedCh).toEqual([]);
    expect(polled.selectedCh).toEqual([]);
    expect(new URL(polled.embedUrl).searchParams.getAll("var-show_ch")).toEqual(["__none__"]);
    expect(new URL(polled.openUrl).searchParams.getAll("var-show_ch")).toEqual(["__none__"]);
  });

  it("uses the new backend's authoritative selection and URLs", async () => {
    const payload = {
      ...readyPayload,
      ch_options: ["CH0", "CH1", "CH2"],
      selected_ch: ["CH2"],
      embed_url: readyPayload.embed_url + "&var-show_ch=CH2",
      open_url: readyPayload.open_url + "&var-show_ch=CH2",
    };
    vi.stubGlobal("fetch", vi.fn(async () => ({ ok: true, json: async () => payload })));
    const result = await loadGrafanaView(3, { showCh: ["CH0"] });
    expect(result.chOptions).toEqual(["CH0", "CH1", "CH2"]);
    expect(result.selectedCh).toEqual(["CH2"]);
    expect(new URL(result.embedUrl).searchParams.getAll("var-show_ch")).toEqual(["CH2"]);
    expect(new URL(result.openUrl).searchParams.getAll("var-show_ch")).toEqual(["CH2"]);
    const empty = normalizeGrafanaView({ ...payload, selected_ch: [] });
    expect(empty.selectedCh).toEqual([]);
  });

  it("does not send arbitrary analog selections or add them to fallback URLs", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    await expect(loadGrafanaView(3, { showCh: ["CH9"] })).rejects.toThrow("CH0, CH1, and CH2");
    expect(fetchMock).not.toHaveBeenCalled();
    const result = normalizeGrafanaView(readyPayload, { showCh: ["CH0", "CH9"] });
    expect(result.selectedCh).toEqual(["CH0"]);
    expect(new URL(result.embedUrl).searchParams.getAll("var-show_ch")).toEqual(["CH0"]);
  });

  it("pins M4 refresh to 500 ms when an older backend still returns 5 s", () => {
    const result = normalizeGrafanaView({
      ...readyPayload,
      embed_url: readyPayload.embed_url.replace("500ms", "5s") + "&var-channel=CH1",
    });
    const url = new URL(result.embedUrl);
    expect(url.searchParams.get("refresh")).toBe("500ms");
    expect(url.searchParams.has("var-channel")).toBe(false);
  });

  it("rejects a retired dashboard even when the backend calls it ready", () => {
    const result = normalizeGrafanaView({
      ...readyPayload,
      embed_url: "http://localhost:3100/d-solo/retired-dashboard/channel",
    });
    expect(result.state).toBe("unavailable");
    expect(result.embedUrl).toBeNull();
  });

  it("encodes the session and target without sending credentials", async () => {
    const fetchMock = vi.fn(async () => ({ ok: true, json: async () => readyPayload }));
    vi.stubGlobal("fetch", fetchMock);

    const result = await loadGrafanaView("run/3", { targetId: "0:0" });

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/sessions/run%2F3/grafana-view?target_id=0%3A0",
      expect.objectContaining({ headers: expect.objectContaining({ Accept: "application/json" }) }),
    );
    expect(fetchMock.mock.calls[0][0]).not.toMatch(/token|credential|secret/i);
    expect(result).toMatchObject({ state: "ready", selectedTargetId: "0:0" });
  });

  // M4 detail and repeated TTL selections cross the browser API boundary intact.
  it("encodes the selected M4 controls without exposing arbitrary variables", async () => {
    const fetchMock = vi.fn(async () => ({ ok: true, json: async () => readyPayload }));
    vi.stubGlobal("fetch", fetchMock);

    const result = await loadGrafanaView(3, {
      targetId: "0:0",
      dataDetail: "50ms",
      showTtl: ["TTL1", "TTL3"],
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/sessions/3/grafana-view?target_id=0%3A0&data_detail=50ms&show_ttl=TTL1&show_ttl=TTL3",
      expect.any(Object),
    );
    expect(result).toMatchObject({
      dataDetailOptions: ["raw", "auto", "50ms", "250ms"],
      selectedDataDetail: "250ms",
      ttlOptions: ["TTL1", "TTL2", "TTL3", "TTL4"],
      selectedTtl: ["TTL1", "TTL3"],
    });
  });

  // No checked TTL boxes is represented by the server's explicit no-TTL sentinel.
  it("encodes an empty TTL selection explicitly", async () => {
    const fetchMock = vi.fn(async () => ({ ok: true, json: async () => readyPayload }));
    vi.stubGlobal("fetch", fetchMock);

    await loadGrafanaView(3, { dataDetail: "250ms", showTtl: [] });

    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/v1/sessions/3/grafana-view?data_detail=250ms&show_ttl=__none__",
    );
  });

  it("normalizes channel choices and bounds the retry interval", () => {
    const result = normalizeGrafanaView(readyPayload);
    expect(result.targets[0].channels).toEqual(["CH0", "CH1"]);
    expect(result.retryAfterSeconds).toBe(5);
  });

  it("rejects non-http iframe URLs even when a response claims to be ready", () => {
    const result = normalizeGrafanaView({ ...readyPayload, embed_url: "javascript:alert(1)" });
    expect(result).toMatchObject({ state: "unavailable", embedUrl: null });
    expect(result.message).toContain("unsafe or missing");
  });

  it("rejects graph URLs that contain browser-visible credentials", () => {
    const embeddedCredential = normalizeGrafanaView({
      ...readyPayload,
      embed_url: "http://viewer:password@127.0.0.1:3100/d-solo/lab/view",
    });
    const tokenQuery = normalizeGrafanaView({
      ...readyPayload,
      embed_url: "http://127.0.0.1:3100/d-solo/lab/view?api_token=visible",
    });
    expect(embeddedCredential.state).toBe("unavailable");
    expect(tokenQuery.state).toBe("unavailable");
  });

  it("rejects unusable descriptor responses", () => {
    expect(() => normalizeGrafanaView({ state: "available" })).toThrow("unusable response shape");
  });
});
