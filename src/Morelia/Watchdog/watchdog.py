from Morelia.Watchdog.hardwareMonitor import HardwareMonitor
from Morelia.Watchdog.dataflowMonitor import DataFlowMonitor
from Morelia.Watchdog.reporting import (
    HARDWARE_HEALTH as _HARDWARE_HEALTH,
    assess_stream,
    build_action_signal,
    build_compact_stream_report,
    build_recovery_event,
    extract_action_error,
    summarize_watchdog_health,
)
from Morelia.Stream.data_flow import DataFlow
import logging
import math
import time
import multiprocessing as mp
import threading
import uuid

# Wall-clock epoch captured when this module is first imported (≈ program start)
_PROGRAM_START = time.time()
NATIVE_READINESS_TIMEOUT_SEC = 30.0

def _now_rel():
    """
    Return the current time relative to when this module was imported.

    Output: float. Seconds elapsed since program start.
    """
    return time.time() - _PROGRAM_START

def _to_rel(epoch_sec):
    """
    Convert an epoch timestamp to seconds relative to program start.

    Output: float or None. Relative seconds, or None when epoch_sec is None.
    """
    return None if epoch_sec is None else epoch_sec - _PROGRAM_START

def _resolve_interval(spec, key, default):
    """
    Resolve a shared or per-item polling interval.

    Output: float or int. The value for key, the shared scalar value, or
    default when spec is None or the key is absent.
    """
    if spec is None:
        return default
    if isinstance(spec, dict):
        return spec.get(key, default)
    return spec   # scalar -> shared

def _normalize_recovery_policy(policy):
    if policy is None:
        return "recommend"
    if hasattr(policy, "value"):
        policy = policy.value
    value = str(policy).strip().lower()
    if value in {"recommend", "recommended", "manual"}:
        return "recommend"
    if value == "automate":
        return "automate"
    return "recommend"

def _manifest_lookup(manifest, *path):
    current = manifest
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current

def _resolve_recovery_policy(manifest=None, recovery_policy=None):
    if recovery_policy is not None:
        return _normalize_recovery_policy(recovery_policy)

    policy = getattr(manifest, "policy", None)
    if policy is None and isinstance(manifest, dict):
        policy = manifest.get("policy")

    return _normalize_recovery_policy(policy)

class Watchdog:
    """
    Build watchdog reports for hardware-only devices or DataFlow streams.

    Responsibilities:
    - Poll stream/worker/heartbeat signals.
    - Track consecutive stream failures.
    - Assess stream health.
    - Return compact or verbose watchdog dictionaries.
    """
    def __init__(self, flowgraph:DataFlow = None, devices=(), failure_threshold:int = 3, max_heartbeat_age_sec:float = 2.0, first_packet_timeout_sec:float = None, max_auto_restart_attempts:int = 3, manifest=None, recovery_policy=None, reconstruction_hook=None, sample_rates=None, port_resolver=None, *, dataflow_monitor_factory=None, port_presence_checker=None):
        """
        Initialize stream and standalone-device monitoring state.

        ``dataflow_monitor_factory`` optionally supplies an application-owned
        DataFlowMonitor subclass. Ordinary callers use the standard monitor.

        Output: None.
        """
        #Input validation
        if flowgraph is None and not devices: 
            raise ValueError("Watchdog needs a flowgraph, devices, or both.")
        
        #Set default value
        if max_auto_restart_attempts < 1:
            raise ValueError("max_auto_restart_attempts must be at least 1.")
        if first_packet_timeout_sec is not None and first_packet_timeout_sec <= 0:
            raise ValueError("first_packet_timeout_sec must be greater than zero.")

        self.failure_threshold = failure_threshold
        self.max_heartbeat_age_sec = max_heartbeat_age_sec
        self.first_packet_timeout_sec = (
            max_heartbeat_age_sec
            if first_packet_timeout_sec is None
            else first_packet_timeout_sec
        )
        self.max_auto_restart_attempts = max_auto_restart_attempts
        self._sample_rates = tuple(sample_rates or ())
        if flowgraph is not None and self._sample_rates:
            stream_count = len(flowgraph._network)
            if len(self._sample_rates) != stream_count:
                raise ValueError(
                    "sample_rates must contain one value per DataFlow stream."
                )
            if any(
                isinstance(rate, bool) or not isinstance(rate, int) or rate <= 0
                for rate in self._sample_rates
            ):
                raise ValueError("sample_rates must contain positive integers.")
        self.recovery_policy = _resolve_recovery_policy(
            manifest=manifest,
            recovery_policy=recovery_policy,
        )
        
        #Setup dataflow monitor
        self.dataflow_monitor = (
            (dataflow_monitor_factory or DataFlowMonitor)(
                flowgraph,
                reconstruction_hook=reconstruction_hook,
                sample_rates=self._sample_rates,
                port_resolver=port_resolver,
                port_presence_checker=port_presence_checker,
            )
            if flowgraph is not None
            else None
        )
        
        #Setup hardware device monitor
        self._standalone_devices = self._filter_standalone_devices(devices)

        self._stream_watchers = {}      # stream_index -> StreamWatcher
        self._device_watchers = {}      # device_key   -> DeviceWatcher
        self._stream_results = {}
        self._device_results = {}
        self._results_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._urgent_report_event = threading.Event()
        self._on_stream_result = None
        self._on_device_result = None

    def preflight(self, *, attempts=3, timeout_sec=5.0, require_ready=True):
        """
        Reset and verify every configured source and standalone device.

        Output: dict.
        - ok: bool (True | False).
          - True: every device passed verification.
          - False: one or more devices failed verification.
        - devices: dict[str, dict]. Maps each device port or object ID to:
          - ok: bool (True | False).
            - True: this device passed verification.
            - False: this device could not be opened or verified.
          - attempts_used: int. Number of verification attempts used.
          - reset: dict, present on verification success.
            - ok: bool (True | False).
              - True: the reset command completed.
              - False: the reset operation failed.
            - drained_packets: int, present on reset success.
            - device_quiet: bool (True | False), present on reset success.
              - True: the device stopped producing packets during reset.
              - False: the reset deadline ended before the device became quiet.
            - error: str, present on reset failure.
          - error: str, present when opening or verification fails.

        Raises RuntimeError when DataFlow workers already own the ports, or
        when require_ready is True and one or more devices are not ready.
        """
        if self.dataflow_monitor is not None and any(
            worker is not None
            for worker in getattr(self.dataflow_monitor.flowgraph, "_workers", [])
        ):
            raise RuntimeError(
                "preflight() must run before flowgraph.collect() — workers already own the ports.")
        hw = HardwareMonitor(failure_threshold=self.failure_threshold)
        results = {}
        flow_sources = []
        if self.dataflow_monitor is not None:
            flow_sources = [src for src, _ in self.dataflow_monitor.flowgraph._network]
        sources = flow_sources + list(self._standalone_devices)
        for index, dev in enumerate(sources):
            desired_sample_rate = (
                self._sample_rates[index]
                if index < len(flow_sources) and self._sample_rates
                else None
            )
            results[str(getattr(dev, "port", id(dev)))] = hw.preflight_device(
                dev,
                attempts=attempts,
                timeout_sec=timeout_sec,
                desired_sample_rate=desired_sample_rate,
            )
        if self.dataflow_monitor is not None:
            self.dataflow_monitor.refresh_rebuild_snapshot()
        not_ready = [k for k, r in results.items() if not r["ok"]]
        for port in not_ready:
            result = results[port]
            logging.getLogger(__name__).error(
                "device preflight failed",
                extra={
                    "action": "preflight",
                    "port": port,
                    "attempt": result.get("attempts_used"),
                    "reason": result.get("error"),
                },
            )
        if require_ready and not_ready:
            raise RuntimeError(f"preflight failed for {not_ready}")
        return {"ok": not not_ready, "devices": results}

    def spawn_threads(self, *, stream_interval, device_interval, timeout_sec, on_stream_result, on_device_result):
        """
        Spawn one non-blocking watcher thread per stream and standalone device.

        Output: None.
        """
        self._stop_event.clear()
        self._on_stream_result = on_stream_result
        self._on_device_result = on_device_result

        if self.dataflow_monitor is not None:
            for idx in self.dataflow_monitor.get_all_streams_index(): #Iterate through stream list and spawn StreamWatcher
                with self._results_lock:#acquire the lock
                    self._stream_results[idx] = {
                        "stream_index": idx, 
                        "stream_health": "unknown",
                        "summary": "starting", 
                        "signals": {},
                        "rule": "starting",
                        "action": {"taken": "none", "detail": None}}
                w = StreamWatcher(idx, self.dataflow_monitor,
                               failure_threshold=self.failure_threshold,
                               max_heartbeat_age_sec=self.max_heartbeat_age_sec,
                               first_packet_timeout_sec=self.first_packet_timeout_sec,
                              interval_sec=_resolve_interval(stream_interval, idx, 30), timeout_sec=timeout_sec,
                              recovery_policy=self.recovery_policy,
                              max_auto_restart_attempts=self.max_auto_restart_attempts,
                              native_transport=bool(self.dataflow_monitor.snapshot_config[idx]["source"]["source_dict"].get("use_d2xx")),
                              publish=self._publish_stream)
                self._stream_watchers[idx] = w
                w.start()

        for device in self._standalone_devices:
            key = DeviceWatcher._device_key(device)
            w = DeviceWatcher(
                device, failure_threshold=self.failure_threshold,
                interval_sec=_resolve_interval(device_interval, key, 30),
                timeout_sec=timeout_sec,
                publish=self._publish_device)
            with self._results_lock:
                self._device_results[key] = {"status": "starting"}
            self._device_watchers[key] = w
            w.start() 

    def stop(self):
        """
        Signal all watcher loops to stop and wait briefly for their threads.

        Output: None.

        This does not stop DataFlow workers or release monitor resources; use
        close() for full cleanup.
        """
        self._stop_event.set()
        self._urgent_report_event.set()
        watchers = (*self._stream_watchers.values(), *self._device_watchers.values())
        for w in watchers:
            w.stop()
        for w in watchers:
            w.join(timeout=10.0)

    def get_report(self, verbose=False):
        """
        Return the latest combined stream and standalone-device snapshot.

        Output: dict.
        - watchdog_status: str (unknown | ok | failed | degraded).
          - unknown: no stream or device health values are available.
          - ok: every monitored item is healthy.
          - failed: every monitored item is unhealthy.
          - degraded: health is mixed, suspect, unknown, or unrecognized.
        - checked_at: float. Seconds since program start.
        - streams: list[dict]. Compact reports when verbose is False; verbose
          reports when verbose is True.
          Compact stream keys:
          - stream_index: int or None.
          - port: str or None.
          - port_owner: str or None (main | worker | none | None).
            - main: the main-process source object has the port open.
            - worker: the live stream worker owns the port.
            - none: neither the main process nor a worker owns the port.
            - None: ownership could not be determined.
          - stream_health: str or None
            (healthy | suspect | unhealthy | unknown | None).
            - healthy: worker and heartbeat signals are normal.
            - suspect: a transient failure or reconnect retry is in progress.
            - unhealthy: a definite failure or failure threshold was reached.
            - unknown: the watcher has started but has not completed a check.
            - None: the field is absent from the input report.
          - worker_status: str or None
            (alive | dead | missing | unknown | None).
            - alive: the worker process exists and is running.
            - dead: the worker process exists but has exited.
            - missing: no worker process object exists for the stream.
            - unknown: reading worker status failed.
            - None: no worker signal is available yet.
          - heartbeat: str or None (fresh | stale | missing | None).
            - fresh: data arrived within the allowed heartbeat age.
            - stale: data exists but is older than the allowed age.
            - missing: no heartbeat data is available or the read failed.
            - None: no heartbeat signal is available yet.
          - failure_count: int or None.
          - action: str or None
            (none | check_failed | stopped_stream_waiting_for_reconnect |
            port_check_failed | waiting_for_port | waiting_for_port_release |
            restart_failed | waiting_for_heartbeat | connection_restored |
            reconnect_failed_stop_stream_failed |
            reconnect_failed_stop_stream_completed | None).
            - none: the check required no recovery action.
            - check_failed: the watcher caught an unexpected check error.
            - stopped_stream_waiting_for_reconnect: the failure escalated and
              the worker was stopped before reconnecting.
            - port_check_failed: checking whether the port exists raised.
            - waiting_for_port: the expected port is absent from the OS list.
            - waiting_for_port_release: the port exists but cannot be opened.
            - restart_failed: rebuilding or starting the stream failed.
            - waiting_for_heartbeat: PING failed after restart and the worker
              is being verified through fresh sample data.
            - connection_restored: restart succeeded and was verified by PING or a
              fresh replacement-worker heartbeat.
            - reconnect_failed_stop_stream_failed: PING failed after restart,
              and stopping that failed restart also raised.
            - reconnect_failed_stop_stream_completed: PING failed after
              restart, and the restarted worker was stopped successfully.
            - None: no action field is available.
          - reason: str or None. Compact rule text or the verbose summary.
          Verbose stream keys:
          - stream_index: int.
          - source_class: str or None.
          - sink_classes: list[str].
          - stream_health: str
            (healthy | suspect | unhealthy | unknown).
            - healthy: worker and heartbeat signals are normal.
            - suspect: a transient failure or reconnect retry is in progress.
            - unhealthy: a definite failure or failure threshold was reached.
            - unknown: the watcher has started but has not completed a check.
          - summary: str.
          - port: str or None.
          - port_owner: str or None (main | worker | none | None).
            - main: the main-process source object has the port open.
            - worker: the live stream worker owns the port.
            - none: neither the main process nor a worker owns the port.
            - None: ownership could not be determined.
          - checked_at: float.
          - rule: str
            (worker_alive_heartbeat_fresh | stream_reconnecting |
            worker_not_alive |
            worker_alive_heartbeat_stale_below_threshold |
            worker_alive_heartbeat_stale_threshold_reached |
            no_data_below_threshold | no_data_threshold_reached |
            heartbeat_missing | action_failure | starting).
            - worker_alive_heartbeat_fresh: worker and heartbeat are healthy.
            - stream_reconnecting: a reconnect retry is in progress.
            - worker_not_alive: the worker is dead, missing, or unknown outside
              an expected reconnect state.
            - worker_alive_heartbeat_stale_below_threshold: heartbeat is stale,
              but the consecutive-failure threshold has not been reached.
            - worker_alive_heartbeat_stale_threshold_reached: heartbeat is
              stale and the failure threshold has been reached.
            - no_data_below_threshold: no packet has ever arrived, but the
              failure threshold has not been reached.
            - no_data_threshold_reached: no packet has ever arrived and the
              failure threshold has been reached.
            - heartbeat_missing: heartbeat data is unavailable for a reason
              other than data never starting.
            - action_failure: fallback for a failure not matched above.
            - starting: the watcher has not completed its first check.
          - action: dict.
            - taken: str
              (none | check_failed | stopped_stream_waiting_for_reconnect |
              port_check_failed | waiting_for_port |
              waiting_for_port_release | restart_failed | waiting_for_heartbeat |
              connection_restored |
              reconnect_failed_stop_stream_failed |
              reconnect_failed_stop_stream_completed).
              Uses the same meanings as the compact action field above.
            - detail: dict or None. Additional action-result fields, or None
              when taken is none.
          - signals: dict.
            - worker: dict.
              - status: str or None
                (alive | dead | missing | unknown | None).
                Uses the same meanings as compact worker_status above.
              - pid: int or None.
              - exitcode: int or None.
              - fault: dict or None. The exception the worker reported before
                exiting (error_type, reason, action, worker_exitcode), or None.
            - heartbeat: dict.
              - status: str (fresh | stale | missing).
                Uses the same meanings as compact heartbeat above.
              - last_data_at: float or None.
              - reason: str or None
                (None | data_older_than_max_age | no_health_sink_attached |
                no_data_seen_yet | heartbeat_check_failed: <error>).
                - None: heartbeat data is fresh.
                - data_older_than_max_age: heartbeat data exists but is stale.
                - no_health_sink_attached: no heartbeat-producing sink exists.
                - no_data_seen_yet: the health sink has received no packet.
                - heartbeat_check_failed: <error>: reading the heartbeat raised.
              - age_sec: float or None.
              - max_age_sec: float.
              - packet_count: int or None.
            - failure: dict.
              - count: int.
              - threshold: int.
              - last_error: str or None.
              - last_error_at: float or None.
        - devices: dict[str, dict]. Maps device keys to reports containing:
          - status: str
            (starting | connected | reconnected | ping failed | disconnected |
            suspect | unknown).
            - starting: the watcher has started but has not completed a poll.
            - connected: the existing device handle answered PING.
            - reconnected: the existing handle failed, but a rebuilt device
              answered PING in the same poll.
            - ping failed: PING and reconnect failed, but the failure count is
              still below the disconnection threshold.
            - disconnected: the consecutive-failure threshold was reached.
            - suspect: the device watcher itself caught an exception.
            - unknown: a completed poll returned no device state.
          - consecutive_failures: int, present after a normal hardware poll.
          - last_error: str or None, present after a normal hardware poll.
          - error: str, present when the watcher catches an exception.
          - checked_at: float, present after the first completed poll.
        """
        with self._results_lock:
            streams = [self._stream_results[i] for i in sorted(self._stream_results)]
            devices = dict(self._device_results)
        if not verbose:
            streams = [self.build_compact_stream_report(s) for s in streams]
        health = [s.get("stream_health") for s in streams]
        health += [_HARDWARE_HEALTH.get(d.get("status"), "suspect")
                   for d in devices.values()]
        return {"watchdog_status": self._summarize_watchdog_health(health),
                "checked_at": _now_rel(), "streams": streams, "devices": devices}

    def run(self, *, report_interval_sec=30.0, stream_interval=30.0, device_interval=30.0, timeout_sec=10.0, on_result=None, on_stream_result=None, on_device_result=None, verbose=False):
        """
        Run all watchers and periodically emit combined reports until stopped.

        Per-stream and per-device callbacks run independently in watcher
        threads, while on_result receives combined snapshots in this thread.

        Output: None.
        """
        self.spawn_threads(stream_interval=stream_interval, device_interval=device_interval,
                   timeout_sec=timeout_sec, on_stream_result=on_stream_result,
                   on_device_result=on_device_result)
        while not self._stop_event.is_set():
            if on_result is not None:
                on_result(self.get_report(verbose=verbose))
            self._urgent_report_event.wait(report_interval_sec)
            self._urgent_report_event.clear()

    def close(self):
        """
        Stop watchers, stream workers, monitor managers, and source resources.

        Safe to call repeatedly and never raises. Each step is isolated so a
        failure in one does not skip the others; otherwise a half-torn-down
        multiprocessing state can hang the interpreter at exit.

        Workers are stopped via the monitor's None-safe stop_stream rather
        than flowgraph.stop_collection(): once the watchdog has stopped a
        stream, flowgraph._workers holds None in that slot, and
        stop_collection() calls worker.join() unconditionally (crashing on
        the None).

        Output: None.
        """
        self.stop()
        if self.dataflow_monitor is not None:
            for i in self.dataflow_monitor.get_all_streams_index():
                try: self.dataflow_monitor.stop_stream(i)
                except Exception: pass
            try: self.dataflow_monitor.close()      # shuts the status Manager
            except Exception: pass
            self._cleanup_sources()
        for child in mp.active_children():           # backstop (unchanged)
            try: child.terminate()
            except Exception: pass
        for child in mp.active_children():
            try: child.join(timeout=2.0)
            except Exception: pass

    def stream_command(self, stream_index=None, command: str = None, *, requested_by:str, blocking:bool = False ):
        """
        Communication protocol for stream control(eg. stop, start, restart one stream)
        """
        if command is None and isinstance(stream_index, str):
            command = stream_index
            stream_index = None

        if self.dataflow_monitor is None:
            return {
                "ok": False,
                "stream_index": stream_index,
                "command": command,
                "requested_by": requested_by,
                "status": "detached",
                "error": "No DataFlowMonitor attached.",
            }

        if command not in {"restart_stream", "restart_session", "stop", "start"}:
            return {
                "ok": False,
                "stream_index": stream_index,
                "command": command,
                "requested_by": requested_by,
                "status": "invalid_command",
                "error": f"Unsupported stream command: {command}",
            }
        monitor = self.dataflow_monitor

        if command == "restart_stream":
            if stream_index is None:
                return {
                    "ok": False,
                    "stream_index": stream_index,
                    "command": command,
                    "requested_by": requested_by,
                    "status": "missing_stream_index",
                    "error": "restart_stream requires a stream_index.",
                }
            result = monitor.guarded_restart_one_stream(
                stream_index,
                requested_by=requested_by,
                blocking=blocking,
            )
            self._sync_stream_watcher_after_command(stream_index, command, result)
            return result

        if command == "restart_session":
            if stream_index is None:
                stream_index = 0
            result = monitor.guarded_restart_session(
                stream_index,
                requested_by=requested_by,
                blocking=blocking,
            )
            if result.get("ok"):
                for watcher in self._stream_watchers.values():
                    watcher.clear_recovery_state()
            return result

        if command == "stop":
            result = monitor.guarded_stop_dataflow(
                requested_by=requested_by,
                blocking=blocking,
            )
            return result

        if command == "start":
            result = monitor.guarded_start_dataflow(
                requested_by=requested_by,
                blocking=blocking,
            )
            if result.get("ok"):
                for watcher in self._stream_watchers.values():
                    watcher.clear_recovery_state()
            return result

        return {
            "ok": False,
            "stream_index": stream_index,
            "command": command,
            "requested_by": requested_by,
            "status": "invalid_command",
        }

    def _sync_stream_watcher_after_command(self, stream_index, command, result):
        if not result.get("ok"):
            return
        watcher = self._stream_watchers.get(stream_index)
        if watcher is not None and command in {"start", "stop", "restart_stream"}:
            watcher.clear_recovery_state()

    def _filter_standalone_devices(self, devices: list):
        """
        Exclude devices whose ports are already used by DataFlow streams.

        Output: list[object]. Device objects not represented in the DataFlow.
        """
        if self.dataflow_monitor is None: 
            return list(devices)
        
        stream_ports = set()
        for status in self.dataflow_monitor.get_all_stream_status(): 
            strict_native = False
            try:                                            
                port = self.dataflow_monitor.get_stream_port(status["stream_index"])
            except Exception:
                port = None
            try:
                source, _ = self.dataflow_monitor.flowgraph._network[status["stream_index"]]
                strict_native = getattr(source, "_d2xx_serial_only", False)
            except (AttributeError, IndexError, TypeError):
                pass
            if port is not None:
                stream_ports.add(("native", str(port)) if strict_native
                                 else ("legacy", str(port).lower()))
        
        standalone = []
        for device in devices:
            device_port = str(getattr(device, "port", ""))
            key = (("native", device_port) if getattr(device, "_d2xx_serial_only", False)
                   else ("legacy", device_port.lower()))
            if device_port and key in stream_ports:
                continue 
            standalone.append(device)
        return standalone
    
    # Compatibility aliases keep existing callers working while the report
    # formatting itself lives in reporting.py.
    _summarize_watchdog_health = staticmethod(summarize_watchdog_health)
    build_compact_stream_report = staticmethod(build_compact_stream_report)
        
    def _publish_stream(self, stream_index, report):
        """
        Store a stream report and invoke the optional stream callback.

        Output: None.
        """
        with self._results_lock:
            previous = self._stream_results.get(stream_index) or {}
            self._stream_results[stream_index] = report
        action = report.get("action", {}).get("taken") if isinstance(report, dict) else None
        if (report.get("recovery_progress") != previous.get("recovery_progress")
                or action in {"unplug_detected", "connection_restored", "heartbeat_age_exceeded"}):
            self._urgent_report_event.set()
        if self._on_stream_result is not None:
            try:
                self._on_stream_result(stream_index, report)
            except Exception:
                pass   # a bad user callback must not kill the watcher thread

    def _publish_device(self, key, report):
        """
        Store a device report and invoke the optional device callback.

        Output: None.
        """
        with self._results_lock:
            self._device_results[key] = report
        if self._on_device_result is not None:
            try:
                self._on_device_result(key, report)
            except Exception:
                pass

    def _cleanup_sources(self):
        """
        Run each DataFlow source's cleanup hook to release serial subprocesses.

        pod._manager (a PacketManager) launches queue_server.py and is the only
        thing that can stop it, but Morelia only calls its cleanup() from
        __del__, which may not run on an abrupt or Ctrl-C exit, so the server
        orphans and keeps holding the port. _manager survives collect()
        (close_port() nulls _port, not _manager) and the main process is the one
        that started the server, so calling cleanup() here actually terminates it.

        Output: None.
        """
        if self.dataflow_monitor is None or self.dataflow_monitor.flowgraph is None:
            return
        for source, _sinks in self.dataflow_monitor.flowgraph._network:
            cleanup = getattr(source, "cleanup", None)
            if callable(cleanup):
                try:
                    cleanup()
                except Exception:
                    pass

class StreamWatcher (threading.Thread):
    """Independent watchdog thread for a single (source, sinks) stream.

    Owns ALL mutable state for its stream (failure count, disconnected flag),
    so a slow reconnect or a crash here cannot touch any other stream.
    """   
    def __init__(self, stream_index: int, monitor: DataFlowMonitor, *, failure_threshold: int, max_heartbeat_age_sec: float, first_packet_timeout_sec: float = None, interval_sec: float, timeout_sec: float, recovery_policy: str, max_auto_restart_attempts: int = 3, native_transport: bool = False, publish):
        """
        Initialize a daemon watcher for one DataFlow stream.

        Output: None.
        """
        super().__init__(name=f"stream-watcher-{stream_index}", daemon=True)
        self.stream_index = stream_index
        self._monitor = monitor #shared dataflow monitor handed to each thread
        self._failure_threshold = failure_threshold
        self._max_heartbeat_age_sec = max_heartbeat_age_sec
        self._first_packet_timeout_sec = (
            max_heartbeat_age_sec
            if first_packet_timeout_sec is None
            else first_packet_timeout_sec
        )
        self._interval_sec = interval_sec
        self._timeout_sec = timeout_sec
        self._native_transport = native_transport
        if native_transport:
            self._first_packet_timeout_sec = max(
                self._first_packet_timeout_sec, NATIVE_READINESS_TIMEOUT_SEC,
            )
        self._recovery_policy = _normalize_recovery_policy(recovery_policy)
        self._max_auto_restart_attempts = max_auto_restart_attempts
        self._publish = publish                #fire-and-forget to report back what it got

        self._stop_event = threading.Event()
        self._failure_count = 0
        self._disconnected = False
        self._recovery_attempt_count = 0
        self._response_grace = None
        self._port_absent_since = None
        self._port_absent_packet_count = None
        self._physical_disconnect = False
        self._disconnect_episode_id = None
        self._disconnect_started_at_epoch = None
        self._first_packet_started_at = time.monotonic()
        self._first_packet_seen = False
        # Cool-down ticks after a worker crashes on its cold open, so recovery
        # keeps polling for the device without respawning a doomed worker every
        # tick during the port/replug settle window.
        self._cold_open_backoff_ticks = 2
        self._cold_open_backoff_remaining = 0
        # Last fatal fault the worker itself reported, harvested from the
        # shutdown transcript (see _capture_worker_fault). None until a worker
        # dies with something to say.
        self._worker_fault = None
        self._recovery_block = None
        self._recovery_progress = None
        self._port_release_started_at = None
        self._port_release_timeout_sec = NATIVE_READINESS_TIMEOUT_SEC if native_transport else 15.0

    # ------------------------------------------------------------------ #
    # Thread lifecycle                                                   #
    # ------------------------------------------------------------------ #
    def run(self):
        """
        Monitor, assess, and publish one stream until stop() is called.

        Output: None.
        """
        while not self._stop_event.is_set():
            checked_at = _now_rel()
            try:
                action_result = self._watch_stream()
            except Exception as error:
                # The loop must never die -- publish an error report and keep going.
                action_result = {
                    "stream_index": self.stream_index,
                    "action": "check_failed",
                    "failure_reason": "watcher_error",
                    "error": f"{type(error).__name__}: {error}",
                }
            report = self._build_verbose_stream_report(checked_at, action_result)
            self._publish(self.stream_index, report)
            # wait() returns immediately when stop() fires -> responsive shutdown.
            self._stop_event.wait(self._interval_sec)

    def stop(self):
        """
        Signal the stream watcher loop to exit.

        Output: None.
        """
        self._stop_event.set()

    def clear_recovery_state(self):
        """
        Clear watcher-local recovery state after a guarded command.

        Output: None.
        """
        self._disconnected = False
        self._recovery_progress = None
        self._recovery_block = None
        self._port_release_started_at = None
        self._failure_count = 0
        self._recovery_attempt_count = 0
        self._response_grace = None
        self._port_absent_since = None
        self._port_absent_packet_count = None
        self._physical_disconnect = False
        self._disconnect_episode_id = None
        self._disconnect_started_at_epoch = None
        self._cold_open_backoff_remaining = 0
        self._first_packet_started_at = time.monotonic()
        self._first_packet_seen = False

    def _set_recovery_progress(self, stage, *, ping_status=None, verification=None,
                               error=None, clear_error=False):
        """Retain observed recovery transitions when periodic reports coalesce."""
        previous = self._recovery_progress
        if previous is None or (stage == "detected" and previous["stage"] != "detected"):
            previous = {"stage": None, "started_at": time.time(), "ping_status": None,
                        "verification": None, "last_error": None, "history": []}
        history = list(previous["history"])
        if previous["stage"] != stage:
            history.append({"stage": stage, "at": time.time(),
                            "attempt": self._recovery_attempt_count or None})
        self._recovery_progress = {
            **previous, "stage": stage, "history": history[-16:],
            "ping_status": ping_status if ping_status is not None else previous["ping_status"],
            "verification": verification if verification is not None else previous["verification"],
            "last_error": (None if clear_error else str(error)[:500] if error
                           else previous["last_error"]),
        }

    def _publish_recovery_progress(self, stage, base, **details):
        """Publish the active operation before a hardware call can block."""
        self._set_recovery_progress(stage, **details)
        result = {**base, "ok": False, "action": "recovery_" + stage,
                  "failure_reason": "recovery_" + stage,
                  "recovery_policy": self._recovery_policy,
                  "recovery_attempt": self._recovery_attempt()}
        self._publish(self.stream_index, self._build_verbose_stream_report(_now_rel(), result))

    def _record_recovery_result(self, result, heartbeat):
        """Associate completed checks with the evidence already observed."""
        action = result.get("action")
        stages = {"unplug_detected": "settling", "filling_missing_samples": "settling",
                  "waiting_for_port": "waiting_for_device", "waiting_for_port_release": "waiting_for_port",
                  "waiting_for_heartbeat": "waiting_for_data", "port_check_failed": "presence_check_failed",
                  "restart_failed": "restart_failed", "reconnect_failed_stop_stream_completed": "retrying"}
        stage = stages.get(action)
        if action == "needs_action":
            stage = {"cleanup_failed": "cleanup_failed", "port_release_timeout": "port_access_timeout"}.get(
                result.get("initiating_failure_reason"),
                "awaiting_approval" if self._recovery_policy == "recommend" else "attempts_exhausted",
            )
        elif action == "check_failed" and self._recovery_progress is not None:
            stage = "check_failed"
        if stage is not None:
            self._set_recovery_progress(stage, error=extract_action_error(result))
        elif action == "connection_restored" and heartbeat.get("status") == "fresh":
            self._set_recovery_progress("streaming", verification="data", clear_error=True)
        elif (action == "none" and result.get("failure_reason") is None
              and heartbeat.get("status") == "fresh" and self._recovery_progress is not None):
            self._set_recovery_progress("streaming", verification="data", clear_error=True)

    def _maybe_rearm_from_needs_action(self):
        """
        Decide whether a stream paused in needs_action should resume recovery.

        Only the automate policy re-arms; recommend mode always waits for an
        explicit control-plane command.

        Automate can reach needs_action after exhausting retries against a live
        device. If the device later disappears, re-arm its automatic port-wait
        loop with a fresh budget. Recommend deliberately never takes this path:
        physical disconnects remain needs_action and expose current port
        presence to the control plane until an operator approves a restart.

        Output: bool (True | False).
        - True: recovery re-armed; the caller should resume the reconnect loop.
        - False: stay paused in needs_action.
        """
        if self._recovery_policy != "automate" or self._recovery_block is not None:
            return False

        try:
            port_present = self._monitor.is_stream_port_present(self.stream_index)
        except Exception:
            return False

        if port_present:
            return False

        # A physical disconnect begins a new recovery episode. Return control
        # to the existing port-wait loop with a fresh restart budget.
        self._recovery_attempt_count = 0
        self._set_recovery_progress("detected")
        self._response_grace = None
        self._disconnected = True
        self._monitor.set_lifecycle_state(
            self.stream_index,
            "running",
            reason="port_absent",
            requested_by="watchdog",
            command="auto_rearm",
        )
        return True

    @staticmethod
    def _is_port_lock_error(error_text):
        """Recognize temporary VCP or D2XX failures while a device reconnects."""
        if not error_text:
            return False
        text = str(error_text).lower()
        if any(
            marker in text
            for marker in (
                "permissionerror",
                "access is denied",
                "could not open port",
                "resource busy",
                "device or resource busy",
            )
        ):
            return True
        parts = [part.strip() for part in text.split(":")]
        native_error = any(
            part == "deviceerror" or part.startswith("native ftdi ")
            for part in parts[:-1]
        )
        # Match driver statuses, so unrelated I/O failures still spend the restart budget.
        return native_error and parts[-1] in {
            "io_error", "device_not_found", "device_not_opened",
        }

    # ------------------------------------------------------------------ #
    # Thread's 2 action paths: Detect & get info or recover disconnected  #
    # ------------------------------------------------------------------ #

    #Helper for per-stream logic
    def _watch_stream(self):
        """
        Inspect a connected stream or advance its reconnection state.

        Output: dict.
        - ok: bool (True | False).
          - True: the stream is healthy or reconnect verification succeeded.
          - False: a failure was detected or recovery is still in progress.
        - stream_index: int.
        - action: str
          (none | stopped_stream_waiting_for_reconnect | port_check_failed |
          waiting_for_port | waiting_for_port_release | restart_failed |
          waiting_for_heartbeat | connection_restored |
          reconnect_failed_stop_stream_failed |
          reconnect_failed_stop_stream_completed | check_failed).
          - none: no recovery action was required this pass.
          - stopped_stream_waiting_for_reconnect: a failure escalated and the
            stream worker was stopped before reconnecting.
          - port_check_failed: checking port presence raised.
          - waiting_for_port: the expected port is absent from the OS list.
          - waiting_for_port_release: the port exists but cannot be opened.
          - restart_failed: rebuilding or starting the stream failed.
          - waiting_for_heartbeat: PING failed after restart and the worker
            is awaiting fresh sample data.
          - connection_restored: restart succeeded and was verified by PING or a
            fresh replacement-worker heartbeat.
          - reconnect_failed_stop_stream_failed: post-restart PING failed and
            stopping that failed restart also raised.
          - reconnect_failed_stop_stream_completed: post-restart PING failed
            and the restarted worker was stopped successfully.
          - check_failed: the watcher caught an unexpected check error.
        - failure_reason: str or None
          (worker_dead | worker_missing | worker_unknown | heartbeat_stale |
          data_never_started | heartbeat_missing | port_check_failed |
          waiting_for_port | waiting_for_port_release | restart_failed |
          restart_completed_worker_not_response | check_failed | None).
          - worker_dead: the worker process exists but has exited.
          - worker_missing: no worker process object exists.
          - worker_unknown: the worker status could not be classified.
          - heartbeat_stale: data is older than the maximum heartbeat age.
          - data_never_started: no packet has ever reached the health sink.
          - heartbeat_missing: heartbeat data is unavailable for another
            reason.
          - port_check_failed: checking port presence raised.
          - waiting_for_port: the expected port is absent.
          - waiting_for_port_release: the port exists but cannot be opened.
          - restart_failed: rebuilding or starting the stream failed.
          - restart_completed_worker_not_response: restart completed, but the
            worker did not answer the verification PING.
          - check_failed: the connected-stream check raised unexpectedly.
          - None: no failure was detected.
        - failure_count: int, present on normal checks and reconnect attempts.
        - stream_status: dict, present when status data is available.
          - stream_index: int.
          - source_class: str or None.
          - sink_classes: list[str].
          - worker_status: str (alive | dead | missing | unknown).
            - alive: the worker process exists and is running.
            - dead: the worker process exists but has exited.
            - missing: no worker process object exists.
            - unknown: reading stream status raised.
          - worker_pid: int or None.
          - worker_exitcode: int or None.
          - error: str, present when reading stream status raises.
        - heartbeat: dict, present during connected-stream checks.
          - status: str (fresh | stale | missing).
            - fresh: data arrived within max_age_sec.
            - stale: data exists but is older than max_age_sec.
            - missing: no heartbeat data is available or the read failed.
          - last_data_at: float or None.
          - reason: str or None
            (None | data_older_than_max_age | no_health_sink_attached |
            no_data_seen_yet | heartbeat_check_failed: <error>).
            - None: heartbeat data is fresh.
            - data_older_than_max_age: heartbeat data exists but is stale.
            - no_health_sink_attached: no heartbeat-producing sink exists.
            - no_data_seen_yet: the health sink has received no packet.
            - heartbeat_check_failed: <error>: reading the heartbeat raised.
          - age_sec: float or None.
          - max_age_sec: float.
          - packet_count: int or None.
        - restart_result: dict, present after a restart attempt. Uses the
          restart_result schema documented by _try_reconnect().
        - verify_result: dict, present after restart verification. Uses the
          verify_result schema documented by _try_reconnect().
        - error: str, present when an operation raises.
        """
        try:
            lifecycle_state = self._monitor.get_lifecycle_state(self.stream_index)
            if lifecycle_state.get("state") == "stopped":
                return self._build_waiting_for_command_result(
                    action="manual_stop",
                    failure_reason="manual_stop",
                    lifecycle_state=lifecycle_state,
                )
            if lifecycle_state.get("state") == "needs_action":
                # "Hardware came back" should resume automatic recovery without a
                # manual control-plane command. Only re-arm under the automate
                # policy; recommend mode always waits for an explicit command.
                if self._maybe_rearm_from_needs_action():
                    return self._try_reconnect()
                return self._build_waiting_for_command_result(
                    action="needs_action",
                    failure_reason="needs_action",
                    lifecycle_state=lifecycle_state,
                )
            if self._disconnected:
                return self._try_reconnect() 
            return self._get_live_stream_info() 
        except Exception as error:
            return {
                "ok": False, "stream_index": self.stream_index,
                "action": "check_failed", "failure_reason": "check_failed",
                "error": f"{type(error).__name__}: {error}",
            }
            
    def _get_live_stream_info(self):
        """
        Read a connected stream and escalate failures when required.

        Output: dict.
        - stream_index: int.
        - stream_status: dict.
          - stream_index: int.
          - source_class: str.
          - sink_classes: list[str].
          - worker_status: str (alive | dead | missing).
            - alive: the worker process exists and is running.
            - dead: the worker process exists but has exited.
            - missing: no worker process object exists.
          - worker_pid: int or None.
          - worker_exitcode: int or None.
        - heartbeat: dict.
          - status: str (fresh | stale | missing).
            - fresh: data arrived within max_age_sec.
            - stale: data exists but is older than max_age_sec.
            - missing: no heartbeat data is available or the read failed.
          - last_data_at: float or None.
          - reason: str or None
            (None | data_older_than_max_age | no_health_sink_attached |
            no_data_seen_yet | heartbeat_check_failed: <error>).
            - None: heartbeat data is fresh.
            - data_older_than_max_age: heartbeat data exists but is stale.
            - no_health_sink_attached: no heartbeat-producing sink exists.
            - no_data_seen_yet: the health sink has received no packet.
            - heartbeat_check_failed: <error>: reading the heartbeat raised.
          - age_sec: float or None.
          - max_age_sec: float.
          - packet_count: int or None.
        - ok: bool (True | False).
          - True: no worker or heartbeat failure was classified.
          - False: the stream has a classified failure.
        - action: str (none | stopped_stream_waiting_for_reconnect).
          - none: the failure is absent or has not reached escalation.
          - stopped_stream_waiting_for_reconnect: the failure escalated and
            the worker was stopped before reconnecting.
        - failure_reason: str or None
          (worker_dead | worker_missing | heartbeat_stale |
          data_never_started | heartbeat_missing | None).
          - worker_dead: the worker process exists but has exited.
          - worker_missing: no worker process object exists.
          - heartbeat_stale: data is older than the maximum heartbeat age.
          - data_never_started: no packet has ever reached the health sink.
          - heartbeat_missing: heartbeat data is unavailable for another
            reason.
          - None: the worker is alive and its heartbeat is fresh.
        - failure_count: int. Consecutive failures, reset to 0 on success.
        """
        m = self._monitor
        stream_status = m.get_stream_status(self.stream_index)
        heartbeat= self._get_heartbeat()
        base = {"stream_index":self.stream_index, "stream_status": stream_status, "heartbeat":heartbeat}

        port_result = self._observe_physical_port(base)
        if port_result is not None:
            return port_result

        if heartbeat.get("status") in {"fresh", "stale"} or heartbeat.get("packet_count", 0):
            self._first_packet_seen = True
        
        #Checking if the stream worker is doing good
        failure_reason = self._classify_stream_failure(stream_status, heartbeat) 
        if failure_reason is None:
            self._failure_count = 0
            return {
                **base,
                "ok": True,
                "action": "none",
                "failure_reason": None,
                "failure_count": 0,
            }

        if failure_reason == "data_never_started" and not self._first_packet_seen:
            elapsed = time.monotonic() - self._first_packet_started_at
            remaining = max(0.0, self._first_packet_timeout_sec - elapsed)
            if remaining > 0:
                self._failure_count = 0
                return {
                    **base,
                    "ok": False,
                    "action": "none",
                    "failure_reason": "first_packet_pending",
                    "failure_count": 0,
                    "startup": {
                        "elapsed_sec": elapsed,
                        "timeout_sec": self._first_packet_timeout_sec,
                        "remaining_sec": remaining,
                    },
                }

        if (self._recovery_progress is None
                or self._recovery_progress["stage"] in {"recovery_verified", "streaming"}):
            self._set_recovery_progress("detected")
        #If the stream worker is in unknown-state, counting up for 3 times
        self._failure_count += 1
        worker_failed = failure_reason in ("worker_dead", "worker_missing")
        escalates = worker_failed or failure_reason in ("heartbeat_stale", "data_never_started")
        if not escalates or (not worker_failed and self._failure_count < self._failure_threshold):
            self._set_recovery_progress("settling")
            return {
                **base,
                "ok": False,
                "action": "none",
                "failure_reason": failure_reason,
                "failure_count": self._failure_count,
            }

        #If not from above cases, escalate immediately by stopping the stream and mark disconnect
        with m.stream_lifecycle_guard(
            self.stream_index,
            command="auto_recovery",
            requested_by="watchdog",
            blocking=False,
        ) as busy:
            if busy is not None:
                return {
                    **base,
                    "ok": False,
                    "action": "lifecycle_busy",
                    "failure_reason": "lifecycle_busy",
                    "failure_count": self._failure_count,
                    "busy": busy,
                    "recovery_policy": self._recovery_policy,
                }
            cleanup_failure = self._stop_for_recovery(base)
            if cleanup_failure is not None:
                return cleanup_failure

        if self._recovery_policy == "recommend":
            lifecycle_state = m.set_lifecycle_state(
                self.stream_index,
                "needs_action",
                reason=failure_reason,
                requested_by="watchdog",
                command="recommend_recovery",
            )
            self._disconnected = True
            return {
                **base,
                "ok": False,
                "action": "needs_action",
                "failure_reason": "needs_action",
                "initiating_failure_reason": failure_reason,
                "failure_count": self._failure_count,
                "recovery_policy": self._recovery_policy,
                "recommended_commands": ["restart_stream"],
                "lifecycle_state": lifecycle_state,
            }

        self._disconnected = True
        return {
            **base,
            "ok": False,
            "action": "stopped_stream_waiting_for_reconnect",
            "failure_reason": failure_reason,
            "failure_count": self._failure_count,
            "recovery_policy": self._recovery_policy,
        }

    def _observe_physical_port(self, base):
        """Return an immediate port-edge result using heartbeat age as the deadline."""
        try:
            port_present = self._monitor.is_stream_port_present(self.stream_index)
        except Exception:
            # Worker/heartbeat supervision remains authoritative when the OS
            # port inventory itself cannot be read.
            return None

        # Preserves OS information for down-the-stream recovery report
        hardware_present = port_present
        now = time.monotonic()
        heartbeat = base.get("heartbeat", {})
        heartbeat_age = heartbeat.get("age_sec")
        if not isinstance(heartbeat_age, (int, float)) or isinstance(heartbeat_age, bool):
            heartbeat_age = None
        worker_alive = base.get("stream_status", {}).get("worker_status") == "alive"

        if port_present:
            if self._port_absent_since is None:
                return None
            elapsed = max(
                0.0,
                heartbeat_age if heartbeat_age is not None else now - self._port_absent_since,
            )
            if not worker_alive:
                # The heartbeat window already ended and the worker closed its
                # sinks. Continue through the normal policy path; Recommend
                # still requires an explicit operator command after a physical
                # reconnect.
                self._physical_disconnect = True
                if elapsed >= self._max_heartbeat_age_sec:
                    self._disconnected = True
                    self._failure_count += 1
                    self._monitor.set_lifecycle_state(
                        self.stream_index,
                        "running",
                        reason="heartbeat_age_exceeded",
                        requested_by="watchdog",
                        command="heartbeat_age_exceeded",
                    )
                    return {
                        **base,
                        "ok": False,
                        "action": "heartbeat_age_exceeded",
                        "failure_reason": "waiting_for_port",
                        "initiating_failure_reason": "heartbeat_age_exceeded",
                        "failure_count": self._failure_count,
                        "hardware_present": hardware_present,
                        "disconnect": {
                            "elapsed_sec": elapsed,
                            "max_heartbeat_age_sec": self._max_heartbeat_age_sec,
                            "recording_continued": False,
                            "episode_id": self._disconnect_episode_id,
                            "started_at": self._disconnect_started_at_epoch,
                            "exceeded_at": time.time(),
                        },
                        "recovery_policy": self._recovery_policy,
                    }
                self._port_absent_since = None
                self._port_absent_packet_count = None
                return None
            packet_count = heartbeat.get("packet_count")
            real_data_resumed = (
                isinstance(packet_count, int)
                and not isinstance(packet_count, bool)
                and isinstance(self._port_absent_packet_count, int)
                and packet_count > self._port_absent_packet_count
            )
            if not real_data_resumed and elapsed < self._max_heartbeat_age_sec:
                return {
                    **base,
                    "ok": False,
                    "action": "filling_missing_samples",
                    "failure_reason": "port_absent_heartbeat_window",
                    "failure_count": self._failure_count,
                    "hardware_present": hardware_present,
                    "disconnect": {
                        "elapsed_sec": elapsed,
                        "remaining_sec": max(0.0, self._max_heartbeat_age_sec - elapsed),
                        "max_heartbeat_age_sec": self._max_heartbeat_age_sec,
                        "fill_value": "NaN",
                        "episode_id": self._disconnect_episode_id,
                        "started_at": self._disconnect_started_at_epoch,
                    },
                    "recovery_policy": self._recovery_policy,
                }
            if not real_data_resumed:
                # The identifier returned but the data path did not. Fall
                # through to the same heartbeat-expiry shutdown used while absent.
                port_present = False
            else:
                episode_id = self._disconnect_episode_id
                started_at = self._disconnect_started_at_epoch
                ended_at = time.time()
                self._port_absent_since = None
                self._port_absent_packet_count = None
                self._failure_count = 0
                self._physical_disconnect = False
                self._disconnect_episode_id = None
                self._disconnect_started_at_epoch = None
                return {
                    **base,
                    "ok": True,
                    "action": "connection_restored",
                    "failure_reason": None,
                    "failure_count": 0,
                    "hardware_present": hardware_present,
                    "disconnect": {
                        "elapsed_sec": elapsed,
                        "max_heartbeat_age_sec": self._max_heartbeat_age_sec,
                        "recording_continued": True,
                        "episode_id": episode_id,
                        "started_at": started_at,
                        "ended_at": ended_at,
                    },
                    "recovery_policy": self._recovery_policy,
                }

        first_observation = self._port_absent_since is None
        if first_observation:
            self._set_recovery_progress("detected")
            self._port_absent_since = now
            packet_count = heartbeat.get("packet_count")
            self._port_absent_packet_count = (
                packet_count
                if isinstance(packet_count, int) and not isinstance(packet_count, bool)
                else 0
            )
            self._physical_disconnect = True
            self._disconnect_episode_id = uuid.uuid4().hex
            self._disconnect_started_at_epoch = time.time() - max(0.0, heartbeat_age or 0.0)
        elapsed = max(
            0.0,
            heartbeat_age if heartbeat_age is not None else now - self._port_absent_since,
        )
        remaining = max(0.0, self._max_heartbeat_age_sec - elapsed)
        self._failure_count += 1

        if elapsed < self._max_heartbeat_age_sec:
            return {
                **base,
                "ok": False,
                "action": "unplug_detected" if first_observation else "filling_missing_samples",
                "failure_reason": "port_absent_heartbeat_window",
                "failure_count": self._failure_count,
                "hardware_present": hardware_present,
                "disconnect": {
                    "elapsed_sec": elapsed,
                    "remaining_sec": remaining,
                    "max_heartbeat_age_sec": self._max_heartbeat_age_sec,
                    "fill_value": "NaN",
                    "episode_id": self._disconnect_episode_id,
                    "started_at": self._disconnect_started_at_epoch,
                },
                "recovery_policy": self._recovery_policy,
            }

        with self._monitor.stream_lifecycle_guard(
            self.stream_index,
            command="heartbeat_age_exceeded",
            requested_by="watchdog",
            blocking=False,
        ) as busy:
            if busy is not None:
                return {
                    **base,
                    "ok": False,
                    "action": "heartbeat_age_exceeded",
                    "failure_reason": "waiting_for_port",
                    "initiating_failure_reason": "heartbeat_age_exceeded",
                    "failure_count": self._failure_count,
                    "busy": busy,
                    "hardware_present": hardware_present,
                    "disconnect": {
                        "elapsed_sec": elapsed,
                        "max_heartbeat_age_sec": self._max_heartbeat_age_sec,
                        "recording_continued": False,
                        "episode_id": self._disconnect_episode_id,
                        "started_at": self._disconnect_started_at_epoch,
                        "exceeded_at": time.time(),
                    },
                    "recovery_policy": self._recovery_policy,
                }
            cleanup_failure = self._stop_for_recovery(base)
            if cleanup_failure is not None:
                return cleanup_failure

        self._disconnected = True
        lifecycle_state = self._monitor.set_lifecycle_state(
            self.stream_index,
            "needs_action" if self._recovery_policy == "recommend" else "running",
            reason="heartbeat_age_exceeded",
            requested_by="watchdog",
            command=(
                "recommend_recovery"
                if self._recovery_policy == "recommend"
                else "heartbeat_age_exceeded"
            ),
        )
        if self._recovery_policy == "recommend":
            return {
                **base,
                "ok": False,
                "action": "needs_action",
                "failure_reason": "needs_action",
                "initiating_failure_reason": "heartbeat_age_exceeded",
                "failure_count": self._failure_count,
                "hardware_present": hardware_present,
                "disconnect": {
                    "elapsed_sec": elapsed,
                    "max_heartbeat_age_sec": self._max_heartbeat_age_sec,
                    "recording_continued": False,
                    "episode_id": self._disconnect_episode_id,
                    "started_at": self._disconnect_started_at_epoch,
                    "exceeded_at": time.time(),
                },
                "recovery_policy": self._recovery_policy,
                "recommended_commands": ["restart_stream"],
                "lifecycle_state": lifecycle_state,
            }
        return {
            **base,
            "ok": False,
            "action": "heartbeat_age_exceeded",
            "failure_reason": "waiting_for_port",
            "initiating_failure_reason": "heartbeat_age_exceeded",
            "failure_count": self._failure_count,
            "hardware_present": hardware_present,
            "disconnect": {
                "elapsed_sec": elapsed,
                "max_heartbeat_age_sec": self._max_heartbeat_age_sec,
                "recording_continued": False,
                "episode_id": self._disconnect_episode_id,
                "started_at": self._disconnect_started_at_epoch,
                "exceeded_at": time.time(),
            },
            "recovery_policy": self._recovery_policy,
        }
    
    def _recovery_attempt(self):
        return {
            "current": self._recovery_attempt_count,
            "max": self._max_auto_restart_attempts,
        }

    def _block_recovery(self, base, *, reason, error, **details):
        """Keep the failure visible until an explicit command clears recovery state."""
        self._recovery_block = {"error": error, **details}
        if reason == "cleanup_failed":
            self._recovery_block["recommended_commands"] = []
        return {
            **self._transition_to_needs_action(base, reason=reason),
            **self._recovery_block,
        }

    def _stop_for_recovery(self, base):
        """Stop the failed stream and require worker exit before recovery."""
        if self._recovery_progress is None:
            self._set_recovery_progress("detected")
        self._publish_recovery_progress("stopping_stream", base)
        try:
            result = self._monitor.stop_stream(self.stream_index)
        except Exception as error:
            result = {"ok": False, "error": f"{type(error).__name__}: {error}"}
        self._capture_worker_fault(result)
        if isinstance(result, dict) and result.get("worker_exited") is not True and (
            result.get("teardown_confirmed") is False
            or (result.get("ok") is False and result.get("teardown_confirmed") is not True)
        ):
            return self._block_recovery(
                base,
                reason="cleanup_failed",
                error=("Stream cleanup failed: acquisition worker exit could not be confirmed. "
                       "Automatic recovery stopped. Stop the session before trying again."),
                shutdown_result={key: result[key] for key in (
                    "ok", "teardown_confirmed", "worker_exited", "worker_exitcode",
                    "descendants_teardown_confirmed", "forced_termination", "error",
                ) if key in result},
            )
        return None

    def _wait_for_port_release(self, base, *, failure_reason="waiting_for_port_release", **details):
        """Give a held port a bounded settling window before requiring operator action."""
        now = time.monotonic()
        if self._port_release_started_at is None:
            self._port_release_started_at = now
        if now - self._port_release_started_at >= self._port_release_timeout_sec:
            return self._block_recovery(
                base,
                reason="port_release_timeout",
                error=(f"Port recovery timed out after {self._port_release_timeout_sec:g} seconds. Automatic recovery stopped. "
                       "Check the device connection and close other programs using it before retrying."),
                **details,
            )
        return {
            **base,
            "ok": False,
            "action": "waiting_for_port_release",
            "failure_reason": failure_reason,
            **details,
        }

    def _capture_worker_fault(self, shutdown_result):
        """Harvest the worker's own account of why it died, if it left one.

        Output: dict or None. The captured fault, if any.
        """
        if self._worker_fault is not None or not isinstance(shutdown_result, dict):
            return self._worker_fault

        for record in shutdown_result.get("transcript") or ():
            # ShutdownPhase/ShutdownOutcome are str enums, so these compare
            # equal whether the record carries enum members (the live path) or
            # plain strings (a transcript rebuilt from stored JSON).
            phase = getattr(record, "phase", None)
            outcome = getattr(record, "outcome", None)
            if phase != "phase_failed" and outcome != "failed":
                continue
            reason = getattr(record, "reason", None)
            error_type = getattr(record, "error_type", None)
            if reason is None and error_type is None:
                # A failure with no attached exception says nothing an exit
                # code does not; keep looking for one that does.
                continue
            fault = {
                "error_type": error_type,
                "reason": reason,
                "action": getattr(record, "action", None),
                "worker_exitcode": getattr(record, "worker_exitcode", None),
            }
            sink_id = getattr(record, "sink_id", None)
            if sink_id is not None:
                fault["sink_id"] = sink_id
            self._worker_fault = fault
            break

        return self._worker_fault

    @staticmethod
    def _worker_crashed_on_start(stream_status):
        """
        Return True when a just-restarted worker has already exited abnormally.

        A worker that is "dead" (its process object exists but is no longer
        alive) with a non-zero exit code right after a restart crashed during
        startup -- almost always on the cold serial-port open while the port was
        still settling after a stop/replug. This is distinct from a live worker
        that is merely slow to produce its first packet, which is handled by the
        heartbeat grace instead.

        Output: bool (True | False).
        """
        if not isinstance(stream_status, dict):
            return False
        if stream_status.get("worker_status") != "dead":
            return False
        return stream_status.get("worker_exitcode") != 0

    def _back_off_worker_cold_open(self, base, restart_result, verify_result,
                                   stream_status):
        """Stop a worker that failed to open the port and retry within the settling deadline."""
        self._recovery_attempt_count = max(0, self._recovery_attempt_count - 1)
        self._response_grace = None
        self._cold_open_backoff_remaining = self._cold_open_backoff_ticks
        cleanup_failure = self._stop_for_recovery(base)
        if cleanup_failure is not None:
            return cleanup_failure
        return self._wait_for_port_release(
            base,
            failure_reason="worker_cold_open_failed",
            restart_result=restart_result,
            verify_result=verify_result,
            stream_status=stream_status,
            recovery_attempt=self._recovery_attempt(),
        )

    def _response_grace_check_count(self):
        """Allow native startup enough checks to cover its readiness window."""
        checks = max(1, (self._failure_threshold + 1) // 2)
        if self._native_transport:
            checks = max(checks, math.ceil(self._first_packet_timeout_sec / self._interval_sec))
        return checks

    def _heartbeat_verify_result(self):
        grace = self._response_grace
        if grace is None:
            return None
        return {
            "ok": any(result["status"] == "fresh" for result in grace["results"]),
            "attempts_used": len(grace["results"]),
            "max_attempts": grace["max_attempts"],
            "remaining": grace["remaining"],
            "results": list(grace["results"]),
        }

    def _finish_recovery_success(self, restart_result, verify_result, heartbeat_verify=None):
        data_verified = isinstance(heartbeat_verify, dict) and heartbeat_verify.get("ok") is True
        self._set_recovery_progress(
            "recovery_verified", ping_status="passed" if verify_result.get("ok") else "failed",
            verification="data" if data_verified else "ping", clear_error=True,
        )
        if data_verified:
            self._set_recovery_progress("streaming", verification="data", clear_error=True)
        self._port_release_started_at = None
        physical_disconnect = self._physical_disconnect
        disconnect_episode_id = self._disconnect_episode_id
        disconnect_started_at = self._disconnect_started_at_epoch
        disconnect_ended_at = time.time() if physical_disconnect else None
        self._disconnected = False
        self._physical_disconnect = False
        self._port_absent_since = None
        self._port_absent_packet_count = None
        self._disconnect_episode_id = None
        self._disconnect_started_at_epoch = None
        self._failure_count = 0
        # A worker fault describes the worker that was replaced. Once recovery
        # is verified, carrying it into every healthy report makes a resolved
        # shutdown problem look like a current stream failure.
        self._worker_fault = None
        recovery_attempt = self._recovery_attempt()
        self._recovery_attempt_count = 0
        self._response_grace = None
        heartbeat_results = (
            heartbeat_verify.get("results", [])
            if isinstance(heartbeat_verify, dict)
            else []
        )
        if any(
            result.get("status") in {"fresh", "stale"}
            or result.get("packet_count", 0)
            for result in heartbeat_results
            if isinstance(result, dict)
        ):
            self._first_packet_seen = True
        elif not self._first_packet_seen:
            # A PING can verify the worker before sample data starts. Give that
            # replacement worker its own first-packet startup window.
            self._first_packet_started_at = time.monotonic()
        self._monitor.set_lifecycle_state(
            self.stream_index,
            "running",
            reason=None,
            requested_by="watchdog",
            command="auto_recovery",
        )
        result = {
            "stream_index": self.stream_index,
            "ok": True,
            "action": "connection_restored",
            "hardware_present": True,
            "failure_reason": None,
            "failure_count": 0,
            "stream_status": self._safe_get_stream_status(),
            "restart_result": restart_result,
            "verify_result": verify_result,
            "recovery_policy": self._recovery_policy,
            "recovery_attempt": recovery_attempt,
        }
        if heartbeat_verify is not None:
            result["heartbeat_verify"] = heartbeat_verify
        if physical_disconnect:
            result["disconnect"] = {
                "recording_continued": False,
                "episode_id": disconnect_episode_id,
                "started_at": disconnect_started_at,
                "ended_at": disconnect_ended_at,
                "max_heartbeat_age_sec": self._max_heartbeat_age_sec,
            }
        return result

    def _transition_to_needs_action(self, base, *, reason, restart_result=None,
                                    verify_result=None, heartbeat_verify=None,
                                    error=None):
        self._response_grace = None
        self._disconnected = True
        lifecycle_state = self._monitor.set_lifecycle_state(
            self.stream_index,
            "needs_action",
            reason=reason,
            requested_by="watchdog",
            command="auto_recovery_exhausted",
        )
        result = {
            **base,
            "ok": False,
            "action": "needs_action",
            "failure_reason": "needs_action",
            "initiating_failure_reason": reason,
            "recovery_policy": self._recovery_policy,
            "recovery_attempt": self._recovery_attempt(),
            "recommended_commands": ["restart_stream"],
            "lifecycle_state": lifecycle_state,
        }
        if restart_result is not None:
            result["restart_result"] = restart_result
        if verify_result is not None:
            result["verify_result"] = verify_result
        if heartbeat_verify is not None:
            result["heartbeat_verify"] = heartbeat_verify
        if error is not None:
            result["error"] = error
        return result

    def _advance_response_grace(self, base):
        """Check a restarted worker's data path on one normal watcher tick."""
        grace = self._response_grace
        base = {**base, "hardware_present": grace.get("port", {}).get("present")}
        heartbeat = self._get_heartbeat()
        grace["results"].append(heartbeat)
        grace["remaining"] = max(0, grace["remaining"] - 1)
        heartbeat_verify = self._heartbeat_verify_result()

        if heartbeat["status"] == "fresh":
            return self._finish_recovery_success(
                grace["restart_result"],
                grace["verify_result"],
                heartbeat_verify=heartbeat_verify,
            )

        # A worker can survive long enough to enter grace (slow Windows spawn)
        # and then crash on its cold open. Stop waiting for a heartbeat that will
        # never arrive: treat it as a settle-window retry, not a used attempt.
        crashed_status = self._safe_get_stream_status()
        if self._worker_crashed_on_start(crashed_status):
            return self._back_off_worker_cold_open(
                base, grace["restart_result"], grace["verify_result"], crashed_status
            )

        deadline = grace.get("deadline")
        waiting = time.monotonic() < deadline if deadline is not None else grace["remaining"] > 0
        if waiting:
            return {
                **base,
                "ok": False,
                "action": "waiting_for_heartbeat",
                "failure_reason": "waiting_for_heartbeat",
                "heartbeat": heartbeat,
                "restart_result": grace["restart_result"],
                "verify_result": grace["verify_result"],
                "heartbeat_verify": heartbeat_verify,
                "recovery_policy": self._recovery_policy,
                "recovery_attempt": self._recovery_attempt(),
            }

        self._response_grace = None
        cleanup_failure = self._stop_for_recovery(base)
        if cleanup_failure is not None:
            return cleanup_failure

        if self._recovery_attempt_count >= self._max_auto_restart_attempts:
            return self._transition_to_needs_action(
                base,
                reason="restart_completed_worker_not_response",
                restart_result=grace["restart_result"],
                verify_result=grace["verify_result"],
                heartbeat_verify=heartbeat_verify,
            )

        return {
            **base,
            "ok": False,
            "action": "reconnect_failed_stop_stream_completed",
            "failure_reason": "restart_completed_worker_not_response",
            "heartbeat": heartbeat,
            "restart_result": grace["restart_result"],
            "verify_result": grace["verify_result"],
            "heartbeat_verify": heartbeat_verify,
            "recovery_policy": self._recovery_policy,
            "recovery_attempt": self._recovery_attempt(),
        }

    def _try_reconnect(self):
        """
        Attempt one reconnect step for a disconnected stream.

        Output: dict.
        - stream_index: int.
        - failure_count: int.
        - stream_status: dict.
          - stream_index: int.
          - source_class: str or None.
          - sink_classes: list[str].
          - worker_status: str (alive | dead | missing | unknown).
            - alive: the worker process exists and is running.
            - dead: the worker process exists but has exited.
            - missing: no worker process object exists.
            - unknown: reading stream status raised.
          - worker_pid: int or None.
          - worker_exitcode: int or None.
          - error: str, present when reading stream status raises.
        - ok: bool (True | False).
          - True: restart completed and the worker was verified by PING or a
            fresh replacement-worker heartbeat.
          - False: reconnect failed or is waiting for the port.
        - action: str
          (port_check_failed | waiting_for_port | waiting_for_port_release |
          restart_failed | waiting_for_heartbeat | connection_restored |
          reconnect_failed_stop_stream_failed |
          reconnect_failed_stop_stream_completed).
          - port_check_failed: checking port presence raised.
          - waiting_for_port: the expected port is absent from the OS list.
          - waiting_for_port_release: the port exists but cannot be opened.
          - restart_failed: rebuilding or starting the stream failed.
          - waiting_for_heartbeat: PING failed after restart and the worker
            is awaiting fresh sample data.
          - connection_restored: restart succeeded and was verified by PING or a
            fresh replacement-worker heartbeat.
          - reconnect_failed_stop_stream_failed: post-restart PING failed and
            stopping that failed restart also raised.
          - reconnect_failed_stop_stream_completed: post-restart PING failed
            and the restarted worker was stopped successfully.
        - failure_reason: str or None
          (port_check_failed | waiting_for_port | waiting_for_port_release |
          restart_failed | restart_completed_worker_not_response | None).
          - port_check_failed: checking port presence raised.
          - waiting_for_port: the expected port is absent.
          - waiting_for_port_release: the port exists but cannot be opened.
          - restart_failed: rebuilding or starting the stream failed.
          - restart_completed_worker_not_response: restart completed, but the
            worker did not answer the verification PING.
          - None: reconnect and verification succeeded.
        - restart_result: dict, present after a restart attempt.
          - ok: bool (True | False).
            - True: the stream was rebuilt and its worker was started.
            - False: restart raised before completion.
          - stream_index: int.
          - worker_status: str (alive), present on success.
          - worker_pid: int, present on success.
          - source_class: str, present on success.
          - sink_classes: list[str], present on success.
          - reset_result: dict, present on success.
            - ok: bool (True | False).
              - True: reset completed.
              - False: opening or resetting the device failed.
            - drained_packets: int, present on reset success.
            - device_quiet: bool (True | False), present on reset success.
              - True: the device stopped producing packets during reset.
              - False: the reset deadline ended before the device became quiet.
            - error: str, present on reset failure.
          - error: str, present on failure.
          - dataflow_status: str (restart_failed), present on failure.
        - verify_result: dict, present after restart verification.
          - ok: bool (True | False).
            - True: the control command completed and returned a response.
            - False: validation, worker state, timeout, or I/O failed.
          - poll_status: str
            (queue_roundtrip_ok | direct_roundtrip_ok |
            queue_roundtrip_timeout | direct_roundtrip_timeout |
            roundtrip_error | worker_dead | worker_missing | detached |
            invalid_stream_index).
            - queue_roundtrip_ok: the worker-owned queue path succeeded.
            - direct_roundtrip_ok: the main-process direct path succeeded.
            - queue_roundtrip_timeout: the worker-owned queue path timed out.
            - direct_roundtrip_timeout: the direct path timed out.
            - roundtrip_error: the attempted command raised a non-timeout
              exception.
            - worker_dead: the worker process exists but has exited.
            - worker_missing: no worker process object exists.
            - detached: no DataFlow is attached.
            - invalid_stream_index: stream_index is outside the snapshot.
          - stream_index: int.
          - source_class: str, when stream status is available.
          - sink_classes: list[str], when stream status is available.
          - worker_status: str (alive | dead | missing), when available.
            - alive: the worker process exists and is running.
            - dead: the worker process exists but has exited.
            - missing: no worker process object exists.
          - worker_pid: int or None, when stream status is available.
          - worker_exitcode: int or None, when stream status is available.
          - routed_through_queue: bool (True | False), when attempted.
            - True: the worker owns the port, so the command uses its queue.
            - False: the main process owns the port, so the command is direct.
          - port_open_in_main: bool (True | False), when attempted.
            - True: the main-process source object has an open port.
            - False: the main-process source object does not have an open port.
          - elapsed_sec: float, present on success.
          - response_type: str, present on success.
          - response_command_number: int or None, present on success.
          - error: str, present on failure.
        - error: str, present when port checking or stream stopping raises.
        """
        m = self._monitor
        base = {
            "stream_index": self.stream_index,
            "failure_count": self._failure_count,
            "stream_status": self._safe_get_stream_status(),
            "recovery_policy": self._recovery_policy,
            "hardware_present": None,
        }

        lifecycle_state = m.get_lifecycle_state(self.stream_index)
        if (
            self._recovery_policy == "recommend"
            or lifecycle_state.get("state") == "needs_action"
        ):
            initiating_failure_reason = (
                lifecycle_state.get("reason") or "waiting_for_explicit_command"
            )
            if lifecycle_state.get("state") != "needs_action":
                lifecycle_state = m.set_lifecycle_state(
                    self.stream_index,
                    "needs_action",
                    reason="waiting_for_explicit_command",
                    requested_by="watchdog",
                    command="recommend_recovery",
                )
            return {
                **base,
                "ok": False,
                "action": "needs_action",
                "failure_reason": "needs_action",
                "initiating_failure_reason": initiating_failure_reason,
                "recommended_commands": ["restart_stream"],
                "lifecycle_state": lifecycle_state,
                **(self._recovery_block or {}),
            }

        with m.stream_lifecycle_guard(
            self.stream_index,
            command="auto_recovery",
            requested_by="watchdog",
            blocking=False,
        ) as busy:
            if busy is not None:
                return {
                    **base,
                    "ok": False,
                    "action": "lifecycle_busy",
                    "failure_reason": "lifecycle_busy",
                    "busy": busy,
                }

            if self._response_grace is not None:
                return self._advance_response_grace(base)

            if self._recovery_progress is None:
                self._set_recovery_progress("detected")
            self._publish_recovery_progress("checking_presence", base)
            try:
                port_present = m.is_stream_port_present(self.stream_index)
            except Exception as error:
                return {
                    **base,
                    "ok": False,
                    "action": "port_check_failed",
                    "failure_reason": "port_check_failed",
                    "error": f"{type(error).__name__}: {error}",
                }
            base["hardware_present"] = port_present
            if not port_present:
                self._port_release_started_at = None
                # A new unplug/replug episode gets a fresh restart budget.
                self._recovery_attempt_count = 0
                return {
                    **base,
                    "ok": False,
                    "action": "waiting_for_port",
                    "failure_reason": "waiting_for_port",
                }
            # An unplug clears the access deadline before it can block recovery.
            if (self._port_release_started_at is not None
                    and time.monotonic() - self._port_release_started_at >= self._port_release_timeout_sec):
                return self._wait_for_port_release(base)
            self._publish_recovery_progress("checking_port", base)
            if not m.is_stream_port_openable(self.stream_index):
                return self._wait_for_port_release(base)
            port_present = True
            base["hardware_present"] = True
            # Cooling down after a worker crashed on its cold open: don't respawn
            # a doomed worker every tick while the device finishes settling. The
            # port is still probed above, so we keep polling and come back on our
            # own the moment a spawned worker can actually hold the port.
            if self._cold_open_backoff_remaining > 0:
                self._cold_open_backoff_remaining -= 1
                return {
                    **base,
                    "ok": False,
                    "action": "waiting_for_port_release",
                    "failure_reason": "worker_cold_open_failed",
                    "recovery_attempt": self._recovery_attempt(),
                }
            self._recovery_attempt_count += 1
            self._publish_recovery_progress("restarting_stream", base)
            restart_result = m.restart_one_stream(self.stream_index)
            if not restart_result["ok"]: #If restart status is bad
                # A device can appear before VCP or D2XX can use it. Wait for
                # the next watchdog tick without spending the restart budget.
                if self._is_port_lock_error(restart_result.get("error")):
                    self._recovery_attempt_count -= 1
                    return self._wait_for_port_release(base, restart_result=restart_result)
                if self._recovery_attempt_count >= self._max_auto_restart_attempts:
                    return self._transition_to_needs_action(
                        base,
                        reason="restart_failed",
                        restart_result=restart_result,
                    )
                return {
                    **base,
                    "ok": False,
                    "action": "restart_failed",
                    "failure_reason": "restart_failed",
                    "restart_result": restart_result,
                    "recovery_attempt": self._recovery_attempt(),
                }

            # Native opening can outlast PING while the worker is still starting.
            verification_started_at = time.monotonic()
            self._publish_recovery_progress("waiting_for_ping", base, ping_status="pending")
            verify_result = m.poll_stream_queue_roundtrip(
                stream_index=self.stream_index,
                cmd="PING",
                timeout_sec=self._timeout_sec,
            )
            self._set_recovery_progress(
                "recovery_verified" if verify_result["ok"] else "waiting_for_data",
                ping_status="passed" if verify_result["ok"] else "failed",
                verification="ping" if verify_result["ok"] else None,
                error=verify_result.get("error"), clear_error=bool(verify_result["ok"]),
            )
            if verify_result["ok"]:#If the worker responsed
                return self._finish_recovery_success(
                    restart_result,
                    verify_result,
                )

            # restart_one_stream reports ok the instant worker.start() returns --
            # it does NOT prove the worker survived opening the port. If the fresh
            # worker has already died (crashed on its cold open during the port /
            # replug settle window), that is "device not ready yet", not a used
            # recovery attempt. Refund the budget and keep polling instead of
            # entering the heartbeat grace and draining toward needs_action.
            crashed_status = self._safe_get_stream_status()
            if self._worker_crashed_on_start(crashed_status):
                return self._back_off_worker_cold_open(
                    base, restart_result, verify_result, crashed_status
                )

            response_grace_checks = self._response_grace_check_count()
            self._response_grace = {
                "restart_result": restart_result,
                "verify_result": verify_result,
                "max_attempts": response_grace_checks,
                "remaining": response_grace_checks,
                "results": [],
                "port": {"present": port_present, "openable": True},
            }
            if self._native_transport:
                self._response_grace["deadline"] = (
                    verification_started_at + self._first_packet_timeout_sec
                )
            return {
                **base,
                "ok": False,
                "action": "waiting_for_heartbeat",
                "failure_reason": "waiting_for_heartbeat",
                "restart_result": restart_result,
                "verify_result": verify_result,
                "heartbeat_verify": self._heartbeat_verify_result(),
                "recovery_attempt": self._recovery_attempt(),
                "port": self._response_grace["port"],
                "recovery_policy": self._recovery_policy,
            }

    def _build_waiting_for_command_result(self, *, action, failure_reason, lifecycle_state):
        try:
            hardware_present = self._monitor.is_stream_port_present(self.stream_index)
        except Exception:
            hardware_present = None
        result = {
            "stream_index": self.stream_index,
            "ok": False,
            "action": action,
            "failure_reason": failure_reason,
            "failure_count": self._failure_count,
            "stream_status": self._safe_get_stream_status(),
            "heartbeat": self._get_heartbeat(),
            "recovery_policy": self._recovery_policy,
            "recommended_commands": ["restart_stream"],
            "hardware_present": hardware_present,
            "lifecycle_state": lifecycle_state,
        }
        if action == "needs_action":
            result["initiating_failure_reason"] = lifecycle_state.get("reason")
            result.update(self._recovery_block or {})
        return result
    
    # ------------------------------------------------------------------ #
    # Signal readers (need self._monitor -> instance methods)            #
    # ------------------------------------------------------------------ #

    def _get_heartbeat(self):
        """
        Read and normalize the stream heartbeat without raising.

        Output: dict.
        - status: str (fresh | stale | missing).
          - fresh: data arrived within max_age_sec.
          - stale: data exists but is older than max_age_sec.
          - missing: no heartbeat data is available or the read failed.
        - last_data_at: float or None. Seconds since program start.
        - reason: str or None
          (None | data_older_than_max_age | no_health_sink_attached |
          no_data_seen_yet | heartbeat_check_failed: <error>).
          - None: heartbeat data is fresh.
          - data_older_than_max_age: heartbeat data exists but is stale.
          - no_health_sink_attached: the stream has no heartbeat-producing
            health sink.
          - no_data_seen_yet: a health sink exists but has received no packet.
          - heartbeat_check_failed: <error>: reading the heartbeat raised.
        - age_sec: float or None.
        - max_age_sec: float.
        - packet_count: int or None.
        - measured_sample_rate: float or None. Observed samples/sec per channel
          over the sink's last completed rate window. None until the first
          window closes. Reported as-is with no threshold or comparison against
          the configured rate; only meaningful while status is "fresh", since a
          stalled stream keeps reporting its final window.
        """
        try:
            hb = self._monitor.get_stream_heartbeat(self.stream_index, self._max_heartbeat_age_sec)
            return {
                "status": hb.get("state"),
                "last_data_at": _to_rel(hb.get("last_data_at")),   # _to_rel is already module-level
                "last_data_at_epoch": hb.get("last_data_at"),
                "reason": hb.get("reason"),
                "age_sec": hb.get("age_sec"),
                "max_age_sec": hb.get("max_age_sec"),
                "packet_count": hb.get("packet_count"),
                "measured_sample_rate": hb.get("measured_sample_rate"),
            }
        except Exception as error:
            return {
                "status": "missing",
                "reason": f"heartbeat_check_failed: {type(error).__name__}: {error}",
                "last_data_at": None, "age_sec": None,
                "max_age_sec": self._max_heartbeat_age_sec, "packet_count": None,
                "measured_sample_rate": None,
            }
    
    def _safe_get_stream_status(self):
        """
        Read stream process metadata and return a fallback on failure.

        Output: dict.
        - stream_index: int.
        - source_class: str or None.
        - sink_classes: list[str].
        - worker_status: str (alive | dead | missing | unknown).
          - alive: the worker process exists and is running.
          - dead: the worker process exists but has exited.
          - missing: no worker process object exists.
          - unknown: reading stream status raised.
        - worker_pid: int or None.
        - worker_exitcode: int or None.
        - error: str, present only when the status read raises.
        """
        try:
            return self._monitor.get_stream_status(self.stream_index)
        except Exception as error:
            return {
                "stream_index": self.stream_index, "source_class": None,
                "sink_classes": [], "worker_status": "unknown",
                "worker_pid": None, "worker_exitcode": None,
                "error": f"{type(error).__name__}: {error}",
            }

    def _safe_get_stream_port(self):
        """
        Resolve the stream's serial port without raising.

        Output: str or None. Port identifier, or None when unavailable.
        """
        try:
            return self._monitor.get_stream_port(self.stream_index)
        except Exception:
            return None     

    def _safe_get_port_owner(self):
        """
        Resolve which process owns the stream port without raising.

        Output: str or None (main | worker | none | None).
        - main: the main-process source object has the port open.
        - worker: the live stream worker owns the port.
        - none: neither the main process nor a worker owns the port.
        - None: ownership lookup raised.
        """
        try:
            return self._monitor.get_port_owner(self.stream_index)
        except Exception:
            return None  
 
    # ------------------------------------------------------------------ #
    # Verdict + report building                                          #
    # ------------------------------------------------------------------ #    

    def _classify_stream_failure(self,stream_status, heartbeat):
        """
        Reduce worker and heartbeat signals to one failure reason.

        Output: str or None
        (worker_dead | worker_missing | worker_unknown | heartbeat_stale |
        data_never_started | heartbeat_missing | None).
        - worker_dead: the worker process exists but has exited.
        - worker_missing: no worker process object exists.
        - worker_unknown: the worker status could not be classified.
        - heartbeat_stale: data is older than the maximum heartbeat age.
        - data_never_started: no packet has ever reached the health sink.
        - heartbeat_missing: heartbeat data is unavailable for another reason.
        - None: the worker is alive and its heartbeat is fresh.
        """
        if stream_status.get("worker_status") != "alive":
            return f"worker_{stream_status.get('worker_status')}"      
        hb = heartbeat.get("status")
        if hb == "stale":
            return "heartbeat_stale"  
        if  hb == "missing":
            if heartbeat.get("reason") == "no_data_seen_yet":
                return "data_never_started"
            return "heartbeat_missing"                           
        return None    
    
    def _build_verbose_stream_report(self, checked_at, action_result):
        """
        Build the complete public report for one stream check.

        Output: dict.
        - stream_index: int.
        - source_class: str or None.
        - sink_classes: list[str].
        - stream_health: str (healthy | suspect | unhealthy).
          - healthy: worker and heartbeat signals are normal.
          - suspect: a transient failure or reconnect retry is in progress.
          - unhealthy: a definite failure or failure threshold was reached.
        - summary: str.
        - port: str or None.
        - port_owner: str or None (main | worker | none | None).
          - main: the main-process source object has the port open.
          - worker: the live stream worker owns the port.
          - none: neither the main process nor a worker owns the port.
          - None: ownership could not be determined.
        - checked_at: float. Seconds since program start.
        - rule: str
          (worker_alive_heartbeat_fresh | stream_reconnecting |
          worker_not_alive |
          worker_alive_heartbeat_stale_below_threshold |
          worker_alive_heartbeat_stale_threshold_reached |
          no_data_below_threshold | no_data_threshold_reached |
          heartbeat_missing | action_failure).
          - worker_alive_heartbeat_fresh: worker and heartbeat are healthy.
          - stream_reconnecting: a reconnect retry is in progress.
          - worker_not_alive: the worker is dead, missing, or unknown outside
            an expected reconnect state.
          - worker_alive_heartbeat_stale_below_threshold: heartbeat is stale,
            but the consecutive-failure threshold has not been reached.
          - worker_alive_heartbeat_stale_threshold_reached: heartbeat is stale
            and the failure threshold has been reached.
          - no_data_below_threshold: no packet has ever arrived, but the
            failure threshold has not been reached.
          - no_data_threshold_reached: no packet has ever arrived and the
            failure threshold has been reached.
          - heartbeat_missing: heartbeat data is unavailable for a reason
            other than data never starting.
          - action_failure: fallback for a failure not matched above.
        - action: dict.
          - taken: str
            (none | check_failed | stopped_stream_waiting_for_reconnect |
            port_check_failed | waiting_for_port | waiting_for_port_release |
            restart_failed | waiting_for_heartbeat | connection_restored |
            reconnect_failed_stop_stream_failed |
            reconnect_failed_stop_stream_completed).
            - none: the check required no recovery action.
            - check_failed: the watcher caught an unexpected check error.
            - stopped_stream_waiting_for_reconnect: a failure escalated and
              the worker was stopped before reconnecting.
            - port_check_failed: checking port presence raised.
            - waiting_for_port: the expected port is absent from the OS list.
            - waiting_for_port_release: the port exists but cannot be opened.
            - restart_failed: rebuilding or starting the stream failed.
            - waiting_for_heartbeat: PING failed after restart and the worker
              is awaiting fresh sample data.
            - connection_restored: restart succeeded and was verified by PING or a
              fresh replacement-worker heartbeat.
            - reconnect_failed_stop_stream_failed: post-restart PING failed
              and stopping that failed restart also raised.
            - reconnect_failed_stop_stream_completed: post-restart PING failed
              and the restarted worker was stopped successfully.
          - detail: dict or None. Non-duplicated action-result fields.
        - signals: dict.
          - worker: dict.
            - status: str or None (alive | dead | missing | unknown | None).
              - alive: the worker process exists and is running.
              - dead: the worker process exists but has exited.
              - missing: no worker process object exists.
              - unknown: reading stream status raised.
              - None: no worker status is available.
            - pid: int or None.
            - exitcode: int or None.
            - fault: dict or None. The exception the worker died on, when it
              reported one before exiting; None when it died silently or is
              still alive.
              - error_type: str or None. Exception class name.
              - reason: str or None. Exception text.
              - action: str or None. Worker step that raised.
              - worker_exitcode: int or None.
          - heartbeat: dict.
            - status: str (fresh | stale | missing).
              - fresh: data arrived within max_age_sec.
              - stale: data exists but is older than max_age_sec.
              - missing: no heartbeat data is available or the read failed.
            - last_data_at: float or None.
            - reason: str or None
              (None | data_older_than_max_age | no_health_sink_attached |
              no_data_seen_yet | heartbeat_check_failed: <error>).
              - None: heartbeat data is fresh.
              - data_older_than_max_age: heartbeat data exists but is stale.
              - no_health_sink_attached: no heartbeat-producing sink exists.
              - no_data_seen_yet: the health sink has received no packet.
              - heartbeat_check_failed: <error>: reading the heartbeat raised.
            - age_sec: float or None.
            - max_age_sec: float.
            - packet_count: int or None.
          - failure: dict.
            - count: int.
            - threshold: int.
            - last_error: str or None.
            - last_error_at: float or None.
        """
        # Reuse the status already gathered by the recovery step when possible.
        stream_status = action_result.get("stream_status")
        if not isinstance(stream_status, dict) or "worker_status" not in stream_status:
            stream_status = self._safe_get_stream_status()
        worker = {
            "status": stream_status.get("worker_status"),
            "pid": stream_status.get("worker_pid"),
            "exitcode": stream_status.get("worker_exitcode"),
            # The exception the worker died on, when it managed to report one.
            # An exit code alone cannot distinguish a config fault from a
            # device fault, and the recovery machinery downstream will happily
            # describe a doomed stream in terms of the port it cannot reopen.
            "fault": self._worker_fault,
        }
        heartbeat = action_result.get("heartbeat") or self._get_heartbeat()
        self._record_recovery_result(action_result, heartbeat)
        failure_reason = action_result.get("failure_reason")
        initiating_failure_reason = action_result.get("initiating_failure_reason")
        startup = action_result.get("startup")
        failure_count = action_result.get("failure_count", 0)
        error = extract_action_error(action_result)

        signals = {
            "worker": worker,
            "heartbeat": heartbeat,
            "failure": {
                "count": failure_count,
                "threshold": self._failure_threshold,
                "last_error": error,
                "last_error_at": checked_at if error is not None else None,
            },
        }
        if isinstance(startup, dict):
            signals["startup"] = startup

        assessment = assess_stream(
            worker=worker,
            heartbeat=heartbeat,
            failure_reason=failure_reason,
            initiating_failure_reason=initiating_failure_reason,
            startup=startup,
            count=failure_count,
            threshold=self._failure_threshold,
            max_heartbeat_age_sec=self._max_heartbeat_age_sec,
        )
        if action_result.get("action") == "connection_restored":
            assessment = {
                "stream_health": "healthy",
                "rule": "connection_restored",
                "summary": "Connection restored; recording real samples again.",
            }
            if heartbeat.get("status") != "fresh":
                assessment["summary"] = "Worker responded to PING; waiting for real samples."

        if action_result.get("initiating_failure_reason") in {"cleanup_failed", "port_release_timeout"}:
            assessment["summary"] = action_result.get("error") or assessment["summary"]

        report = {
            "stream_index": self.stream_index,
            "source_class": stream_status.get("source_class"),
            "sink_classes": stream_status.get("sink_classes", []),
            "stream_health": assessment["stream_health"],
            "summary": assessment["summary"],
            "port": self._safe_get_stream_port(),
            "port_owner": self._safe_get_port_owner(),
            "checked_at": checked_at,
            "rule": assessment["rule"],
            "failure_reason": failure_reason,
            "initiating_failure_reason": initiating_failure_reason,
            "action": build_action_signal(action_result),
            "signals": signals,
            "recovery_progress": self._recovery_progress,
        }
        report["recovery_event"] = build_recovery_event(report, action_result)
        return report
    
class DeviceWatcher(threading.Thread):
    """Independent watchdog thread for one standalone (non-DataFlow) device."""
    @staticmethod
    def _device_key(device):
        """
        Build a stable display key from the device's available identity fields.

        Output: str. The first non-empty port, device_name, or _name value;
        otherwise a process-local "device-<object ID>" fallback.
        """
        for attr in ("port", "device_name", "_name"):
            v = getattr(device, attr, None)
            if v:
                return str(v)
        return f"device-{id(device)}"
    
    def __init__(self, device, *, failure_threshold, interval_sec, timeout_sec,
                 publish):
        """
        Initialize a daemon watcher for one standalone hardware device.

        Output: None.
        """
        self._key = self._device_key(device)
        super().__init__(name=f"device-watcher-{self._key}", daemon=True)
        self._hw = HardwareMonitor(failure_threshold=failure_threshold)
        self._hw.watch(device)                 # this thread owns just this device
        self._interval_sec = interval_sec
        self._timeout_sec = timeout_sec
        self._publish = publish
        self._stop_event = threading.Event()

    @property
    def key(self):
        """
        Return the stable key used to publish this device's reports.

        Output: str. Device key selected by _device_key().
        """
        return self._key

    def stop(self):
        """
        Signal the device watcher loop to exit.

        Output: None.
        """
        self._stop_event.set()

    def run(self):
        """
        Poll and publish standalone-device health until stop() is called.

        Output: None.

        Published reports are dicts with:
        - status: str
          (connected | reconnected | ping failed | disconnected | suspect |
          unknown).
          - connected: the existing device handle answered PING.
          - reconnected: the existing handle failed, but a rebuilt device
            answered PING in the same poll.
          - ping failed: PING and reconnect failed, but the failure count is
            still below the disconnection threshold.
          - disconnected: the consecutive-failure threshold was reached.
          - suspect: the device watcher itself caught an exception.
          - unknown: a completed poll returned no device state.
        - consecutive_failures: int, present after a normal hardware poll.
        - last_error: str or None, present after a normal hardware poll.
        - error: str, present when polling raises.
        - checked_at: float. Seconds since program start.
        """
        while not self._stop_event.is_set():
            try:
                states = self._hw.poll_device_health_once(self._timeout_sec)
                report = next(iter(states.values()), {"status": "unknown"})
            except Exception as error:
                report = {"status": "suspect",
                          "error": f"{type(error).__name__}: {error}"}
            report = {**report, "checked_at": _now_rel()}
            self._publish(self._key, report)
            self._stop_event.wait(self._interval_sec)
