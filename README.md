# Morelia

<!DOCTYPE html>
<html lang="en">
   <div style="text-align: center;">
      <img src="docs/legacy/Logos/Rings.png" alt="logo" width="160"/></center>
   </div>
</html>


## Introduction 

![GitHub license](https://img.shields.io/github/license/Pinnacle-Technology-Inc/Python-POD-API)
![GitHub repo size](https://img.shields.io/github/repo-size/Pinnacle-Technology-Inc/Python-POD-API)
![GitHub issues](https://img.shields.io/github/issues-raw/Pinnacle-Technology-Inc/Python-POD-API)
![GitHub pull requests](https://img.shields.io/github/issues-pr-raw/Pinnacle-Technology-Inc/Python-POD-API)
![GitHub last commit](https://img.shields.io/github/last-commit/Pinnacle-Technology-Inc/Python-POD-API)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/Pinnacle-Technology-Inc/Python-POD-API)

Morelia is a free, open-source Python application programming interface (API) for Pinnacle Technology, Inc. data acquisition POD devices. Morelia core modules, usage examples, and supporting documentation can be found here on GitHub and are available freely under the New BSD License. 

Currently, the API supports 8206 (legacy 4100), 8206-HR, 8401-HR, 8229, 8480-SC, and 8274-D POD devices. In the future, we will offer support to other Pinnacle devices.

### Legacy 8206 acquisition and Qt plotting

Use `from Morelia.Devices import Pod8206` for the original device (TYPE 1).
`Pod8206HR` is for TYPE 48. To try the live three-channel Qt plot from this checkout:

```bash
pip install -e ".[plot]"
python examples/device_examples/8206_scripts/8206_plot_stream.py --com-port COM9 --sample-rate 400 --preamp-gain 100
```

For direct FTDI USB, also install `.[d2xx]` and replace `--com-port COM9` with
`--device SERIAL` (or a device index). Close the plot window to stop acquisition.
The default baud rate is 9600; use `--baudrate` if the connection requires another rate.

The legacy sample rates are 200, 400, 600, 800, 1000, and 2000 Hz. Set
`pod.sample_rate` while stopped; this preserves the existing filters and gains.
`pod.read_configuration()` returns `(eeg_lowpass, eeg_gain_index, emg_lowpass,
emg_gain_index, sample_rate)`, and `pod.configure(...)` writes those five values.
Lowpass values use hundredths of Hz. Gain indices 0–7 select multipliers
1, 2, 4, 5, 8, 10, 16, and 20. The preamp gain must match the attached hardware.

`DataPacket8206.ch0`, `.ch1`, and `.ch2` contain tuples of samples in microvolts;
`.sample_count` gives the batch length. The reader handles variable-length
BINARY packets and separate TTL control events. `PlotSink` supports these
batches with per-sample timing; other device-specific sinks have not been
extended for the legacy 8206.

You can find extensive documentation for the package [here](https://pinnacle-technology-inc.github.io/Morelia).

## Installation

Create and activate a virtual environment, then install with pip:

- **Windows:** `python -m venv venv` then `venv\Scripts\activate`
- **Linux / WSL:** `python3 -m venv venv` then `source venv/bin/activate`

Then run:

```bash
pip install ptech-morelia
```

`ptech-morelia` depends on **`pypvfs`** (Pinnacle PVFS bindings). Until **`pypvfs` 0.1.0** (or newer) is published on PyPI with the full `pvfs_tools` implementation, install it from a local checkout first:

```bash
pip install -e /path/to/pypvfs
pip install ptech-morelia
```

To install from a local clone: `pip install .` (or `pip install .[plot]` to include plotting support)

### Development install (editable mode)

If you are modifying the source or building a derivative application, use an **editable install**:

```bash
pip install -e .
pip install -e .[plot]        # with plotting support
```

A normal `pip install .` copies your source files into the virtual environment's `site-packages` directory. Python imports from that copy, so any edits you make to the source are not visible until you run `pip install .` again. The `-e` flag (short for `--editable`) skips the copy and links `site-packages` back to your working directory instead. Python then imports directly from your source tree, so edits take effect the next time you import the module with no additional steps.

If you are using conda for the Qt stack on ARM or other platforms, install Morelia in editable mode after the conda dependencies:

```bash
conda install -c conda-forge pyqtgraph pyqt numpy
pip install -e .
```

### Live plotting (optional)

Live EEG-style plotting requires Qt libraries that are not included in the base install. To add plotting support:

```bash
pip install ptech-morelia[plot]
```

This installs `pyqtgraph` and `PyQt6`.

| Platform | Install command | Notes |
|---|---|---|
| Windows / macOS | `pip install ptech-morelia[plot]` | Works out of the box |
| Ubuntu / Debian / WSL | `bash install_ubuntu.sh && pip install ptech-morelia[plot]` | System libraries needed first |
| ARM Linux (RPi, etc.) | See conda instructions below | pip may fail to build Qt |

**Ubuntu / Debian / WSL:** Run `install_ubuntu.sh` before installing the plot extra to get the required system libraries. If the plotting examples (e.g. `8206_plot_stream.py`) still fault after that, install the xcb libraries:

```bash
sudo apt install libxcb-xinerama0 libxcb-cursor0 libxkbcommon-x11-0 libxcb-icccm4 libxcb-image0 libxcb-keysyms1 libxcb-randr0 libxcb-render-util0 libxcb-shape0
```

With WSLg (WSL 2), the plot window will appear on your Windows desktop. Older versions of WSL do not support graphics — in that case, use the non-plot examples.

**ARM Linux or other platforms where pip cannot build Qt:** Install [Miniforge](https://github.com/conda-forge/miniforge) (a lightweight conda distribution), then use conda for the Qt stack and pip for Morelia:

```bash
conda init bash && source ~/.bashrc   # only needed once after installing conda
conda create -n morelia python=3.11
conda activate morelia
conda install -c conda-forge pyqtgraph pyqt numpy
pip install ptech-morelia
```

All non-plotting features (streaming, file recording, data conversion, etc.) work without the plot extra installed.

### Sink performance and the GIL

Morelia’s streaming pipeline uses `DataFlow` workers implemented as separate Python processes. Each worker has its own Python Global Interpreter Lock (GIL), but *within a worker* all sinks and RxPy threads share a single GIL.

- **Default sinks (in-process)**: Sinks like `PvfsSink(output_path, pod)` run entirely inside the worker process. This is simple and efficient when a sink runs alone (e.g. `8206_pvfs_stream.py`).
- **Threaded sinks (`observe_on_scheduler`)**: Some sinks can request `observe_on_scheduler="thread_pool"` so their `flush()` runs on a thread-pool thread instead of the emission thread. This helps when a sink occasionally does extra Python work, but still shares the same GIL as other sinks in that worker.
- **Writer-process sinks (`use_writer_process=True`)**: `PvfsSink` can offload all PVFS I/O into a dedicated child process:

  ```python
  from Morelia.Stream.sink import PvfsSink

  # Record only (single sink)
  pvfs_sink = PvfsSink(output_path, pod)

  # Plot + record at high sample rates – keeps PlotSink responsive
  pvfs_sink = PvfsSink(output_path, pod, use_writer_process=True)
  ```

  In this mode, `PvfsSink.flush()` in the worker is very lightweight (it just enqueues values to a multiprocessing queue), and a separate writer process handles all PVFS disk I/O under its own GIL. This is recommended for “plot + record” examples like `8401_plot_and_save_stream.py` at high sample rates.

For “record only” tools (such as `8206_pvfs_stream.py`), the default in-process `PvfsSink` is usually preferred: there is no competing sink to starve, and the simpler single-process path is both easier to debug and slightly more efficient.

### PVFS native libraries

PVFS recording uses the **`pypvfs`** package (Pinnacle PVFS file support). It is installed automatically with `pip install ptech-morelia` and provides native libraries for Windows and Linux (including WSL) when using published wheels.

If you are developing or building from source on Linux/WSL and the bundled `.so` files are incompatible, rebuild them from the **[pypvfs](https://pypi.org/project/pypvfs/)** repository:

```bash
git clone https://github.com/Pinnacle-Technology-Inc/pypvfs.git
cd pypvfs/src/pvfs_tools/Core
./build_linux.sh
pip install -e ../../..
```

Requires `cmake` and a C++17 compiler (e.g. `g++`). Use `pip install -e path/to/pypvfs` in the same environment as Morelia until a matching **pypvfs** release is on PyPI.

### Standalone PVFS tools (EDF / WebM / CLI)

PVFS-only utilities (EDF converter, WebM exporter, synthetic file CLI) live in the **pypvfs** repo under `examples/`. See the [pypvfs README](https://pypi.org/project/pypvfs/).

