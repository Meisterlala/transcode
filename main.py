#!/usr/bin/env python3

import json
import os
import random
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from prometheus_client import Enum, Gauge, Info, start_http_server

# Load environment variables from .env file if present
_ = load_dotenv()

# Directory to monitor for input files
INPUT_DIR = os.environ.get("INPUT_DIR", "in_test")
# URL to Jellyfin server
JELLYFIN_URL = os.environ.get("JELLYFIN_URL", "http://jellyfin:8096")
# Jellyfin API key
JELLYFIN_API = os.environ.get("JELLYFIN_API", "")
# Prometheus metrics port
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9100"))
# Render device for VAAPI
VAAPI_RENDER_DEVICE = os.environ.get("VAAPI_RENDER_DEVICE", "/dev/dri/renderD128")
DB_PATH = Path(os.environ.get("DB_PATH", "tracker.sqlite3")).expanduser()
TEXT_BASED_SUBTITLE_CODECS = {
    "subrip",
    "srt",
    "webvtt",
    "ass",
    "ssa",
    "text",
}
SKIP_REASON_TEXT = "text_subtitles_present"
SCAN_REASON_TRANSCODE = "requires_transcode"

ENDING = " - Transcoded"
ENDING_ORG = " - Original"
TARGET_FROMAT = "mkv"
ALLOWED_EXTENSIONS = ["mp4", "mkv"]
DISALLOWED_ENDINGS = [ENDING]


SUBTITLE_LIMIT = 3
# Subtitles to remove
REMOVE_SUBTITLES = ["sing", "song"]


MAX_TRANSCODE_DURATION_SECONDS = int(
    os.environ.get("MAX_TRANSCODE_DURATION_SECONDS", str(10 * 60 * 60))
)

# Minimum acceptable overlap between original and transcoded durations.
# overlap = min(orig, trans) / max(orig, trans)
MIN_DURATION_OVERLAP_RATIO = float(os.environ.get("MIN_DURATION_OVERLAP_RATIO", "0.90"))

# Before starting a transcode, ensure the file isn't still being copied/written.
FILE_SIZE_STABILITY_INTERVAL_SECONDS = int(
    os.environ.get("FILE_SIZE_STABILITY_INTERVAL_SECONDS", "5")
)
FILE_SIZE_STABILITY_THRESHOLD_BYTES = int(
    os.environ.get("FILE_SIZE_STABILITY_THRESHOLD_BYTES", str(1024))
)

# Prometheus metrics
total_files = Gauge("transcode_total_files", "Total number of files that exist")
total_files_to_process = Gauge(
    "transcode_total_files_to_process",
    "Total number of files that still need to be processed",
)
total_files_transcoded = Gauge(
    "transcode_total_files_transcoded", "Total number of files transcoded"
)
total_files_skipped = Gauge(
    "transcode_total_files_skipped",
    "Total number of files skipped because subtitles are browser-readable",
)
current_state = Enum(
    "transcode_current_state",
    "Current state of the transcoder",
    states=["idle", "processing"],
)
current_file = Info("transcode_current_file", "File currently being processed")

# Metrics readiness tracking
metrics_ready = threading.Event()
metrics_server_started = False

# Shutdown coordination
shutdown_event = threading.Event()
current_ffmpeg_process = None
_DB_INIT_LOCK = threading.Lock()
_DB_INITIALIZED = False


def init_skip_db() -> None:
    """Initialize (if needed) the SQLite DB that tracks skipped files."""
    global _DB_INITIALIZED
    if _DB_INITIALIZED:
        return
    with _DB_INIT_LOCK:
        if _DB_INITIALIZED:
            return
        db_parent = DB_PATH.parent
        if not db_parent.exists():
            db_parent.mkdir(parents=True, exist_ok=True)
        try:
            conn = sqlite3.connect(str(DB_PATH), timeout=30)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS skipped_transcodes (
                    file_path TEXT PRIMARY KEY,
                    reason TEXT NOT NULL,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        except Exception as exc:
            print("Failed to initialize skip DB:", exc)
            print("Removing potentially corrupt DB file.")
            try:
                DB_PATH.unlink()
            except Exception as rm_exc:
                print("Failed to remove DB file:", rm_exc)
                sys.exit(1)
        _DB_INITIALIZED = True


def get_db_connection() -> sqlite3.Connection:
    init_skip_db()
    return sqlite3.connect(str(DB_PATH), timeout=30)


def record_file_decision(
    file_path: Path, reason: str, metadata: dict[str, Any]
) -> None:
    payload = json.dumps(metadata, separators=(",", ":"), sort_keys=True)
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO skipped_transcodes (file_path, reason, metadata, created_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(file_path) DO UPDATE SET
                reason=excluded.reason,
                metadata=excluded.metadata,
                created_at=CURRENT_TIMESTAMP
            """,
            (str(file_path), reason, payload),
        )


def load_skip_record(file_path: Path) -> dict[str, Any] | None:
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT reason, metadata FROM skipped_transcodes WHERE file_path = ?",
            (str(file_path),),
        ).fetchone()
    if not row:
        return None
    try:
        metadata = json.loads(row[1]) if row[1] else {}
    except json.JSONDecodeError:  # pragma: no cover - defensive
        metadata = {}
    return {"reason": row[0], "metadata": metadata}


def delete_skip_record(file_path: Path) -> None:
    with get_db_connection() as conn:
        conn.execute(
            "DELETE FROM skipped_transcodes WHERE file_path = ?",
            (str(file_path),),
        )


def clear_skip_records() -> None:
    with get_db_connection() as conn:
        conn.execute("DELETE FROM skipped_transcodes")


def ensure_metrics_server_started() -> None:
    global metrics_server_started
    if metrics_server_started or METRICS_PORT <= 0:
        return
    if not metrics_ready.is_set():
        return
    _ = start_http_server(METRICS_PORT)
    metrics_server_started = True
    print(f"Prometheus metrics server started on port {METRICS_PORT}")


def probe_subtitle_streams(
    file_path: str, verbose: bool = False
) -> list[dict[str, Any]]:
    command = [
        "ffprobe",
        "-v",
        "error",
        "-print_format",
        "json",
        "-analyzeduration",
        "50G",
        "-probesize",
        "50M",
        "-select_streams",
        "s",
        "-show_entries",
        "stream=index,codec_name,codec_type:stream_tags=language,title",
        file_path,
    ]
    if verbose:
        print(" ".join(command))
    else:
        print(f"ffprobe subtitles (quiet): {file_path}")
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    out = result.stdout.strip()
    if not out:
        return []
    data = json.loads(out)
    if verbose:
        print("FFprobe stream info:")
        print(json.dumps(data, indent=4))
    return data.get("streams", [])


def file_signature(file_path: Path) -> dict[str, int]:
    stat = file_path.stat()
    return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _matches_signature(metadata: dict[str, Any], signature: dict[str, int]) -> bool:
    return (
        metadata.get("size") == signature["size"]
        and metadata.get("mtime_ns") == signature["mtime_ns"]
    )


def should_skip_due_to_text_subtitles(file_path: Path) -> bool:
    try:
        signature = file_signature(file_path)
    except FileNotFoundError:
        return False

    record = load_skip_record(file_path)
    cached_meta = (record or {}).get("metadata", {})
    cached_reason = (record or {}).get("reason")
    if cached_meta and _matches_signature(cached_meta, signature):
        if cached_reason == SKIP_REASON_TEXT:
            return True
        if cached_reason == SCAN_REASON_TRANSCODE:
            return False

    try:
        streams = probe_subtitle_streams(str(file_path))
    except subprocess.CalledProcessError as exc:
        print(f"Failed to probe subtitles for {file_path}: {exc}")
        return False

    if not streams:
        record_file_decision(file_path, SCAN_REASON_TRANSCODE, signature)
        return False

    text_codecs: list[str] = []
    non_text = False
    for stream in streams:
        codec_name = str(stream.get("codec_name", "")).lower()
        if codec_name in TEXT_BASED_SUBTITLE_CODECS:
            text_codecs.append(codec_name)
        else:
            non_text = True

    if text_codecs and not non_text:
        metadata: dict[str, Any] = {**signature, "codecs": sorted(set(text_codecs))}
        record_file_decision(file_path, SKIP_REASON_TEXT, metadata)
        pretty_codecs = ", ".join(metadata["codecs"])
        print(
            f"Skipping transcode for {file_path.name}: detected browser-readable subtitles ({pretty_codecs})."
        )
        return True

    metadata: dict[str, Any] = {**signature}
    if text_codecs:
        metadata["codecs"] = sorted(set(text_codecs))
    record_file_decision(file_path, SCAN_REASON_TRANSCODE, metadata)
    return False


def handle_shutdown(signum, frame) -> None:  # pyright: ignore[reportUnknownParameterType]
    """Signal handler for graceful shutdown (SIGINT/SIGTERM)."""
    print(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()
    # Attempt to terminate ffmpeg process if running
    global current_ffmpeg_process
    if current_ffmpeg_process and current_ffmpeg_process.poll() is None:
        try:
            print("Sending SIGTERM to ffmpeg process...")
            current_ffmpeg_process.terminate()
        except Exception as e:  # pragma: no cover - defensive
            print("Failed to terminate ffmpeg process:", e)


def install_signal_handlers():
    _ = signal.signal(signal.SIGTERM, handle_shutdown)  # pyright: ignore[reportUnknownArgumentType]
    _ = signal.signal(signal.SIGINT, handle_shutdown)  # pyright: ignore[reportUnknownArgumentType]


def main():
    # Check if VAAPI render device exists
    if not Path(VAAPI_RENDER_DEVICE).exists():
        print(f"VAAPI render device {VAAPI_RENDER_DEVICE} not found!")
        sys.exit(1)

    # Start Prometheus metrics server
    if METRICS_PORT > 0:
        print(
            f"Prometheus metrics server will start after initial scan on port {METRICS_PORT}"
        )
    else:
        print("Prometheus metrics server disabled (METRICS_PORT <= 0)")

    install_signal_handlers()

    try:
        _ = subprocess.run(["ffmpeg", "-version"], capture_output=False, text=True)
    except FileNotFoundError:
        print("FFmpeg not found!")
        sys.exit(1)

    init_skip_db()

    # Setup metrics
    current_state.state("idle")

    # Clean up any bad transcodes on startup
    try:
        cleanup_bad_transcodes()
    except Exception:
        print("Failed to cleanup bad transcodes on startup.")

    while not shutdown_event.is_set():
        processed_file = process_new()
        if processed_file:
            # Update Jellyfin
            if JELLYFIN_API != "":
                print("Updating Jellyfin libraries...")
                try:
                    update_all_libraries(JELLYFIN_URL, JELLYFIN_API)
                except Exception as e:
                    print("Failed to update Jellyfin libraries.", e)
            # Short sleep if a file was processed
            for _ in range(100):  # interruptible sleep (100 * 0.1 = 10s)
                if shutdown_event.is_set():
                    break
                time.sleep(0.1)
        else:
            # Interruptible longer sleep (60s)
            print("No files to process. Sleeping for 60 seconds...")
            current_state.state("idle")
            for _ in range(600):  # 600 * 0.1 = 60s
                if shutdown_event.is_set():
                    break
                time.sleep(0.1)

    # Final cleanup before exit
    print("Shutdown requested. Cleaning up...")
    current_state.state("idle")
    current_file.info({"file": ""})

    # Ensure ffmpeg process is terminated if still running
    global current_ffmpeg_process
    if current_ffmpeg_process and current_ffmpeg_process.poll() is None:
        try:
            print("Terminating ffmpeg process during shutdown...")
            current_ffmpeg_process.terminate()
            current_ffmpeg_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print("FFmpeg did not exit after SIGTERM, killing...")
            current_ffmpeg_process.kill()
        except Exception as e:  # pragma: no cover - defensive
            print("Error terminating ffmpeg during shutdown:", e)

    print("Graceful shutdown complete.")


# Main loop
def process_new() -> bool:
    # Get all files that need to be processed
    all = get_all_files()
    to_process, skipped_files = remove_files_if_procesed(all)

    # Update metrics
    total_files.set(len(all))
    total_files_to_process.set(len(to_process))
    total_files_skipped.set(len(skipped_files))
    processed_count = len(all) - len(to_process) - len(skipped_files)
    total_files_transcoded.set(processed_count)

    metrics_ready.set()
    ensure_metrics_server_started()

    if len(to_process) >= 1 and not shutdown_event.is_set():
        print(f"Found {len(to_process)} files to process.")
        random_file = random.Random().choice(to_process)
        print(f"Picking random file: {random_file}")
        process_file(random_file)
        return True
    return False


# Execute ffmpeg
def run_ffmpeg_vaapi(
    input_path: str,
    output_path: str,
    subtitle_limit: int = SUBTITLE_LIMIT,
):
    _wait_for_file_size_stability(Path(input_path))

    filters: list[str] = []
    maps: list[str] = []

    if subtitle_limit > 0:
        streams = get_stream_info(input_path)
        streams = streams[:subtitle_limit]  # Limit number of subtitles
        print("Subtitle streams found:", streams)
        # Split
        # filters.append(
        #     f"[0:v]split={len(streams)}{''.join(f'[split{i}]' for i in range(len(streams)))}"
        # )
        for i, sub_index in enumerate(streams):
            chain = (
                f"[0:v][0:{sub_index}]overlay[burned_{i}];"
                f"[burned_{i}]format=nv12,hwupload[v_out{i}]"
            )
            filters.append(chain)
            maps.extend(["-map", f"[v_out{i}]"])
    else:
        # No subtitles? Just pass the hardware stream through
        maps.extend(["-map", "0:v"])

    # Combine filters
    filter_complex: list[str] = (
        ["-filter_complex", ";".join(filters)] if filters else []
    )
    print("Filter complex:", filter_complex)

    # Figure out compression_level
    def compression_level() -> int:  # type: ignore
        vbaq = 16
        pre_encode = 8
        quality_preset = 4
        balanced_preset = 2
        speed_preset = 0

        level = vbaq + balanced_preset
        # level = (level << 1) | 1  # Set validity bit
        return level

    command = [
        "ffmpeg",
        "-hide_banner",  # suppress banner
        "-stats_period",
        "5",  # Only show stats periodically
        "-progress",
        "pipe:1",  # progress to stdout
        "-nostats",  # suppress periodic stats, we use the progress for that
        "-analyzeduration",
        "20G",  # increase analyze duration
        "-probesize",
        "20M",  # increase probe size
        "-init_hw_device",
        f"vaapi=va:{VAAPI_RENDER_DEVICE}",  # Initialize VAAPI device
        # "-hwaccel",
        # "vaapi",  # Use VAAPI hardware acceleration for decoding
        # "-hwaccel_device",
        # "va", # Use VAAPI device for filters
        "-hwaccel_output_format",
        "vaapi",  # Use VAAPI for hwaccel output
        "-i",
        input_path,
        # "-filter_hw_device",
        # "va",  # Use VAAPI device for filters
        *filter_complex,  # Add filters
        *maps,  # video map
        "-map",
        "0:a",  # all audio streams
        "-c:v",
        "hevc_vaapi",  # Video Encoder
        "-qp",  # Constant Quality
        "22",  # Lower => better quality
        "-rc_mode",
        "CQP",  #
        "-compression_level",
        str(compression_level()),  # Higher => faster
        "-c:a",
        "libvorbis",  # Audio Encoder
        "-fflags",
        "+genpts",  # Generate PTS if missing
        "-avoid_negative_ts",
        "make_zero",  # Avoid negative timestamps
        "-shortest:v",  # Cut to shortest stream (avoid hanging on subtitles)
        # "-t",
        # "00:02:00",  # Limit to first 30 minutes for testing
        output_path,
    ]
    print(" ".join(command))
    # ffmpeg_filtergraph(command)
    start_ffmpeg_process(command)


def _wait_for_file_size_stability(
    file_path: Path,
    interval_seconds: int = FILE_SIZE_STABILITY_INTERVAL_SECONDS,
    threshold_bytes: int = FILE_SIZE_STABILITY_THRESHOLD_BYTES,
) -> None:
    """Block until file size remains stable within threshold over one interval."""
    if interval_seconds <= 0:
        return

    while not shutdown_event.is_set():
        try:
            size1 = file_path.stat().st_size
        except FileNotFoundError:
            return
        except Exception as exc:  # pragma: no cover - defensive
            print(f"Failed to stat {file_path} for stability check: {exc}")
            return

        for _ in range(interval_seconds * 10):  # interruptible sleep
            if shutdown_event.is_set():
                return
            time.sleep(0.1)

        try:
            size2 = file_path.stat().st_size
        except FileNotFoundError:
            return
        except Exception as exc:  # pragma: no cover - defensive
            print(f"Failed to stat {file_path} for stability check: {exc}")
            return

        if abs(size2 - size1) <= threshold_bytes:
            return

        print(
            f"Input file still changing (Δ{abs(size2 - size1)} bytes > {threshold_bytes}); waiting {interval_seconds}s: {file_path.name}"
        )


def start_ffmpeg_process(
    command: list[str],
    termination_timeout: int = 15,
) -> None:
    """Start ffmpeg process and handle graceful shutdown."""
    global current_ffmpeg_process
    # Start ffmpeg in a new process group so we can terminate the whole group if needed
    current_ffmpeg_process = subprocess.Popen(
        command,
        text=True,
        start_new_session=True,
        env={**os.environ, "LIBVA_DRIVER_NAME": "radeonsi"},
    )
    try:
        while True:
            if shutdown_event.is_set():
                if current_ffmpeg_process.poll() is None:
                    print(
                        "Shutdown detected. Sending SIGTERM to ffmpeg process group..."
                    )
                    try:
                        # Send SIGTERM to the process group for all ffmpeg children
                        os.killpg(current_ffmpeg_process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    # Wait up to termination_timeout seconds
                    waited = 0
                    while waited < termination_timeout:
                        if current_ffmpeg_process.poll() is not None:
                            break
                        time.sleep(1)
                        waited += 1
                    if current_ffmpeg_process.poll() is None:
                        print(
                            "FFmpeg did not exit after SIGTERM, sending SIGKILL to process group..."
                        )
                        try:
                            os.killpg(current_ffmpeg_process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    raise InterruptedError("Transcode interrupted by shutdown")
                break

            ret = current_ffmpeg_process.poll()
            if ret is not None:
                break
            time.sleep(1)
        ret_code = current_ffmpeg_process.returncode
        if ret_code != 0:
            raise Exception(f"FFmpeg exited with non-zero status {ret_code}")
    finally:
        current_ffmpeg_process = None


def ffmpeg_filtergraph(command: list[str]):
    """Extract filtergraph from ffmpeg command for logging."""
    # Extract input -i file
    input: list[str] = command[0 : command.index("-i") + 2]
    # Extract filter_complex
    filter_complex: str = command[command.index("-filter_complex") + 1]
    print(input, filter_complex)

    result = subprocess.run(
        [
            *input,
            "-filter_complex",
            filter_complex,
            "-f",
            "graphviz",
            "-loglevel",
            "debug",
            "pipe:1",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"FFmpeg filtergraph error: {result.stderr}")
    print(result.stdout)


def get_stream_info(file_path: str) -> list[str]:
    try:
        streams = probe_subtitle_streams(file_path, verbose=True)
    except subprocess.CalledProcessError as exc:  # pragma: no cover - ffprobe failure
        raise Exception(f"FFprobe error: {exc.stderr}") from exc

    # keep only streams that don't contain unwanted tags
    streams = [
        s
        for s in streams
        if not any(
            rem.lower() in s.get("tags", {}).get("title", "").lower()
            for rem in REMOVE_SUBTITLES
        )
    ]

    # sort directly on the JSON objects by language
    streams.sort(
        key=lambda s: (
            0
            if s.get("tags", {}).get("language") == "eng"
            else 1
            if s.get("tags", {}).get("language") == "und"
            else 2
        )
    )

    result_streams: list[str] = []
    for stream in streams:
        index = stream.get("index")
        if index is None:
            continue
        result_streams.append(str(index))
    return result_streams


# Process a single file
def process_file(file_path: Path):
    current_state.state("processing")
    current_file.info({"file": str(file_path.name)})
    try:
        dir_name = file_path.parent
        name = file_path.stem.removesuffix(ENDING_ORG)
        output_name = f"{name}{ENDING}.{TARGET_FROMAT}"
        output_path = dir_name / output_name
        print("===================== Processing started ======================")
        run_ffmpeg_vaapi(str(file_path), str(output_path))
        print("===================== Finished processing =====================")
    except InterruptedError:
        print("Processing interrupted. Cleaning partial transcode...")
        delete_transcode(file_path)
    except KeyboardInterrupt:
        print("KeyboardInterrupt detected. Cleaning partial transcode...")
        delete_transcode(file_path)
        sys.exit(1)
    except BaseException as e:
        traceback.print_exc()
        print(f"Error processing file {file_path}:\n\t {e}")
        delete_transcode(file_path)
    finally:
        # Adjust metrics only if we actually started a file (avoid negative values)
        if total_files_to_process._value.get() > 0:  # type: ignore[attr-defined]
            total_files_to_process.dec()
        total_files_transcoded.inc()
        current_state.state("idle")
        current_file.info({"file": ""})
    current_state.state("idle")


# Scan "INPUT_DIR" for all files
def get_all_files() -> list[Path]:
    input_dir = Path(INPUT_DIR).resolve()
    input_files: list[Path] = []
    for ext in ALLOWED_EXTENSIONS:
        for file_path in input_dir.rglob(f"*.{ext}"):
            name = file_path.stem
            if not any(name.endswith(ending) for ending in DISALLOWED_ENDINGS):
                input_files.append(file_path)
    return input_files


def get_all_transcoded_files(all_files: list[Path]) -> list[Path]:
    transcoded_files: list[Path] = []
    for file_path in all_files:
        dir_name = file_path.parent
        name = file_path.stem.removesuffix(ENDING_ORG)
        for ext in set([TARGET_FROMAT, *ALLOWED_EXTENSIONS]):
            processed_name = f"{name}{ENDING}.{ext}"
            processed_path = dir_name / processed_name
            if processed_path.exists():
                transcoded_files.append(processed_path)
    return transcoded_files


# Remove files that have already been processed
def remove_files_if_procesed(file_list: list[Path]) -> tuple[list[Path], list[Path]]:
    unprocessed_files: list[Path] = []
    skipped_files: list[Path] = []
    for file_path in file_list:
        dir_name = file_path.parent
        name = file_path.stem.removesuffix(ENDING_ORG)
        processed_name = f"{name}{ENDING}.{TARGET_FROMAT}"
        processed_path = dir_name / processed_name
        if processed_path.exists():
            continue

        try:
            if should_skip_due_to_text_subtitles(file_path):
                skipped_files.append(file_path)
                continue
        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"Failed to evaluate subtitles for {file_path}: {exc}")

        unprocessed_files.append(file_path)
    return unprocessed_files, skipped_files


# Update jellyfin registries
def update_all_libraries(jellyfin_url: str, api_key: str):
    """Fetch all libraries from Jellyfin and trigger a scan for each."""
    headers = {"X-Emby-Token": api_key}

    # Fetch libraries
    try:
        resp = requests.get(f"{jellyfin_url}/Library/VirtualFolders", headers=headers)
        resp.raise_for_status()
        libraries = resp.json()  # pyright: ignore[reportAny]
    except Exception as e:
        print(f"Failed to fetch libraries: {e}")
        return

    if not libraries:
        print("No libraries found.")
        return

    # Trigger a scan for each library
    for lib in libraries:  # pyright: ignore[reportAny]
        lib_id = lib.get("ItemId")  # pyright: ignore[reportAny]
        lib_name = lib.get("Name", "Unknown")  # pyright: ignore[reportAny]
        if not lib_id:
            print(f"Skipping library {lib_name} (no ID)")
            continue

        print(f"Starting scan for library '{lib_name}' (ID: {lib_id})...")
        try:
            scan_url = (
                f"{jellyfin_url}/Items/{lib_id}/Refresh"
                "?Recursive=true&ImageRefreshMode=Default&MetadataRefreshMode=Default"
                "&ReplaceAllImages=false&RegenerateTrickplay=false&ReplaceAllMetadata=false"
            )
            _ = requests.post(scan_url, headers=headers)
            print(f"Scan triggered for '{lib_name}'.")
        except Exception as e:
            print(f"Failed to scan library {lib_name}: {e}")


def delete_transcode(file: Path):
    dir_name = file.parent
    name = file.stem.removesuffix(ENDING_ORG)
    processed_name = f"{name}{ENDING}.{TARGET_FROMAT}"
    processed_path = dir_name / processed_name
    if processed_path.exists():
        processed_path.unlink()
        print(f"Deleted transcoded file: {processed_path}")


def cleanup_bad_transcodes():
    all_files = get_all_files()
    for file_path in all_files:
        dir_name = file_path.parent
        name = file_path.stem.removesuffix(ENDING_ORG)
        processed_name = f"{name}{ENDING}.{TARGET_FROMAT}"
        processed_path = dir_name / processed_name
        if processed_path.exists() and processed_path.stat().st_size < 100:
            print(f"Deleting bad transcode because its empty: {processed_path}")
            processed_path.unlink()


def probe_duration_seconds(file_path: Path) -> float | None:
    """Return the media duration in seconds using ffprobe."""
    command = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(file_path),
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as exc:
        print(f"Failed to probe duration for {file_path}: {exc.stderr or exc}")
        return None
    raw = result.stdout.strip()
    try:
        return float(raw)
    except ValueError:
        print(f"Unable to parse duration for {file_path}: '{raw}'")
        return 60 * 60 * 999  # 999 hours


def duration_overlap_ratio(original_seconds: float, transcoded_seconds: float) -> float:
    """Return overlap ratio in [0, 1] where 1 means equal durations."""
    if original_seconds <= 0 or transcoded_seconds <= 0:
        return 0.0
    shorter = min(original_seconds, transcoded_seconds)
    longer = max(original_seconds, transcoded_seconds)
    if longer <= 0:
        return 0.0
    return shorter / longer


def find_original_for_transcode(
    transcoded_path: Path, all_files: list[Path]
) -> Path | None:
    """Find the original input file corresponding to a given transcoded file."""
    # Normalize stem back to the base "name" used in transcode naming.
    # Originals may be named either "<name> - Original.ext" or "<name>.ext".
    stem = transcoded_path.stem
    if stem.endswith(ENDING):
        base = stem[: -len(ENDING)]
    else:
        base = stem

    candidates: list[Path] = []
    for src in all_files:
        src_stem = src.stem
        if src_stem == base or src_stem == f"{base}{ENDING_ORG}":
            candidates.append(src)

    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    # Prefer a file explicitly marked as Original.
    for c in candidates:
        if c.stem.endswith(ENDING_ORG):
            return c
    return candidates[0]


def clean_bad_transcodes(
    limit_seconds: int = MAX_TRANSCODE_DURATION_SECONDS,
    min_overlap_ratio: float = MIN_DURATION_OVERLAP_RATIO,
) -> None:
    print("Scanning for suspiciously long or mismatched transcoded files...")
    all_files = get_all_files()
    transcoded = sorted(
        {path.resolve() for path in get_all_transcoded_files(all_files)}
    )
    flagged: list[tuple[Path, str]] = []
    for transcode_path in transcoded:
        trans_duration = probe_duration_seconds(transcode_path)
        if trans_duration is None:
            continue

        # 1) Flag transcoded files that are implausibly long.
        if trans_duration > limit_seconds:
            flagged.append(
                (
                    transcode_path,
                    f"transcoded duration {trans_duration / 3600:.2f}h exceeds {limit_seconds / 3600:.2f}h",
                )
            )
            continue

        # 2) Flag files where original/transcoded durations diverge too much.
        original_path = find_original_for_transcode(transcode_path, all_files)
        if original_path is None:
            # If we can't find the original, don't delete automatically.
            continue

        orig_duration = probe_duration_seconds(original_path)
        if orig_duration is None:
            continue

        overlap = duration_overlap_ratio(orig_duration, trans_duration)
        if overlap < min_overlap_ratio:
            flagged.append(
                (
                    transcode_path,
                    (
                        f"duration overlap {overlap * 100:.1f}% < {min_overlap_ratio * 100:.1f}% "
                        f"(orig {orig_duration:.1f}s vs trans {trans_duration:.1f}s)"
                    ),
                )
            )

    hours_limit = limit_seconds / 3600
    if not flagged:
        print(
            "No transcoded files exceeded duration limits or failed overlap checks. Nothing to clean."
        )
        return

    print(
        f"Found {len(flagged)} candidate transcodes to delete (limit {hours_limit:.2f}h, overlap >= {min_overlap_ratio * 100:.1f}%):"
    )
    for file_path, reason in flagged:
        print(f" - {file_path} ({reason})")

    confirmation = input("Delete these files? Type 'y' to confirm: ").strip().lower()
    if confirmation != "y":
        print("Aborting clean. No files were deleted.")
        return

    deleted = 0
    for file_path, _ in flagged:
        try:
            file_path.unlink()
            deleted += 1
            print(f"Deleted {file_path}")
        except FileNotFoundError:
            print(f"File already missing, skipping: {file_path}")
        except Exception as exc:
            print(f"Failed to delete {file_path}: {exc}")

    print(f"Clean complete. Deleted {deleted} files.")


if __name__ == "__main__":
    init_skip_db()
    # main.py delete
    if len(sys.argv) > 1 and sys.argv[1] == "delete":
        print("Finding all transcoded files for delition ...")
        all_files = get_all_files()
        print(f"Found {len(all_files)} total files.")
        transcoded = get_all_transcoded_files(all_files)
        print(f"Found {len(transcoded)} transcoded files to delete.")
        for file_path in transcoded:
            print(f" - {file_path.name}")
        print("Press y to continue...")
        confirmation = input().strip().lower()
        if confirmation != "y":
            print("Aborting deletion.")
            sys.exit(0)
        for file_path in all_files:
            delete_transcode(file_path)
        print("Deletion complete.")
        sys.exit(0)
    if len(sys.argv) > 1 and sys.argv[1] == "clear-db":
        print("This will remove all skip-tracking records.")
        print(f"Database path: {DB_PATH}")
        print("Press y to continue...")
        confirmation = input().strip().lower()
        if confirmation != "y":
            print("Aborting DB clear.")
            sys.exit(0)
        clear_skip_records()
        print("Skip-tracking table cleared.")
        sys.exit(0)
    # main.py list
    if len(sys.argv) > 1 and sys.argv[1] == "list":
        print("Finding all transcoded files ...")
        all_files = get_all_files()
        print(f"Found {len(all_files)} total files.")
        transcoded = get_all_transcoded_files(all_files)
        transcoded = sorted({p for p in transcoded}, key=lambda p: p.name)
        print(f"Found {len(transcoded)} transcoded files:")
        for file_path in transcoded:
            print(f" - {file_path.name}")
        sys.exit(0)
    if len(sys.argv) > 1 and sys.argv[1] == "clean":
        clean_bad_transcodes()
        sys.exit(0)

    print("Starting transcoder...")
    print("Input Directory:", INPUT_DIR)
    print("Run `main.py delete` to delete all transcoded files.")
    print("Run `main.py list` to list all transcoded files.")
    print("Run `main.py clear-db` to remove skip-tracking metadata.")
    print(
        "Run `main.py clean` to remove transcoded files longer than"
        f" {MAX_TRANSCODE_DURATION_SECONDS / 3600:.0f} hours."
    )
    main()
