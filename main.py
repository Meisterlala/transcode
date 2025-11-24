#!/usr/bin/env python3

import os
import random
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

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

ENDING = " - Transcoded"
ENDING_ORG = " - Original"
TARGET_FROMAT = "mkv"
ALLOWED_EXTENSIONS = ["mp4", "mkv"]
DISALLOWED_ENDINGS = [ENDING]

# Prometheus metrics
total_files = Gauge("transcode_total_files", "Total number of files that exist")
total_files_to_process = Gauge(
    "transcode_total_files_to_process",
    "Total number of files that still need to be processed",
)
total_files_transcoded = Gauge(
    "transcode_total_files_transcoded", "Total number of files transcoded"
)
current_state = Enum(
    "transcode_current_state",
    "Current state of the transcoder",
    states=["idle", "processing"],
)
current_file = Info("transcode_current_file", "File currently being processed")

# Shutdown coordination
shutdown_event = threading.Event()
current_ffmpeg_process = None


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
    # Start Prometheus metrics server
    if METRICS_PORT > 0:
        _ = start_http_server(METRICS_PORT)
        print(f"Prometheus metrics server started on port {METRICS_PORT}")
    else:
        print("Prometheus metrics server disabled (METRICS_PORT <= 0)")

    install_signal_handlers()

    try:
        _ = subprocess.run(["ffmpeg", "-version"], capture_output=False, text=True)
    except FileNotFoundError:
        print("FFmpeg not found!")
        sys.exit(1)

    # Setup metrics
    current_state.state("idle")

    # Clean up any bad transcodes on startup
    try:
        cleanup_bad_transcodes()
    except Exception:
        print("Failed to cleanup bad transcodes on startup.")

    while not shutdown_event.is_set():
        if process_new():
            # Update Jellyfin
            if JELLYFIN_API != "":
                print("Updating Jellyfin libraries...")
                try:
                    update_all_libraries(JELLYFIN_URL, JELLYFIN_API)
                except Exception as e:
                    print("Failed to update Jellyfin libraries.", e)
            # Short sleep if a file was processed
            for _ in range(10):  # interruptible sleep (10 * 0.1 = 1s)
                if shutdown_event.is_set():
                    break
                time.sleep(0.1)
        else:
            # Interruptible longer sleep (60s)
            print("No files to process. Sleeping for 60 seconds...")
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
    to_process = remove_files_if_procesed(all)

    # Update metrics
    total_files.set(len(all))
    total_files_to_process.set(len(to_process))
    total_files_transcoded.set(len(all) - len(to_process))

    if len(to_process) >= 1 and not shutdown_event.is_set():
        print(f"Found {len(to_process)} files to process.")
        random_file = random.Random().choice(to_process)
        print(f"Picking random file: {random_file}")
        process_file(random_file)
        return True
    return False


# Execute ffmpeg
def run_ffmpeg(
    input_path: str,
    output_path: str,
    subtitle_limit: int = 0,
    termination_timeout: int = 15,
):
    filters: list[str] = []
    maps: list[str] = []

    if subtitle_limit > 0:
        streams = get_stream_info(input_path)
        streams = streams[:subtitle_limit]  # Limit number of subtitles
        print("Subtitle streams found:", streams)
        # 1. SPLIT (Hardware)
        # outputs = "".join([f"[base_hw{i}]" for i in range(len(streams))])
        # filters.append(f"[0:v]split={len(streams)}{outputs}")

        # 2. MAP, OVERLAY & UPLOAD
        for i, sub_index in enumerate(streams):
            # The Pipeline:
            # scale_vaapi=format=nv12  -> VITAL: GPU converts 10-bit to 8-bit here.
            #                             Prevents 'Failed to map frame: -22' error.
            # hwmap=...                -> Zero-copy map to CPU.
            # overlay                  -> Burn subtitle (Creates NEW frame, breaking the map).
            # hwupload                 -> Upload new frame to GPU.

            chain = (
                f"[base_hw{i}]hwdownload,format=nv12[base_soft{i}];"
                f"[base_soft{i}][0:{sub_index}]overlay[burned_{i}];"
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

    # if file path contains "anime", tune for anime
    tune = "animation" if "anime" in input_path.lower() else "film"

    command = [
        "ffmpeg",
        "-hide_banner",  # suppress banner
        "-stats_period",
        "3",  # Only show stats periodically
        "-progress",
        "pipe:1",  # progress to stdout
        "-nostats",  # suppress periodic stats, we use the progress for that
        "-analyzeduration",
        "50G",  # increase analyze duration
        "-probesize",
        "50M",  # increase probe size
        "-init_hw_device",
        "vaapi=va:/dev/dri/renderD129",  # Initialize VAAPI device
        "-hwaccel",
        "vaapi",  # Use VAAPI hardware acceleration
        "-hwaccel_device",
        "va",
        "-hwaccel_output_format",
        "vaapi",  # Use VAAPI for hwaccel output
        "-i",
        input_path,
        "-filter_hw_device",
        "va",  # Use VAAPI device for filters
        *filter_complex,  # Add filters
        *maps,  # video map
        "-map",
        "0:a",  # all audio streams
        "-c:v",
        "hevc_vaapi",  # Video Encoder
        # "-global_quality",
        # "50",
        # "-rc_mode",
        # "ICQ",  #
        # "-preset",
        # "superfast",  # speed vs quality
        # "-c:a",
        # "libvorbis",  # Audio Encoder
        # "-tune",
        # tune,
        output_path,
    ]
    print(" ".join(command))

    global current_ffmpeg_process
    # Start ffmpeg in a new process group so we can terminate the whole group if needed
    current_ffmpeg_process = subprocess.Popen(
        command,
        text=True,
        start_new_session=True,
        env={**os.environ, "LIBVA_DRIVER_NAME": "radeonsi"},
    )
    try:
        start_time = time.time()
        # Poll loop so we can react to shutdown_event
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


def get_stream_info(file_path: str) -> list[str]:
    command = [
        "ffprobe",
        "-v",
        "error",
        "-analyzeduration",
        "50G",
        "-probesize",
        "50M",
        "-select_streams",
        "s",
        "-show_entries",
        "stream=index:stream_tags=language",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        file_path,
    ]
    print(" ".join(command))
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    if result.returncode != 0:
        raise Exception(f"FFprobe error: {result.stderr}")
    print(result.stdout)
    out = result.stdout.strip().split("\n")

    # If odd number of lines, something went wrong
    if len(out) % 2 != 0:
        return out if out else []

    # Sort by language tag (prefer eng)
    combined: list[tuple[str, str]] = []
    for i in range(0, len(out), 2):
        index = out[i]
        language = out[i + 1]
        combined.append((index, language))

    # Sort so that "eng" comes first
    combined.sort(key=lambda x: 0 if x[1] == "eng" else 1 if x[1] == "und" else 2)
    out = [index for index, _ in combined]

    return out if out else []


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
        run_ffmpeg(str(file_path), str(output_path))
        print("===================== Finished processing =====================")
    except InterruptedError:
        print("Processing interrupted. Cleaning partial transcode...")
        delete_transcode(file_path)
    except KeyboardInterrupt:
        print("KeyboardInterrupt detected. Cleaning partial transcode...")
        delete_transcode(file_path)
        sys.exit(1)
    except BaseException as e:
        print(f"Error processing file {file_path}:\n\t {e}")
        delete_transcode(file_path)
    finally:
        # Adjust metrics only if we actually started a file (avoid negative values)
        if total_files_to_process._value.get() > 0:  # type: ignore[attr-defined]
            total_files_to_process.dec()
        total_files_transcoded.inc()
        current_state.state("idle")
        current_file.info({"file": ""})


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
def remove_files_if_procesed(file_list: list[Path]) -> list[Path]:
    unprocessed_files: list[Path] = []
    for file_path in file_list:
        dir_name = file_path.parent
        name = file_path.stem.removesuffix(ENDING_ORG)
        processed_name = f"{name}{ENDING}.{TARGET_FROMAT}"
        processed_path = dir_name / processed_name
        if not processed_path.exists():
            unprocessed_files.append(file_path)
    return unprocessed_files


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


if __name__ == "__main__":
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
    # main.py list
    if len(sys.argv) > 1 and sys.argv[1] == "list":
        print("Finding all transcoded files ...")
        all_files = get_all_files()
        print(f"Found {len(all_files)} total files.")
        transcoded = get_all_transcoded_files(all_files)
        print(f"Found {len(transcoded)} transcoded files:")
        for file_path in transcoded:
            print(f" - {file_path.name}")
        sys.exit(0)

    print("Starting transcoder...")
    print("Input Directory:", INPUT_DIR)
    print("Run `main.py delete` to delete all transcoded files.")
    print("Run `main.py list` to list all transcoded files.")
    main()
