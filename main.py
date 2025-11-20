#!/usr/bin/env python3

import os
import random
import subprocess
import sys
import time
from pathlib import Path

import requests
from prometheus_client import Enum, Gauge, start_http_server

# Directory to monitor for input files
INPUT_DIR = os.environ.get("INPUT_DIR", "in_test")
# URL to Jellyfin server
JELLYFIN_URL = os.environ.get("JELLYFIN_URL", "http://jellyfin:8096")
# Jellyfin API key
JELLYFIN_API = os.environ.get("JELLYFIN_API", "")
# Prometheus metrics port
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9100"))

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


def main():
    # Start Prometheus metrics server
    _ = start_http_server(METRICS_PORT)
    print(f"Prometheus metrics server started on port {METRICS_PORT}")

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

    while True:
        if process_new():
            # Update Jellyfin
            if JELLYFIN_API != "":
                print("Updating Jellyfin libraries...")
                try:
                    update_all_libraries(JELLYFIN_URL, JELLYFIN_API)
                except Exception as e:
                    print("Failed to update Jellyfin libraries.", e)
            time.sleep(1)  # Short sleep if a file was processed

        time.sleep(60)  # Sleep before checking for new files


# Main loop
def process_new() -> bool:
    # Get all files that need to be processed
    all = get_all_files()
    to_process = remove_files_if_procesed(all)

    # Update metrics
    total_files.set(len(all))
    total_files_to_process.set(len(to_process))
    total_files_transcoded.set(len(all) - len(to_process))

    if len(to_process) >= 1:
        print(f"Found {len(to_process)} files to process.")
        random_file = random.Random().choice(to_process)
        print(f"Picking random file: {random_file}")
        process_file(random_file)
        return True
    return False


# Execute ffmpeg
def run_ffmpeg(input_path: str, output_path: str, subtitle_limit: int = 1):
    if subtitle_limit > 0:
        # Get subtitle stream count
        streams = get_stream_info(input_path)
        streams = streams[:subtitle_limit]  # Limit number of subtitles
        print("Subtitle streams found:", streams)
        # Build overlay filter
        filters: list[str] = []
        maps: list[str] = []
        for streams_index in streams:
            filters.append(f"[0:v][0:{streams_index}]overlay[v{streams_index}]")
            maps.extend(["-map", f"[v{streams_index}]"])
        # Fallback if no subtitles
        if not streams:
            maps.extend(["-map", "0:v"])
    else:
        filters = []
        maps = ["-map", "0:v"]  # all video and subtitles if exist

    # Combine filters
    filter_complex: list[str] = (
        ["-filter_complex", ";".join(filters)] if filters else []
    )

    # if file path contains "anime", tune for anime
    # if "anime" in input_path.lower():
    #     tune = "animation"
    # else:
    #     tune = "film"

    command = [
        "ffmpeg",
        "-hide_banner",  # suppress banner
        "-stats_period",
        "3",  # Only show stats every second
        "-progress",
        "pipe:1",  # progress to stdout
        "-nostats",  # suppress periodic stats, we use the progress for that
        "-analyzeduration",  # increase analyze duration
        "50G",
        "-probesize",  # increase probe size
        "50M",
        "-i",
        input_path,
        *filter_complex,  # Add filters
        *maps,  # video map
        "-map",
        "0:a",  # all audio streams
        "-c:v",
        "libsvtav1",  # Use H.264 codec
        "-crf",
        "23",  # (lower = better quality)
        "-preset",
        "10",  # speed vs quality (0=best quality, 13=fastest but really bad)
        # "-t",
        # "20",
        "-c:a",
        "libvorbis",  # Audio Encoder
        "-movflags",
        "+faststart",  # for MP4 streaming,
        # "-tune",
        # tune,
        output_path,
    ]
    print(" ".join(command))
    result = subprocess.run(command, capture_output=False, text=True, check=True)
    if result.returncode != 0:
        raise Exception(f"FFmpeg error: {result.stderr}")


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
    # Combine index and language
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
    try:
        dir_name = file_path.parent
        name = file_path.stem
        output_name = f"{name}{ENDING}.{TARGET_FROMAT}"
        output_path = dir_name / output_name
        print("===================== Processing started ======================")
        run_ffmpeg(str(file_path), str(output_path))
        print("===================== Finished processing =====================")
    except KeyboardInterrupt:
        delete_transcode(file_path)
        sys.exit(1)
    except BaseException as e:
        print(f"Error processing file {file_path}:\n\t {e}")
        delete_transcode(file_path)
    finally:
        total_files_to_process.dec()
        total_files_transcoded.inc()
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


# Remove files that have already been processed
def remove_files_if_procesed(file_list: list[Path]) -> list[Path]:
    unprocessed_files: list[Path] = []
    for file_path in file_list:
        dir_name = file_path.parent
        name = file_path.stem
        processed_name = f"{name}{ENDING}.{TARGET_FROMAT}"
        processed_path = dir_name / processed_name
        if not processed_path.exists():
            unprocessed_files.append(file_path)
    return unprocessed_files


# Update jellyfin registries
def update_all_libraries(jellyfin_url: str, api_key: str):
    """
    Fetch all libraries from Jellyfin and trigger a scan for each.

    :param jellyfin_url: Base URL of the Jellyfin server (e.g., http://server_ip)
    :param api_key: Your Jellyfin API key
    """
    headers = {"X-Emby-Token": api_key}

    # Fetch libraries
    try:
        resp = requests.get(f"{jellyfin_url}/Library/VirtualFolders", headers=headers)
        resp.raise_for_status()
        libraries = resp.json()
    except Exception as e:
        print(f"Failed to fetch libraries: {e}")
        return

    if not libraries:
        print("No libraries found.")
        return

    # Trigger a scan for each library
    for lib in libraries:
        lib_id = lib.get("ItemId")
        lib_name = lib.get("Name", "Unknown")
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
            requests.post(scan_url, headers=headers)
            print(f"Scan triggered for '{lib_name}'.")
        except Exception as e:
            print(f"Failed to scan library {lib_name}: {e}")


def delete_transcode(file: Path):
    dir_name = file.parent
    name = file.stem
    processed_name = f"{name}{ENDING}.{TARGET_FROMAT}"
    processed_path = dir_name / processed_name
    if processed_path.exists():
        processed_path.unlink()
        print(f"Deleted transcoded file: {processed_path}")


def cleanup_bad_transcodes():
    all_files = get_all_files()
    for file_path in all_files:
        dir_name = file_path.parent
        name = file_path.stem
        processed_name = f"{name}{ENDING}.{TARGET_FROMAT}"
        processed_path = dir_name / processed_name
        if processed_path.exists() and processed_path.stat().st_size < 100:
            print(f"Deleting bad transcode because its empty: {processed_path}")
            processed_path.unlink()


if __name__ == "__main__":
    print("Starting transcoder...")
    print("Input Directory:", INPUT_DIR)
    print("Run `main.py delete` to delete all transcoded files.")

    if len(sys.argv) > 1 and sys.argv[1] == "delete":
        print("Deleting all transcoded files...")
        print("Press y to continue...")
        confirmation = input().strip().lower()
        if confirmation != "y":
            print("Aborting deletion.")
            sys.exit(0)
        all_files = get_all_files()
        for file_path in all_files:
            delete_transcode(file_path)
        print("Deletion complete.")
        sys.exit(0)
    main()
