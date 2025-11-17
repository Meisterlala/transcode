#!/usr/bin/env python3

import os
import random
import subprocess
import sys
import time
from pathlib import Path

import requests

# Directory to monitor for input files
INPUT_DIR = os.environ.get("INPUT_DIR", "in_test")
# URL to Jellyfin server
JELLYFIN_URL = os.environ.get("JELLYFIN_URL", "http://jellyfin:8096")
# Jellyfin API key
JELLYFIN_API = os.environ.get("JELLYFIN_API", "")

ENDING = " - Transcoded"
ENDING_ORG = " - Original"
TARGET_FROMAT = "mp4"
ALLOWED_EXTENSIONS = ["mp4", "mkv"]
DISALLOWED_ENDINGS = [ENDING]


def main():
    try:
        result = subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True)
        print("FFmpeg version:")
        print(result.stdout.split("\n")[0])  # First line has version
    except FileNotFoundError:
        print("FFmpeg not found!")
        sys.exit(1)

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
    to_process = remove_files_if_procesed(get_all_files())

    if len(to_process) >= 1:
        print(f"Found {len(to_process)} files to process.")
        random_file = random.Random().choice(to_process)
        print(f"Picking random file: {random_file}")
        process_file(random_file)
        return True
    return False


# Execute ffmpeg
def run_ffmpeg(input_path: str, output_path: str):
    # Get subtitle stream count
    streams = get_stream_info(input_path)
    streams = streams[:3]  # Limit to max 3 subtitle streams
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

    # if file path contains "anime", tune for anime
    if "anime" in input_path.lower():
        tune = "animation"
    else:
        tune = "film"

    command = [
        "ffmpeg",
        "-hide_banner",  # suppress banner
        "-stats_period",
        "1",  # Only show stats every second
        "-progress",
        "pipe:1",  # progress to stdout
        "-nostats",
        "-analyzeduration",
        "50G",
        "-probesize",
        "50M",
        "-i",
        input_path,
        "-filter_complex",
        ";".join(filters),
        *maps,
        "-map",
        "0:a",  # all audio streams
        # "-map",
        # "0:s?",  # subtitles, optional if they exist
        "-c:v",
        "libx264",
        "-preset",
        "slow",
        "-crf",
        "23",  # (lower = better quality)
        # "-t",
        # "20",
        "-c:a",
        "aac",
        "-movflags",
        "+faststart",  # for MP4 streaming,
        "-tune",
        tune,
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
        "stream=index",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        file_path,
    ]
    print(" ".join(command))
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    if result.returncode != 0:
        raise Exception(f"FFprobe error: {result.stderr}")
    out = result.stdout.strip()
    # Convert to arryy of stream indexes
    return out.split("\n") if out else []


# Process a single file
def process_file(file_path: Path):
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
