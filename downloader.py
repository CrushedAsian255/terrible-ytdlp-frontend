import os
from typing import Any, cast

import yt_dlp # type: ignore

from datatypes import VideoID, convert_file_size

CONCURRENT_THREADS = 8

class NoLog:
    @staticmethod
    def warning(content: str) -> None:
        pass
    @staticmethod
    def debug(content: str) -> None:
        pass
    @staticmethod
    def error(content: str) -> None:
        pass

InfoDict = dict[str,Any]

def ytdlp_download_video(media_path: str, vid: VideoID, max_res: int | None) -> InfoDict | None:
    dl = yt_dlp.YoutubeDL({
        # "logger": NoLog,
        # "verbose": True,
        "noplaylist": True,
        "outtmpl": {"default": "/tmp/video_dl_%(id)s.mkv"},
        "final_ext": "mkv",
        "merge_output_format": "mkv",
        "concurrent_fragment_downloads": CONCURRENT_THREADS,
        "ignoreerrors": True,
        "match_filter": lambda x: 'Skipped: Live video' if x['is_live'] else None,
        "writesubtitles": True,
        "format":"bestaudio+bestvideo",
        "subtitleslangs": ['all', '-live_chat'],
        "postprocessors":[
            {"key": "FFmpegEmbedSubtitle"},
            {
                "key": "FFmpegMetadata",
                'add_chapters': True
            }
        ],
        "format_sort":[
            "acodec:opus",
            f"res{f":{max_res}" if max_res is not None else ""}"
        ]
    })
    dest_file = vid.filename(media_path)
    info: InfoDict | None = dl.extract_info(str(vid),download=not os.path.isfile(dest_file))
    if info is None or info["is_live"] is True:
        return None

    if os.path.isfile(dest_file):
        return info

    src_file = f"/tmp/video_dl_{vid}.mkv"
    src_size = os.stat(src_file).st_size
    os.makedirs(vid.foldername(media_path),exist_ok=True)

    with open(src_file, "rb") as src:
        with open(f"{dest_file}.tmp", "wb") as dest:
            copied = 0
            while True:
                blk = src.read(2**23) # 8mb blocks
                if not blk:
                    break
                dest.write(blk)
                copied += len(blk)
                print(
                    f"Copying file: {convert_file_size(copied)} / {convert_file_size(src_size)}, "
                    f"{copied*100/src_size:.01f}%",end="\r"
                )
    if os.stat(f"{dest_file}.tmp").st_size != src_size:
        print("\nError: file sizes don't match")
        return None
    print(f"\nCopied {convert_file_size(src_size)}")
    os.rename(f"{dest_file}.tmp", dest_file)
    os.remove(src_file)
    return info

def ytdlp_download_playlist_metadata(purl: str , channel_mode: bool = False) -> InfoDict | None:
    dl = yt_dlp.YoutubeDL({
        "extract_flat": (True if channel_mode else 'in_playlist'),
        "skip_download": True,
        "ignoreerrors": True,
        # "logger": NoLog
    })
    return cast(InfoDict | None,dl.extract_info(purl, download=False))