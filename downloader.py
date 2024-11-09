import os
from typing import Any, cast


import yt_dlp # type: ignore

from media_filesystem import MediaFilesystem
from datatypes import VideoID

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

def ytdlp_download_video(media_fs: MediaFilesystem, vid: VideoID, max_res: int | None, logged_in_path: str | None) -> InfoDict | None:
    parameters = {
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
            "vcodec:vp9", # Possibly switch to AV1
            "acodec:opus",
            f"res{f":{max_res}" if max_res is not None else ""}"
        ]
    }
    if logged_in_path:
        po_token = None
        try:
            with open(f"{logged_in_path}.pot","r",encoding="utf-8") as f:
                po_token = f.read()
        except (FileNotFoundError, IsADirectoryError):
            pass
        if po_token:
            parameters.update({
                "extractor_args": {
                    "youtube":{
                        'player-client':'web,default',
                        'po_token':[f"web+{po_token}"]
                    }
                },
                "cookiefile": f"{logged_in_path}.cjar"
            })
            print("Using PO Token")
        else:
            print("Invalid PO Token")

    dl = yt_dlp.YoutubeDL(parameters)
    info: InfoDict | None = dl.extract_info(str(vid),download=not media_fs.video_exists(vid))
    if info is None or info["is_live"] is True:
        return None

    src_file = f"/tmp/video_dl_{vid}.mkv"
    if not os.path.isfile(src_file):
        raise IOError("Error downloading video")
    media_fs.write_video(vid,src_file)
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
