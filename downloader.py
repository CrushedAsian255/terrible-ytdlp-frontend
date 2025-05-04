""" Handles the actual downloading using yt-dlp """
import os
from typing import Any, cast


import yt_dlp # type: ignore

from datatypes import VideoID

CONCURRENT_THREADS = 8

class NoLog:
    """ Null logger """
    @staticmethod
    def warning(content: str) -> None:
        """ N/A """
    @staticmethod
    def debug(content: str) -> None:
        """ N/A """
    @staticmethod
    def error(content: str) -> None:
        """ N/A """

InfoDict = dict[str,Any]

def ytdlp_download_video(
        vid: VideoID, max_res: int | None,
        logged_in_path: str | None, should_download: bool,
        format_selector: str) -> tuple[InfoDict | None,str | None]:
    """ Download a video from YouTube and return its metadata """
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
        "format":format_selector,
        "subtitleslangs": ['all', '-live_chat'],
        "postprocessors":[
            {"key": "FFmpegEmbedSubtitle"},
            {
                "key": "FFmpegMetadata",
                'add_chapters': True
            }
        ],
        "format_sort":[
            f"res{f":{max_res}" if max_res is not None else ""}",
            "vcodec:vp9.2", # Possibly switch to AV1
            "acodec:opus"
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

    info: InfoDict | None = dl.extract_info(str(vid),download=should_download)
    if info is None or info["is_live"] is True:
        return (None,None)

    if not should_download:
        return (info, None)

    src_file = f"/tmp/video_dl_{vid}.mkv"
    if not os.path.isfile(src_file):
        raise IOError("Error downloading video")
    return (info, src_file)

def ytdlp_download_playlist_metadata(purl: str , channel_mode: bool = False) -> InfoDict | None:
    """ Download information about a playlist """
    dl = yt_dlp.YoutubeDL({
        "extract_flat": (True if channel_mode else 'in_playlist'),
        "skip_download": True,
        "ignoreerrors": True,
        # "logger": NoLog
    })
    return cast(InfoDict | None,dl.extract_info(purl, download=False))
