import yt_dlp
from threading import Thread, Lock
import os

concurrent_threads = 8

def get_file_name(media_path, vid):
    return f"{media_path}/{ord(vid[0])-32}/{ord(vid[1])-32}/{vid}.mkv"

class NoLog:
    def warning(self): pass
    def debug(self): pass
    def error(self): pass

def convert_file_size(size: int):
    size=int(size)
    if size < 2**10: return f"{size           } B"
    if size < 2**20: return f"{size/(2**10):.02f} KiB"
    if size < 2**30: return f"{size/(2**20):.02f} MiB"
    if size < 2**40: return f"{size/(2**30):.02f} GiB"
    if size < 2**50: return f"{size/(2**40):.02f} TiB"
    if size < 2**60: return f"{size/(2**50):.02f} PiB"
    if size < 2**70: return f"{size/(2**60):.02f} EiB"
    else:            return f"{int(size/(2**60))} EiB"

def download_video(media_path: str, vid: str, max_res: int | None): 
    dl = yt_dlp.YoutubeDL({
        # "logger": NoLog,
        # "verbose": True,
        "noplaylist": True,
        "outtmpl": {"default": "/tmp/video_dl_%(id)s.mkv"},
        "final_ext": "mkv",
        "merge_output_format": "mkv",
        "concurrent_fragment_downloads": concurrent_threads,
        "ignoreerrors": True,
        "match_filter": lambda x: 'Skipped: Live video' if x['is_live'] else None,
        "writesubtitles": True,
        "format":"bestvideo+bestaudio/best",
        "subtitleslangs": ['all', '-live_chat'],
        "postprocessors":[
            {"key": "FFmpegEmbedSubtitle"},
            {
                "key": "FFmpegMetadata",
                'add_chapters': True
            }
        ],
        "format_sort":[f"res{f":{max_res}" if max_res is not None else ""}","vcodec:vp9","acodec:opus"]
    })
    dest_file = get_file_name(media_path, vid)
    if os.path.isfile(dest_file):
        info = dl.extract_info(vid,download=False)
        if info is None: return None
        if info["is_live"] is True: return None
        return info
    else:
        info = dl.extract_info(vid)
        if info is None: return None
        if info["is_live"] is True: return None

    src_file = f"/tmp/video_dl_{vid}.mkv"
    src_size = os.stat(src_file).st_size
    os.makedirs(f"{media_path}/{ord(vid[0])-32}/{ord(vid[1])-32}",exist_ok=True)
    if os.path.isfile(dest_file):
        if src_size == os.stat(dest_file).st_size: return info
    with open(src_file, "rb") as src:
        with open(f"{dest_file}.tmp", "wb") as dest:
            copied = 0
            while True:
                blk = src.read(2**23) # 8mb blocks
                if not blk: break
                dest.write(blk)
                copied += len(blk)
                print(f"Copying file: {convert_file_size(copied)} / {convert_file_size(src_size)}, {copied*100/src_size:.01f}%",end="\r")
    if os.stat(f"{dest_file}.tmp").st_size != src_size:
        print("\nError: file sizes don't match")
        return None
    else:
        print(f"\nCopied {convert_file_size(src_size)}")
    os.rename(f"{dest_file}.tmp", dest_file)
    os.remove(src_file)
    return info

def download_playlist_metadata(pid: str , channel_mode=False):
    dl = yt_dlp.YoutubeDL({
        "extract_flat": (True if channel_mode else 'in_playlist'),
        "skip_download": True,
        "ignoreerrors": True,
        "logger": NoLog
    })
    return dl.extract_info(pid, download=False)