import os
import shutil
import subprocess
from typing import Any

from argparse import ArgumentParser

from library import Library
from datatypes import VideoID, PlaylistID, ChannelID, TagID, TagNumID
from datatypes import VideoMetadata, PlaylistMetadata, convert_file_size
from datatypes import infer_type

def get_item_fzf(items: list[str]) -> str | None:
    with subprocess.Popen(
        ['fzf'],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    ) as process:
        if process.stdin is None:
            return None
        with process.stdin as stdin:
            stdin.write("\n".join(items))
        if process.stdout is None:
            return None
        out_str = process.stdout.readline().split(" | ")[0]
        if len(out_str)==0:
            return None
        return out_str
    return None

def open_mpv(in_str: str | None) -> None:
    if in_str is None:
        return
    with subprocess.Popen(
        ['mpv', '--playlist=-'],
        stdin=subprocess.PIPE, text=True
    ) as process:
        if process is None or process.stdin is None:
            return
        with process.stdin as stdin:
            stdin.write(in_str)
        try:
            process.wait()
        except KeyboardInterrupt:
            process.kill()
            return

def pick_video_fzf(videos: list[VideoMetadata]) -> VideoID | None:
    x = get_item_fzf([video.to_string() for video in videos])
    if x is None:
        return None
    return VideoID(x)

def pick_playlist_fzf(playlists: list[PlaylistMetadata[Any]]) -> PlaylistID | None:
    x = get_item_fzf([playlist.to_string()for playlist in playlists])
    if x is None:
        return None
    return PlaylistID(x)

def pick_content_fzf(
    videos: list[VideoMetadata],
    playlists: list[PlaylistMetadata[Any]]
) -> VideoID | PlaylistID | None:
    x = get_item_fzf(
        [video.to_string() for video in videos]+
        [playlist.to_string()for playlist in playlists]
    )
    if x is None:
        return None
    out = infer_type(x)
    match out:
        case VideoID() | PlaylistID():
            return out
        case None:
            return None
        case _:
            raise SystemError("what")

def parse_command(
    lib: Library,
    command: str,
    url: str | None,
    auxiliary: bool,
    tag: TagID | None
) -> None:
    def fname(vid: VideoID) -> str:
        return vid.filename(lib.media_dir)
    content_id = None
    match command:
        case 'download':
            if not url:
                print("Error: No URL given")
                return
            content_id = infer_type(url)
            match content_id:
                case VideoID():
                    lib.download_video(content_id)
                    if tag:
                        lib.add_tag(tag,content_id)
                    if auxiliary:
                        open_mpv(fname(content_id))
                case PlaylistID():
                    lib.download_playlist(content_id)
                    if tag:
                        lib.add_tag(tag,content_id)
                case ChannelID():
                    lib.download_channel(content_id,not auxiliary)
        case 'new-tag':
            if tag is None:
                print("Error: A tag is required (pass with -t <tag>)")
                return
            lib.db.create_tag(tag,(url or ""))
        case 'tag':
            if not tag:
                print("Error: A tag is required (pass with -t <tag>)")
                return
            if url:
                content_id = infer_type(url)
            else:
                content_id = pick_content_fzf(
                    lib.get_all_videos(),
                    lib.get_all_playlists()
                )
            match content_id:
                case VideoID() | PlaylistID():
                    lib.add_tag(tag,content_id)
                case ChannelID():
                    print("Error: Cannot tag channel")
        case 'play':
            if url:
                content_id = infer_type(url)
                if isinstance(content_id,ChannelID):
                    content_id = pick_content_fzf(
                        lib.get_all_videos_from_channel(content_id),
                        lib.get_all_playlists_from_channel(content_id)
                    )
            else:
                content_id = pick_content_fzf(
                    lib.get_all_videos(tag),
                    lib.get_all_playlists(tag)
                )
            match content_id:
                case VideoID():
                    open_mpv(fname(content_id))
                case PlaylistID():
                    open_mpv(lib.create_playlist_m3u8(content_id,auxiliary))
        case 'play-v':
            if url:
                content_id = infer_type(url)
                if isinstance(content_id,PlaylistID):
                    raise NotImplementedError("Not implemented")
                if isinstance(content_id,ChannelID):
                    content_id = pick_video_fzf(lib.get_all_videos_from_channel(content_id))
            else:
                content_id = pick_video_fzf(lib.get_all_videos(tag))
            if content_id is not None:
                if auxiliary:
                    print(fname(content_id))
                else:
                    open_mpv(fname(content_id))
        case 'play-pl':
            if url:
                content_id = infer_type(url)
                if isinstance(content_id,VideoID):
                    raise NotImplementedError("Not implemented")
                if isinstance(content_id,ChannelID):
                    content_id = pick_playlist_fzf(lib.get_all_playlists_from_channel(content_id))
            else:
                content_id = pick_playlist_fzf(lib.get_all_playlists(tag))
            open_mpv(lib.create_playlist_m3u8(content_id,auxiliary))
        case 'check':
            lib.integrity_check()
        case 'prune':
            lib.prune()
        case 'purge':
            print(f"Total removed size: {convert_file_size(lib.purge())}")
        case 'size-v':
            video_sizes = []
            for vid in [x.id for x in lib.get_all_videos()]:
                is_single_video = len([
                    x for x in lib.db.get_video_tags(vid) if x != TagNumID(0)
                ]) <= 1
                if is_single_video and len(lib.db.get_video_playlists(vid)) == 0:
                    try:
                        video_sizes.append((vid,os.path.getsize(fname(vid))))
                    except FileNotFoundError:
                        print(f"ERROR: Missing file: {vid.fileloc}")
            video_sizes.sort(key=lambda x: -x[1])
            for vid, size in video_sizes[10:0:-1]:
                print(f"{vid} | {convert_file_size(size)}")
        case 'size-p':
            playlists: list[PlaylistID] = [x.id for x in lib.get_all_playlists()]
            playlist_sizes = []
            for idx, pid in enumerate(playlists):
                size = 0
                for vid in [x.id for x in lib.get_playlist_videos(pid)]:
                    video_tags = len(lib.db.get_video_tags(vid))
                    video_playlists = len(lib.db.get_video_playlists(vid))
                    if video_tags == 0 and video_playlists == 1:
                        try:
                            size += os.path.getsize(fname(vid))
                        except FileNotFoundError:
                            print(f"ERROR: Missing file: {vid.filename()}")
                playlist_sizes.append((pid,size))
                print(f"Enumerating... ({idx+1}/{len(playlists)})",end="\r")
            print()

            playlist_sizes.sort(key=lambda x: -x[1])
            for pid, size in playlist_sizes[10:0:-1]:
                print(f"{pid} | {convert_file_size(size)}")
        case 'update-thumbs':
            lib.update_thumbnails()

def try_copy(src: str, dst: str) -> bool:
    try:
        shutil.copy(src, dst)
    except FileNotFoundError:
        return False
    return True

def main() -> None:
    arg_parser = ArgumentParser(prog="ytd", description="YouTube downloader and database")

    arg_parser.add_argument(
        "-m", help="Path to media directory",
        dest="media_dir"
    )
    arg_parser.add_argument(
        "-d", help="Path to store database file",
        dest="database_path"
    )
    arg_parser.add_argument(
        "-r", help="Maximum video resolution to downloads",
        dest="max_resolution"
    )
    arg_parser.add_argument(
        "-v", help="Print all database requests",
        dest="print_db_log", action='store_true'
    )
    arg_parser.add_argument(
        "-l", help="Library name",
        dest="library", default='master'
    )
    arg_parser.add_argument(
        "-a", help="Perform auxiliary action (command specific)",
        dest="auxiliary", action='store_true'
    )
    arg_parser.add_argument(
        "-t", help="Tag to operate on or filter by",
        dest="tag"
    )
    arg_parser.add_argument("command", help="Command")
    arg_parser.add_argument("url",  help="URL", nargs='?')
    args = arg_parser.parse_args()

    bpath = os.path.expanduser("~/YouTube")
    if args.library == 'master' and (args.database_path or args.media_dir):
        print("Error: cannot use master db with custom paths")
        return
    if args.max_resolution and args.library == "master":
        print("Error: cannot use master db with resolution caps")
        return
    if args.max_resolution:
        args.library = f"{args.library}.{args.max_resolution}"
    lib_db = args.database_path if args.database_path else f"{bpath}/{args.library}.db"
    try_copy(f"{lib_db}.bak", f"{lib_db}.bak2")
    media_dir: str = args.media_dir if args.media_dir else f"{bpath}/{args.library}"

    try:
        with open(f"{bpath}/{args.library}","r",encoding="utf-8") as f:
            media_dir = f.read()
    except (FileNotFoundError, IsADirectoryError):
        pass

    if args.media_dir:
        media_dir = args.media_dir
        with open(f"{bpath}/{args.library}","w",encoding="utf-8") as f:
            f.write(media_dir)

    os.makedirs(media_dir, exist_ok=True)

    try_copy(f"{lib_db}.bak", f"{lib_db}.bak2")
    try_copy(lib_db, f"{lib_db}.bak")

    try:
        library = Library(lib_db,media_dir,args.max_resolution,args.print_db_log)
    except IOError as e:
        try_copy(f"{lib_db}.bak2", f"{lib_db}.bak")
        print(f"Error loading database!\n{e}\nAttempting to revert to backup")
        shutil.copy(lib_db, f"{lib_db}.err")
        if not try_copy(f"{lib_db}.bak", lib_db):
            print("Sorry, no backup could be found")
            return
        try:
            library = Library(lib_db,media_dir,args.max_resolution,args.print_db_log)
        except IOError as e2:
            print(f"Uh oh...\n{e2}\nBackup is also corrupted or none exists. Sorry mate :(")
            return
        print(f"Backup loaded successfully, corrupted version stored in {lib_db}.err")

    tag: TagID | None = None
    if args.tag:
        tag = TagID(args.tag)

    parse_command(library, args.command, args.url, args.auxiliary, tag)
    library.exit()

    try:
        os.remove(f"{lib_db}.bak2")
    except FileNotFoundError:
        pass

if __name__ == "__main__":
    main()
