import os
import shutil
import subprocess
import re
from typing import Any, Union

from argparse import ArgumentParser

from library import Library
from datatypes import VideoID, PlaylistID, ChannelID, TagID
from datatypes import VideoMetadata, PlaylistMetadata, convert_file_size

def infer_type(url: str) -> Union[VideoID,PlaylistID,ChannelID,TagID]:
    re_match = re.match(
        r"(?:https?:\/\/)?(?:www.)?youtube.com(?:\.[a-z]+)?\/"
        r"(?:watch\?v=|playlist\?list=|(?=@))(@?[0-9a-zA-Z-_]+)",
        url
    )
    value = re_match.groups()[0] if re_match else url
    try:
        return ChannelID(value)
    except ValueError:
        pass
    try:
        return TagID(value)
    except ValueError:
        pass
    try:
        return PlaylistID(value)
    except ValueError:
        pass
    try:
        return VideoID(value)
    except ValueError:
        pass
    raise ValueError(f"Unable to determine the format of {value}")

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

def parse_command(lib: Library, command: str, params: list[str], auxiliary: bool = False) -> None:
    def fname(vid: VideoID) -> str:
        return vid.filename(lib.media_dir)
    content_id = None
    match command:
        case 'download':
            if len(params) < 1:
                print("Error: No URL given")
            content_id = infer_type(params[0])
            match content_id:
                case VideoID():
                    lib.download_video(content_id)
                    if auxiliary:
                        open_mpv(fname(content_id))
                case PlaylistID():
                    lib.download_playlist(content_id)
                case ChannelID():
                    lib.download_channel(content_id,not auxiliary)
                case TagID():
                    print("Error: Invalid input")
        case 'new-tag':
            if len(params) < 1:
                print("Error: No tag ID given")
                return
            lib.db.create_tag(TagID(params[0]),(params[1] if len(params) > 1 else ""))
        case 'add-tag':
            if len(params) < 2:
                print("Error: Tag ID and URL required")
                return
            content_id = infer_type(params[1])
            match content_id:
                case VideoID():
                    lib.add_tag_to_video(TagID(params[0]),content_id)
                case PlaylistID():
                    lib.add_tag_to_playlist(TagID(params[0]),content_id)
                case ChannelID() | TagID():
                    print("Error: Invalid input")
        case 'play':
            try:
                content_id = infer_type(params[0])
                match content_id:
                    case TagID():
                        content_id = pick_content_fzf(
                            lib.get_all_videos(content_id),
                            lib.get_all_playlists(content_id)
                        )
                    case ChannelID():
                        content_id = pick_content_fzf(
                            lib.get_all_videos_from_channel(content_id),
                            lib.get_all_playlists_from_channel(content_id)
                        )
            except (ValueError, IndexError):
                content_id = pick_content_fzf(
                    lib.get_all_videos(),
                    lib.get_all_playlists()
                )
            match content_id:
                case VideoID():
                    open_mpv(fname(content_id))
                case PlaylistID():
                    open_mpv(lib.create_playlist_m3u8(content_id,auxiliary))
        case 'play-v':
            try:
                content_id = infer_type(params[0])
                match content_id:
                    case TagID():
                        content_id = pick_video_fzf(lib.get_all_videos(content_id))
                    case ChannelID():
                        content_id = pick_video_fzf(lib.get_all_videos_from_channel(content_id))
                    case PlaylistID():
                        raise NotImplementedError("Not implemented")
            except (ValueError, IndexError):
                content_id = pick_video_fzf(lib.get_all_videos())
            if content_id is not None:
                if auxiliary:
                    print(fname(content_id))
                else:
                    open_mpv(fname(content_id))
        case 'play-pl':
            try:
                content_id = infer_type(params[0])
                match content_id:
                    case TagID():
                        content_id = pick_playlist_fzf(lib.get_all_playlists(content_id))
                    case ChannelID():
                        content_id = pick_playlist_fzf(
                            lib.get_all_playlists_from_channel(content_id)
                        )
                    case VideoID():
                        raise NotImplementedError("Impossible!")
            except (ValueError, IndexError):
                content_id = pick_playlist_fzf(lib.get_all_playlists())
            open_mpv(lib.create_playlist_m3u8(content_id,auxiliary))
        case 'check':
            videos_filesystem: list[VideoID] = lib.get_all_filesystem_videos()
            videos_database: list[VideoID] = [x.id for x in lib.get_all_videos()]
            total_size = 0
            for vid in videos_filesystem:
                if vid not in videos_database:
                    size = os.path.getsize(fname(vid))
                    total_size += size
                    print(f"Orphaned file: {vid.filename()} | {convert_file_size(size)}")
            print(f"Total orphaned file size: {convert_file_size(total_size)}")
            total_size = 0
            for vid in videos_database:
                if vid not in videos_filesystem:
                    print(f"ERROR: Missing file: {vid.fileloc}")
                video_tags = len(lib.db.get_video_tags(vid))
                video_playlists = len(lib.db.get_video_playlists(vid))
                if video_tags == 0 and video_playlists == 0:
                    size = os.path.getsize(fname(vid))
                    total_size += size
                    print(f"Orphaned video: {vid} | {convert_file_size(size)}")
            print(f"Total orphaned video size: {convert_file_size(total_size)}")
        case 'prune':
            lib.prune()
        case 'purge':
            print(f"Total removed size: {convert_file_size(lib.purge())}")
        case 'size-v':
            video_sizes = []
            for vid in [x.id for x in lib.get_all_single_videos()]:
                if len(lib.db.get_video_playlists(vid)) == 0:
                    try:
                        video_sizes.append((vid,os.path.getsize(fname(vid))))
                    except FileNotFoundError:
                        print(f"ERROR: Missing file: {vid.fileloc}")
            video_sizes.sort(key=lambda x: -x[1])
            for vid, size in video_sizes[10:0:-1]:
                print(f"{vid} | {convert_file_size(size)}")
        case 'size-p':
            playlists = [x.id for x in lib.get_all_playlists()]
            playlist_sizes = []
            for idx1, pid in enumerate(playlists):
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
                print(f"Enumerating... ({idx1+1}/{len(playlists)})",end="\r")
            print()

            playlist_sizes.sort(key=lambda x: -x[1])
            for pid, size in playlist_sizes[10:0:-1]:
                print(f"{pid} | {convert_file_size(size)}")

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
    arg_parser.add_argument("command", help="Command")
    arg_parser.add_argument("params",  help="Parameters", nargs='*')
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

    parse_command(library, args.command, args.params, args.auxiliary)
    library.exit()

    try:
        os.remove(f"{lib_db}.bak2")
    except FileNotFoundError:
        pass

if __name__ == "__main__":
    main()
