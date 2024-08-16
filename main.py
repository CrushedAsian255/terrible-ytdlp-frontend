import os
import shutil
import subprocess
import re
from typing import Any, Union

from argparse import ArgumentParser

from library import Library
from datatypes import *

InferredID=Union[VideoID,PlaylistID,ChannelID,TagID]
def infer_type(url: str) -> InferredID:
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
    process = subprocess.Popen(
        ['fzf'],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    if process is not None and process.stdin is not None:
        process.stdin.write("\n".join(items))
        process.stdin.close()
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
    process = subprocess.Popen(
        ['mpv', '--really-quiet', '--playlist=-'],
        stdin=subprocess.PIPE, text=True
    )
    if process is not None and process.stdin is not None:
        process.stdin.write(in_str)
        process.stdin.close()

def parse_command(lib: Library, command: str, params: list[str], auxiliary: bool = False) -> None:
    def fname(vid: VideoID) -> str:
        return vid.filename(lib.media_dir)

    def get_videos_list_str(vids: list[VideoMetadata]) -> list[str]:
        return [
            video.to_string()
            for video in vids
        ]

    def get_playlists_list_str(playlists: list[PlaylistMetadata[int]]) -> list[str]:
        return [
            playlist.to_string()
            for playlist in playlists
        ]

    def pick_video_fzf(videos: list[VideoMetadata]) -> VideoID | None:
        x = get_item_fzf(get_videos_list_str(videos))
        if x is None:
            return None
        return VideoID(x)

    def pick_playlist_fzf(playlists: list[PlaylistMetadata[Any]]) -> PlaylistID | None:
        x = get_item_fzf(get_playlists_list_str(playlists))
        if x is None:
            return None
        return PlaylistID(x)

    commands_helptext: list[tuple[str,list[str],str,list[str]|str|None]]=[
        ("help"     , []             , "Show this help"                   , None),
        ("download" , ["url"]        , "Downloads the content <url>"      , ["Video: play after downloading","Channel: Don't download channel playlists"]),
        ("new-tag"  , ["tid","text"] , "Create new tag"                   , None),
        ("add-tag"  , ["tid","url"]  , "Add tag <tid> to <url>"           , None),
        ("play"     , []             , "Pick and then play something"     , ["Write filename to stdout"]),
        ("play-v"   , []             , "Pick and then play a video"       , ["Write filename to stdout"]),
        ("play-pl"  , []             , "Pick and then play a playlist"    , ["Play in reverse order"]),
        ("check"    , []             , "Check for orphaned content"       , None),
        ("prune"    , []             , "Remove orphans from the database" , None),
        ("purge"    , []             , "Delete orphaned video files"      , None),
        ("size-v"   , ["max"]        , "Find the largest untagged videos" , None),
        ("size-p"   , ["max"]        , "Find the largest playlists"       , None)
    ]

    command_found = False
    for cmd,args,_,_ in commands_helptext:
        if cmd == command:
            command_found = True
            if len(params) < len(args):
                print(f"Usage: {cmd} {" ".join([f"<{arg}>" for arg in args])}")
                return
            break

    if not command_found:
        print(f"Unknown command: {command}\nUse 'help' for help")
        return

    match command:
        case 'help':
            for cmd,args,desc,alt in commands_helptext:
                print(f"{cmd} {" ".join([f"<{arg}>" for arg in args])}\n{desc}")
                if alt:
                    print("Auxiliary function (-a):")
                    for altline in alt:
                        print(f"\t{altline}")
                print()
        case 'download':
            url = infer_type(params[0])
            match url:
                case VideoID():
                    lib.download_video(url)
                    if auxiliary:
                        open_mpv(fname(url))
                case PlaylistID():
                    lib.download_playlist(url)
                case ChannelID():
                    lib.download_channel(url,not auxiliary)
                case TagID():
                    print("Error: Invalid input")
        case 'new-tag':
            lib.db.create_tag(TagID(params[0]),params[1])
        case 'add-tag':
            tag = TagID(params[0])
            url = infer_type(params[1])
            match url:
                case VideoID():
                    lib.add_tag_to_video(tag,url)
                case PlaylistID():
                    lib.add_tag_to_playlist(tag,url)
                case ChannelID() | TagID():
                    print("Error: Invalid input")
        case 'play':
            content_id: InferredID | None = None
            try:
                content_id = infer_type(params[0])
                match content_id:
                    case VideoID() | PlaylistID():
                        pass
                    case TagID():
                        item_id = get_item_fzf(
                            get_videos_list_str(lib.get_all_videos(content_id))+
                            get_playlists_list_str(lib.get_all_playlists(content_id))
                        )
                        if item_id is not None:
                            content_id = infer_type(item_id)
                    case ChannelID():
                        item_id = get_item_fzf(
                            get_videos_list_str(lib.get_all_videos_from_channel(content_id))+
                            get_playlists_list_str(lib.get_all_playlists_from_channel(content_id))
                        )
                        if item_id is not None:
                            content_id = infer_type(item_id)
            except (ValueError, IndexError):
                item_id = get_item_fzf(
                    get_videos_list_str(lib.get_all_videos())+
                    get_playlists_list_str(lib.get_all_playlists())
                )
                if item_id is not None:
                    content_id = infer_type(item_id)
            match content_id:
                case VideoID():
                    open_mpv(fname(content_id))
                case PlaylistID():
                    open_mpv(lib.create_playlist_m3u8(content_id,auxiliary))
                case None:
                    pass
                case _:
                    raise ValueError(content_id)
        case 'play-v':
            content_id = None
            try:
                content_id = infer_type(params[0])
                match content_id:
                    case VideoID():
                        pass
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
            content_id = None
            try:
                content_id = infer_type(params[0])
                match content_id:
                    case PlaylistID():
                        pass
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
            if content_id is not None:
                open_mpv(lib.create_playlist_m3u8(content_id,auxiliary))
        case 'check':
            videos_filesystem: list[VideoID] = lib.get_all_filesystem_videos()
            videos_database: list[VideoID] = [x.id for x in lib.get_all_videos()]
            total_size=0
            for fs_vid in videos_filesystem:
                if fs_vid not in videos_database:
                    size = os.path.getsize(fname(fs_vid))
                    total_size += size
                    print(f"Orphaned file: {fs_vid.filename()} | {convert_file_size(size)}")
            if total_size > 0:
                print(f"Total orphaned file size: {convert_file_size(total_size)}")
            total_size=0
            for db_vid in videos_database:
                if db_vid not in videos_filesystem:
                    print(f"ERROR: Missing file: {db_vid.fileloc}")
                video_tags = len(lib.db.get_video_tags(db_vid))
                video_playlists = len(lib.db.get_video_playlists(db_vid))
                if video_tags == 0 and video_playlists == 0:
                    size = os.path.getsize(fname(db_vid))
                    total_size += size
                    print(f"Orphaned video: {db_vid} | {convert_file_size(size)}")
            if total_size > 0:
                print(f"Total orphaned video size: {convert_file_size(total_size)}")
        case 'prune':
            videos_to_remove: list[VideoID] = []
            for db_vid in [x.id for x in lib.get_all_videos()]:
                video_tags = len(lib.db.get_video_tags(db_vid))
                video_playlists = len(lib.db.get_video_playlists(db_vid))
                if video_tags == 0 and video_playlists == 0:
                    print(f"Removing orphaned video: {db_vid}")
                    videos_to_remove.append(db_vid)
            print("Removing... ")
            lib.db.remove_videos(videos_to_remove)
        case 'purge':
            videos_database = [x.id for x in lib.get_all_videos()]
            total_size=0
            for fs_vid in lib.get_all_filesystem_videos():
                if fs_vid not in videos_database:
                    size = os.path.getsize(fname(fs_vid))
                    total_size += size
                    os.remove(fname(fs_vid))
            if total_size > 0:
                print(f"Removed file count: {convert_file_size(total_size)}")
        case 'size-v':
            videos_database = [x.id for x in lib.get_all_single_videos()]
            video_sizes = []
            for vid in videos_database:
                if len(lib.db.get_video_playlists(vid)) == 0:
                    try:
                        video_sizes.append((vid,os.path.getsize(fname(vid))))
                    except FileNotFoundError:
                        print(f"ERROR: Missing file: {db_vid.fileloc}")
            video_sizes.sort(key=lambda x: -x[1])
            for vid, size in video_sizes[int(params[0]):0:-1]:
                print(f"{vid} | {convert_file_size(size)}")
        case 'size-p':
            playlists = [x.id for x in lib.get_all_playlists()]
            playlist_sizes = []
            for idx1, pid in enumerate(playlists):
                size = 0
                for vid in [x.id for x in lib.get_playlist_videos(pid)]:
                    video_tags = len(lib.db.get_video_tags(vid))
                    video_playlists = len(lib.db.get_video_playlists(vid))
                    if video_playlists == 0:
                        print("This should not be possible")
                    if video_tags == 0 and video_playlists == 1:
                        try:
                            size += os.path.getsize(fname(vid))
                        except FileNotFoundError:
                            print(f"ERROR: Missing file: {vid.filename()}")
                if size != 0:
                    playlist_sizes.append((pid,size))
                print(f"Enumerating... ({idx1+1}/{len(playlists)})",end="\r")
            print()

            playlist_sizes.sort(key=lambda x: -x[1])
            for pid, size in playlist_sizes[int(params[0]):0:-1]:
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
        "-q", help="Quiet mode",
        dest="quiet", action='store_true'
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

    library = None

    try:
        library = Library(lib_db,media_dir,args.max_resolution,args.print_db_log)
    except Exception as e:
        try_copy(f"{lib_db}.bak2", f"{lib_db}.bak")
        print("Error loading database!")
        print(e)
        print("Attempting to revert to backup")
        shutil.copy(lib_db, f"{lib_db}.err")
        if not try_copy(f"{lib_db}.bak", lib_db):
            print("Sorry, no backup could be found")
            return
        try:
            library = Library(lib_db,media_dir,args.max_resolution,args.print_db_log)
        except Exception as e2:
            print("Uh oh...")
            print(e2)
            print("Backup either doesn't exist or is also corrupted, sorry mate :(")
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
