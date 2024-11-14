import os
import shutil
import subprocess
from typing import Any, Union

from argparse import ArgumentParser

import re
from library import Library
from datatypes import VideoID, PlaylistID
from datatypes import ChannelHandle, ChannelUUID, TagID
from datatypes import VideoMetadata, PlaylistMetadata
from media_filesystem import *

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
    try:
        return PlaylistID(x)
    except ValueError:
        pass
    try:
        return VideoID(x)
    except ValueError:
        pass
    raise ValueError(x)

def parse_custom_media_fs(media_handle: str, library_path: str) -> MediaFilesystem:
    split_handle = media_handle.split(":")
    match split_handle[0]:
        case "s3":
            if len(split_handle) > 2:
                return AWSFilesystem(library_path,split_handle[1],split_handle[2])
            return AWSFilesystem(library_path,split_handle[1],None)
        case _:
            return LocalFilesystem(media_handle)

    return LocalFilesystem(media_handle)

def parse_command(
    lib: Library,
    command: str,
    url: str | None,
    auxiliary: bool,
    tag: TagID | None
) -> None:
    def infer_type(url: str) -> Union[VideoID,PlaylistID,ChannelUUID,ChannelHandle]:
        re_match = re.match(
            r"(?:https?:\/\/)?(?:www.)?(?:youtube(?:education)?.com(?:\.[a-z]+)?\/"
            r"(?:watch\?v=|shorts\/|playlist\?list=|(?=@))|youtu.be\/)(@?[0-9a-zA-Z-_]+)(?:\/(videos|shorts))?",
            url
        )
        value = re_match.groups()[0] if re_match else url
        try:
            channel_handle = ChannelHandle(value)
            if re_match and re_match.groups()[1]:
                uuid=lib.convert_handle_to_uuid(channel_handle).value
                print(PlaylistID(f"${uuid}.{re_match.groups()[1]}"))
                return PlaylistID(f"${uuid}.{re_match.groups()[1]}")
            return channel_handle
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
        try:
            return ChannelUUID(value)
        except ValueError:
            pass
        raise ValueError(f"Unable to determine the format of {value}")
    content_id = None
    lib.write_log("command",f"{command} {url}")
    match command:
        case 'download' | 'dl':
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
                        open_mpv(lib.media_fs.get_video_url(content_id,False))
                case PlaylistID():
                    lib.download_playlist(content_id)
                    if tag:
                        lib.add_tag(tag,content_id)
                case _:
                    lib.download_channel(content_id,not auxiliary)
        case 'new-tag':
            if tag is None:
                print("Error: A tag is required (pass with -t <tag>)")
                return
            lib.create_tag(tag,(url or ""))
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
                case ChannelHandle() | ChannelUUID():
                    print("Error: Cannot tag channel")
        case 'play':
            if url:
                content_id = infer_type(url)
                if isinstance(content_id,ChannelHandle):
                    content_id = lib.convert_handle_to_uuid(content_id)
                if isinstance(content_id,ChannelUUID):
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
                    open_mpv(lib.media_fs.get_video_url(content_id,True))
                case PlaylistID():
                    open_mpv(lib.create_playlist_m3u8(content_id,auxiliary))
        case 'play-v':
            if url:
                content_id = infer_type(url)
                if isinstance(content_id,ChannelHandle):
                    content_id = lib.convert_handle_to_uuid(content_id)
                if isinstance(content_id,PlaylistID):
                    raise NotImplementedError("Not implemented")
                if isinstance(content_id,ChannelUUID):
                    content_id = pick_video_fzf(lib.get_all_videos_from_channel(content_id))
            else:
                content_id = pick_video_fzf(lib.get_all_videos(tag))
            if content_id is not None:
                if auxiliary:
                    print(lib.media_fs.get_video_url(content_id,False))
                else:
                    open_mpv(lib.media_fs.get_video_url(content_id,False))
        case 'play-pl':
            if url:
                content_id = infer_type(url)
                if isinstance(content_id,ChannelHandle):
                    content_id = lib.convert_handle_to_uuid(content_id)
                if isinstance(content_id,VideoID):
                    raise NotImplementedError("Not implemented")
                if isinstance(content_id,ChannelUUID):
                    content_id = pick_playlist_fzf(lib.get_all_playlists_from_channel(content_id))
            else:
                content_id = pick_playlist_fzf(lib.get_all_playlists(tag))
            open_mpv(lib.create_playlist_m3u8(content_id,auxiliary))
        case 'check':
            lib.integrity_check()
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
        dest="media_handle"
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
    if args.max_resolution and args.library == "master":
        print("Error: cannot use master db with resolution caps")
        return
    if args.max_resolution:
        args.library = f"{args.library}.{args.max_resolution}"
    library_path = f"{bpath}/{args.library}"
    db_path = f"{library_path}.db"
    try_copy(f"{db_path}.bak", f"{db_path}.bak2")
    
    custom_media_handle: str | None = None
    try:
        with open(f"{library_path}.ext","r", encoding="utf-8") as f:
            custom_media_handle = f.read()
    except (FileNotFoundError, IsADirectoryError):
        pass

    if args.media_handle:
        custom_media_handle = args.media_handle
        with open(f"{library_path}.ext","w", encoding="utf-8") as f:
            f.write(custom_media_handle)

    if custom_media_handle:
        media_fs = parse_custom_media_fs(custom_media_handle,library_path)
    else:
        media_fs = LocalFilesystem(library_path)

    try_copy(f"{db_path}.bak", f"{db_path}.bak2")
    try_copy(db_path, f"{db_path}.bak")

    try:
        library = Library(library_path,media_fs,args.max_resolution,args.print_db_log)
    except IOError as e:
        try_copy(f"{db_path}.bak2", f"{db_path}.bak")
        print(f"Error loading database!\n{e}\nAttempting to revert to backup")
        shutil.copy(db_path, f"{db_path}.err")
        if not try_copy(f"{db_path}.bak", db_path):
            print("Sorry, no backup could be found")
            return
        try:
            library = Library(library_path,media_fs,args.max_resolution,args.print_db_log)
        except IOError as e2:
            print(f"Uh oh...\n{e2}\nBackup is also corrupted or none exists. Sorry mate :(")
            return
        print(f"Backup loaded successfully, corrupted version stored as {db_path}.err")

    tag: TagID | None = None
    if args.tag:
        tag = TagID(args.tag)

    parse_command(library, args.command, args.url, args.auxiliary, tag)
    library.exit()

    try:
        os.remove(f"{db_path}.bak2")
    except FileNotFoundError:
        pass

if __name__ == "__main__":
    main()
