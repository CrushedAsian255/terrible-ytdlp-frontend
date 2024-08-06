from library import Library
from downloader import convert_file_size
from datatypes import *
import subprocess
import os
from dbconnection import *
from typing import Any, Union
import re

yt_url_regex = r"(?:https?:\/\/)?(?:www.)?youtube.com(?:\.[a-z]+)?\/(?:watch\?v=|playlist\?list=|(?=@))(@?[0-9a-zA-Z-_]+)"

def convert_duration(dur: int) -> str: return f"{int(dur/3600)}:{int(dur/60)%60:02d}:{dur%60:02d}"

def print_channel(channel_id: ChannelID, channel_title: str) -> str:
    return f"{channel_id} ({channel_title})"

InferredID=Union[VideoID,PlaylistID,ChannelID]
def infer_type(url: str) -> InferredID:
    re_match = re.match(yt_url_regex,url)
    value = re_match.groups()[0] if re_match else url
    try:   return ChannelID(value)
    except ValueError: pass
    try:   return PlaylistID(value)
    except ValueError: pass
    try:   return VideoID(value)
    except ValueError: pass
    raise ValueError(f"Unable to determine the format of {value}")

def run_command(lib: Library, command: str, params: list[str], auxiliary: bool = False) -> None:
    def fname(vid: VideoID) -> str: return vid.filename(lib.media_dir)

    def get_videos_list_str(vids: list[VideoMetadata]) -> list[str]:
        return [
            f"{video.id} | {convert_duration(video.duration)} | {print_channel(video.channel, video.channel_name)}: {video.title}"
            for video in vids
        ]

    def get_playlist_videos_list_str(vids: list[VideoMetadata]) -> list[str]:
        return [
            f"{str(index+1).ljust(len(str(len(vids)+1)),' ')}: {video.id} | {convert_duration(video.duration)} | {print_channel(video.channel, video.channel_name)}: {video.title}"
            for index, video in enumerate(vids)
        ]

    def get_playlists_list_str(playlists: list[PlaylistMetadata[int]]) -> list[str]:
        return [
            f"{playlist.id} | {playlist.entries} video(s) | {print_channel(playlist.channel, playlist.channel_name)}: {playlist.title}"
            for playlist in playlists
        ]

    def open_mpv(in_str: str | None) -> None:
        if in_str is None: return
        process = subprocess.Popen(['mpv', '--really-quiet', '--playlist=-'], stdin=subprocess.PIPE, text=True)
        if process is not None and process.stdin is not None:
            process.stdin.write(in_str)
            process.stdin.close()

    def get_item_fzf(items_: list[str]) -> str | None:
        items = "\n".join(items_)
        process = subprocess.Popen(['fzf'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        if process is not None and process.stdin is not None:
            process.stdin.write(items)
            process.stdin.close()
            if process.stdout is None:
                return None
            out_str = process.stdout.readline().split(" | ")[0]
            if len(out_str)==0: return None
            return out_str
        return None

    def pick_video_fzf(videos: list[VideoMetadata]) -> VideoID | None:
        x = get_item_fzf(get_videos_list_str(videos))
        if x is None: return None
        return VideoID(x)

    def pick_playlist_fzf(playlists: list[PlaylistMetadata[Any]]) -> PlaylistID | None:
        x = get_item_fzf(get_playlists_list_str(playlists))
        if x is None: return None
        return PlaylistID(x)

    def media_path(vid: VideoID | None) -> str | None:
        if vid is None: return None
        return fname(vid)

    optional0: str | None = params[0] if len(params)>0 else None
    givenTag: TagID | None = None
    try:               givenTag=TagID(optional0)
    except ValueError: pass

    for x in range(len(params)):
        matched = re.match(yt_url_regex,params[x])
        if matched:
            params[x]=matched.groups()[0]

    commands_helptext: list[tuple[str,list[str],str,list[str]|str|None]]=[
        ("help"     , []             , "Show this help"                                  , None),
        ("download" , ["url"]        , "Downloads the content <url>"                     , ["Video: play after downloading","Channel: Don't download channel playlists"]),
        ("new-tag"  , ["tid","text"] , "Create new tag <tid> with desciption <text>"     , None),
        ("add-tag"  , ["tid","url"]  , "Add tag <tid> to <url>"                          , None),
        ("play"     , []             , "Pick and then play a video"                      , ["Write filename to stdout"]),
        ("play"     , ["tid"]        , "Pick and then play a video with <tag>"           , ["Write filename to stdout"]),
        ("play"     , ["vid"]        , "Play a video"                                    , ["Write filename to stdout"]),
        ("play-pl"  , []             , "Pick and then play a playlist"                   , ["Play in reverse order"]),
        ("play-pl"  , ["tid"]        , "Pick and then play a playlist with <tag>"        , ["Play in reverse order"]),
        ("play-pl"  , ["pid"]        , "Play a playlist"                                 , ["Play in reverse order"]),
        ("check"    , []             , "Check database and filesystem for orphans"       , None),
        ("prune"    , []             , "Remove orphaned videos from database"            , None),
        ("purge"    , []             , "Delete orphaned videos from the filesystem"      , None),
        ("size-v"   , ["max"]        , "Find the largest untagged videos"                , None),
        ("size-p"   , ["max"]        , "Find the playlist with largest possible savings" , None)
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
                case ChannelID():
                    print("Error: cannot add tag to channel")

        case 'play':
            video_id = None
            if len(params) == 0: 
                video_id = pick_video_fzf(lib.get_all_videos())
            else:
                try:
                    video_id = VideoID(params[0])
                except ValueError:
                    try:
                        video_id = pick_video_fzf(lib.get_all_videos(TagID(params[0])))
                    except ValueError:
                        video_id = pick_video_fzf(lib.get_all_videos())
            if video_id is not None:
                if auxiliary: print(fname(video_id))
                else:         open_mpv(fname(video_id))
        
        case 'play-pl':
            playlist_id = None
            if len(params) == 0: 
                playlist_id = pick_playlist_fzf(lib.get_all_playlists())
            else:
                try:
                    playlist_id = PlaylistID(params[0])
                except ValueError:
                    try:
                        playlist_id = pick_playlist_fzf(lib.get_all_playlists(TagID(params[0])))
                    except ValueError:
                        playlist_id = pick_playlist_fzf(lib.get_all_playlists())
            if playlist_id is not None:
                open_mpv(lib.create_playlist_m3u8(playlist_id,auxiliary))

        case 'check':
            videos_filesystem: list[VideoID] = lib.get_all_filesystem_videos()
            videos_database: list[VideoID] = [x.id for x in lib.get_all_videos()]

            total_size=0
            for fs_vid in videos_filesystem:
                if fs_vid not in videos_database:
                    size = os.path.getsize(fname(fs_vid))
                    total_size += size
                    print(f"Orphaned file: {fs_vid.filename()} | {convert_file_size(size)}")
            if total_size > 0: print(f"Total orphaned file size: {convert_file_size(total_size)}")
            
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
            if total_size > 0: print(f"Total orphaned video size: {convert_file_size(total_size)}")
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
            videos_database: list[VideoID] = [x.id for x in lib.get_all_videos()]

            total_size=0
            for fs_vid in lib.get_all_filesystem_videos():
                if fs_vid not in videos_database:
                    size = os.path.getsize(fname(fs_vid))
                    total_size += size
                    os.remove(fname(fs_vid))
            if total_size > 0: print(f"Removed file count: {convert_file_size(total_size)}")

        case 'size-v':
            videos_database = [x.id for x in lib.get_all_single_videos()]
            video_sizes = []
            for vid in videos_database:
                if len(lib.db.get_video_playlists(vid)) == 0:
                    try: video_sizes.append((vid,os.path.getsize(fname(vid))))
                    except FileNotFoundError: print(f"ERROR: Missing file: {db_vid.fileloc}")
            
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
                    if video_playlists == 0: print("This should not be possible")
                    if video_tags == 0 and video_playlists == 1:
                        try: size += os.path.getsize(fname(vid))
                        except FileNotFoundError: print(f"ERROR: Missing file: {vid.filename()}")
                if size != 0: playlist_sizes.append((pid,size))
                print(f"Enumerating... ({idx1+1}/{len(playlists)})",end="\r")
            print()
                    
            playlist_sizes.sort(key=lambda x: -x[1])
            for pid, size in playlist_sizes[int(params[0]):0:-1]:
                print(f"{pid} | {convert_file_size(size)}")