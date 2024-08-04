from library import Library
from downloader import convert_file_size
from datatypes import *
import subprocess
import os
from dbconnection import *

import re

yt_url_regex = r"(?:https?:\/\/)?(?:www.)?youtube.com(?:\.[a-z]+)?\/(?:watch\?v=|playlist\?list=|(?=@))(@?[0-9a-zA-Z-_]+)"

def convert_duration(dur: int) -> str: return f"{int(dur/3600)}:{int(dur/60)%60:02d}:{dur%60:02d}"

def print_channel(channel_id: str, channel_title: str) -> str:
    return channel_id if channel_title == channel_id[1:] else f"{channel_id} ({channel_title})"

def run_command(lib: Library, command: str, params: list[str]) -> None:
    def media_name(vid: str | None) -> str | None:
        return None if vid is None else VideoID(vid).filename(lib.media_dir)

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

    def get_playlists_list_str(playlists: list[PlaylistMetadataVCountWithChannelName]) -> list[str]:
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

    optional0: str | None = params[0] if len(params)>0 else None
    
    for x in range(len(params)):
        matched = re.match(yt_url_regex,params[x])
        if matched:
            params[x]=matched.groups()[0]

    match command:
        case 'dv':      lib.download_video(params[0])
        case 'sv':     lib.download_video(params[0]); open_mpv(media_name(params[0]))
        case 'dp':      lib.download_playlist(params[0])
        case 'dc':      lib.download_channel(params[0],False)
        case 'dcp':     lib.download_channel(params[0],True)

        case 'tc':      lib.db.create_tag(params[0],params[1])
        case 'tav':     lib.add_tag_to_video(params[0],params[1])
        case 'tap':     lib.add_tag_to_playlist(params[0],params[1])

        case 'lv':      print('\n'.join(get_videos_list_str(lib.get_all_videos(optional0))))
        case 'lvs':     print('\n'.join(get_videos_list_str(lib.get_all_single_videos(optional0))))
        case 'lcv':     print('\n'.join(get_videos_list_str(lib.get_all_videos_from_channel(ChannelID(params[0])))))
        case 'lp':      print('\n'.join(get_playlists_list_str(lib.get_all_playlists(optional0))))
        case 'lpv':     print('\n'.join(get_playlist_videos_list_str(lib.get_playlist_videos(params[0]))))

        
        case 'pv':      open_mpv(media_name(params[0]))
        case 'xv':      print(media_name(params[0]))
        case 'plv':     open_mpv(media_name(get_item_fzf(get_videos_list_str(lib.get_all_videos(optional0)))))
        case 'xlv':     print(media_name(get_item_fzf(get_videos_list_str(lib.get_all_videos(optional0)))))
        case 'plvs':    open_mpv(media_name(get_item_fzf(get_videos_list_str(lib.get_all_single_videos(optional0)))))
        case 'xlvs':    print(media_name(get_item_fzf(get_videos_list_str(lib.get_all_single_videos(optional0)))))

        case 'pp':      open_mpv(lib.create_playlist_m3u8(params[0]))
        case 'xp':      print(lib.create_playlist_m3u8(params[0]))
        case 'plp':     open_mpv(lib.create_playlist_m3u8(get_item_fzf(get_playlists_list_str(lib.get_all_playlists(optional0)))))
        case 'xlp':     print(lib.create_playlist_m3u8(get_item_fzf(get_playlists_list_str(lib.get_all_playlists(optional0)))))
        
        case 'ppr':     open_mpv(lib.create_playlist_m3u8(params[0],True))
        case 'fpr':     print(lib.create_playlist_m3u8(params[0],True))
        case 'plpr':    open_mpv(lib.create_playlist_m3u8(get_item_fzf(get_playlists_list_str(lib.get_all_playlists(optional0))),True))
        case 'xlpr':    print(lib.create_playlist_m3u8(get_item_fzf(get_playlists_list_str(lib.get_all_playlists(optional0))),True))

        case 'prune':
            videos_filesystem = [f[:-4] for f in [f0 for f1 in [f3[2] for f3 in os.walk(lib.media_dir)] for f0 in f1] if f[-4:] == ".mkv"]
            videos_database = [x.id for x in lib.get_all_videos()]

            for fs_vid in videos_filesystem:
                if fs_vid not in videos_database:
                    print(f"Orphaned file: {VideoID(fs_vid).filename()} | {convert_file_size(os.path.getsize(VideoID(fs_vid).filename(lib.media_dir)))}")

            for db_vid in videos_database:
                if db_vid not in videos_filesystem:
                    print(f"ERROR: Missing file: {VideoID(db_vid).filename()}")
                
                video_tags = len(lib.db.get_video_tags(db_vid))
                video_playlists = len(lib.db.get_video_playlists(db_vid))
                if video_tags == 0 and video_playlists == 0:
                    print(f"Orphaned video: {db_vid} | {convert_file_size(os.path.getsize(VideoID(db_vid).filename(lib.media_dir)))}")

        case 'prune-v':
            videos_database = [x.id for x in lib.get_all_single_videos()]
            video_sizes = []
            for vid in videos_database:
                if len(lib.db.get_video_playlists(vid)) == 0:
                    try: video_sizes.append((vid,os.path.getsize(VideoID(vid).filename(lib.media_dir))))
                    except FileNotFoundError: print(f"ERROR: Missing file: {VideoID(db_vid).filename()}")
            maximum = 5
            if optional0 is not None:
                try: maximum = int(optional0)
                except ValueError: pass
            for vid, size in video_sizes[maximum:0:-1]:
                print(f"{vid} | {convert_file_size(size)}")
        case 'prune-p':
            playlists = [x.id for x in lib.get_all_playlists()]
            playlist_sizes = []
            for idx1, pid in enumerate(playlists):
                size = 0
                for vid in [x.id for x in lib.get_playlist_videos(pid)]:
                    video_tags = len(lib.db.get_video_tags(vid))
                    video_playlists = len(lib.db.get_video_playlists(vid))
                    if video_playlists == 0: print("This should not be possible")
                    if video_tags == 0 and video_playlists == 1:
                        try: size += os.path.getsize(VideoID(vid).filename(lib.media_dir))
                        except FileNotFoundError: print(f"ERROR: Missing file: {VideoID(vid).filename()}")
                if size != 0: playlist_sizes.append((pid,size))
                print(f"Enumerating... ({idx1+1}/{len(playlists)})",end="\r")
            print()
                    
            playlist_sizes.sort(key=lambda x: -x[1])
            maximum = 5
            if optional0 is not None:
                try: maximum = int(optional0)
                except ValueError: pass
            for pid, size in playlist_sizes[maximum:0:-1]:
                print(f"{pid} | {convert_file_size(size)}")

        case _: print(f"Unknown command: {command}")