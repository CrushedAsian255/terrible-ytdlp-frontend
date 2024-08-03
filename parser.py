from library import Library
from downloader import get_file_name

import subprocess
import os

from shutil import get_terminal_size
from urwid.util import str_util

def trunc(s, max_len: int | None = 20):
    if max_len is None: return s
    out_chars = ""
    chars = 1
    for c in s:
        if chars > max_len: out_chars=out_chars[:-3]; out_chars += "..."; break
        out_chars += c
        chars += str_util.get_width(ord(c))
    return out_chars

def convert_duration(dur): return f"{int(dur/3600)}:{int(dur/60)%60:02d}:{dur%60:02d}"
def gen_temp_file_name(): return f"_tmp_{os.times().elapsed}.m3u"

def print_channel(channel_id: str, channel_title: str):
    return channel_id if channel_title == channel_id[1:] else f"{channel_id} ({channel_title})"

def run_command(lib: Library, command: str, params: list[str]):
    max_line_len = get_terminal_size((80,20))[0]-5

    def media_name(vid: str | None):
        return None if vid is None else get_file_name(lib.media_dir,vid)

    def get_videos_list_str(vids: list):
        return [
            f"{video.id} | {convert_duration(video.duration)} | {print_channel(video.channel, video.channel_name)}: {video.title}"
            for video in vids
        ]

    def get_playlist_videos_list_str(vids: list):
        return [
            f"{str(video.playlist_position+1).ljust(len(str(len(vids)+1)),' ')}: {video.id} | {convert_duration(video.duration)} | {print_channel(video.channel, video.channel_name)}: {video.title}"
            for video in vids
        ]

    def get_playlists_list_str(playlists: list):
        return [
            f"{playlist.id} | {playlist.entries} video(s) | {print_channel(playlist.channel, playlist.channel_name)}: {playlist.title}"
            for playlist in playlists
        ]

    def open_mpv(in_str: str | None):
        if in_str is None: return
        process = subprocess.Popen(['mpv', '--really-quiet', '--playlist=-'], stdin=subprocess.PIPE, text=True)
        if process is not None and process.stdin is not None:
            process.stdin.write(in_str)
            process.stdin.close()

    def write_to_temporary(in_str: str):
        name = gen_temp_file_name()
        with open(name, 'w') as f: f.write(in_str)
        return name

    def get_item_fzf(items_: list[str]):
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

    optional0=params[0] if len(params)>0 else None

    match command:
        case 'dv':      lib.download_video(params[0])
        case 'sv':     lib.download_video(params[0]); open_mpv(media_name(params[0]))
        case 'dp':      lib.download_playlist(params[0])
        case 'dc':      lib.download_channel(params[0],False)
        case 'dcp':     lib.download_channel(params[0],True)

        case 'tc':      lib.db.create_tag(params[0],params[1])
        case 'tav':     lib.db.add_tag_to_video(params[0],params[1])
        case 'tap':     lib.db.add_tag_to_playlist(params[0],params[1])

        case 'lv':      print('\n'.join([trunc(x,max_line_len) for x in get_videos_list_str(lib.get_all_videos(optional0))]))
        case 'lvs':     print('\n'.join([trunc(x, max_line_len) for x in get_videos_list_str(lib.get_all_single_videos(optional0))]))
        case 'lcv':     print('\n'.join([trunc(x, max_line_len) for x in get_videos_list_str(lib.get_all_videos_from_channel(optional0))]))
        case 'lp':      print('\n'.join([trunc(x,max_line_len) for x in get_playlists_list_str(lib.get_all_playlists(optional0))]))
        case 'lpv':     print('\n'.join([trunc(x,max_line_len) for x in get_playlist_videos_list_str(lib.get_playlist_videos(optional0))]))

        
        case 'pv':      open_mpv(media_name(params[0]))
        case 'xv':      print(media_name(params[0]))
        case 'plv':     open_mpv(media_name(get_item_fzf(get_videos_list_str(lib.get_all_videos(optional0)))))
        case 'xlv':     print(media_name(get_item_fzf(get_videos_list_str(lib.get_all_videos(optional0)))))
        case 'plvs':    open_mpv(media_name(get_item_fzf(get_videos_list_str(lib.get_all_single_videos(optional0)))))
        case 'xlvs':    print(media_name(get_item_fzf(get_videos_list_str(lib.get_all_single_videos(optional0)))))

        case 'pp':      open_mpv(lib.create_playlist_m3u8(params[0]))
        case 'fp':      print(write_to_temporary(lib.create_playlist_m3u8(params[0])))
        case 'xp':      print(lib.create_playlist_m3u8(params[0]))
        case 'plp':     open_mpv(lib.create_playlist_m3u8(get_item_fzf(get_playlists_list_str(lib.get_all_playlists(optional0)))))
        case 'xlp':     print(lib.create_playlist_m3u8(get_item_fzf(get_playlists_list_str(lib.get_all_playlists(optional0)))))
        
        case 'ppr':     open_mpv(lib.create_playlist_m3u8(params[0],True))
        case 'fpr':     print(write_to_temporary(lib.create_playlist_m3u8(params[0],True)))
        case 'fpr':     print(lib.create_playlist_m3u8(params[0],True))
        case 'plpr':    open_mpv(lib.create_playlist_m3u8(get_item_fzf(get_playlists_list_str(lib.get_all_playlists(optional0))),True))
        case 'xlpr':    print(lib.create_playlist_m3u8(get_item_fzf(get_playlists_list_str(lib.get_all_playlists(optional0))),True))

        case 'prune':
            videos_filesystem = [f[:-4] for f in [f0 for f1 in [f3[2] for f3 in os.walk(lib.media_dir)] for f0 in f1] if f[-4:] == ".mkv"]
            videos_database = [x.id for x in lib.get_all_videos()]

            for fs_vid in videos_filesystem:
                if fs_vid not in videos_database:
                    print(f"Orphaned file: {get_file_name("",fs_vid)}")

            for db_vid in videos_database:
                if db_vid not in videos_filesystem:
                    print(f"ERROR: Missing file: {get_file_name("",db_vid)}")
                
                video_tags = lib.db.get_video_tags(db_vid)
                video_playlists = [x[0] for x in lib.db.get_video_playlists(db_vid)]
                if len(video_tags) == 0 and len(video_playlists) == 0:
                    print(f"Orphaned video: {db_vid}")

        case _: print(f"Unknown command: {command}")