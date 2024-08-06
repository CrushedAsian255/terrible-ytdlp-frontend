from dbconnection import *
import downloader
from datatypes import *
import os
import time

zeroTag = TagNumID(0)

youtube_all_paths = [16+x for x in range(10)] + [63,13] + [65+x for x in range(26)] + [33+x for x in range(26)]

class Library:
    def __init__(self, db_filename: str, media_dir: str, max_resolution: int | None, print_db_log: bool):
        self.media_dir = media_dir
        self.db = Database(db_filename,print_db_log)
        self.closed = False
        self.online = True
        self.max_video_resolution = max_resolution

    def exit(self) -> None: self.db.exit()

    def get_all_filesystem_videos(self) -> list[VideoID]:
        start = time.perf_counter_ns()
        videos_list = []
        dir0list = [x for x in os.scandir(f"{self.media_dir}") if x.is_dir()]
        for dir0idx, dir0item in enumerate(dir0list):
            dir1list = [x for x in os.scandir(dir0item) if x.is_dir()]
            for dir1idx, dir1item in enumerate(dir1list):
                dir2list = [VideoID(x.name[:-4]) for x in os.scandir(dir1item) if not x.is_dir() and x.name[-4:]=='.mkv']
                videos_list += dir2list
            print(f"Enumerating directories: {dir0idx+1} / {len(dir0list)} ({len(videos_list)} items)",end="\r")
        end = time.perf_counter_ns()
        print(f"\nEnumerated {len(videos_list)} videos in {(end-start)/1_000_000_000:.2f} seconds")
        return videos_list

    def create_playlist_m3u8(self, pid: PlaylistID | None, invert: bool = False) -> str:
        if pid is None: return ""
        data = self.db.get_playlist_info(pid)
        if data is None: return ""
        m3ustring = "#EXTM3U\n#EXTENC:UTF-8\n"
        m3ustring += f"#PLAYLIST:{data.title}\n"
        for item in (list(reversed(data.entries)) if invert else data.entries):
            m3ustring+=f"#EXTINF:{item.duration},{item.title}\n{item.id.filename(self.media_dir)}\n"
        return m3ustring

    def add_tag_to_video(self, tag: TagID, vid: VideoID) -> None:
        self.db.add_tag_to_video(
            self.db.get_tnumid(tag),
            self.db.get_vnumid(vid)
        )
    
    def add_tag_to_playlist(self, tag: TagID, pid: PlaylistID) -> None:
        self.db.add_tag_to_playlist(
            self.db.get_tnumid(tag),
            self.db.get_pnumid(pid)
        )

    def get_all_videos(self, tag: TagID | None = None) -> list[VideoMetadata]:
        if tag: return self.db.get_videos([self.db.get_tnumid(tag)])
        else:   return self.db.get_videos()

    def get_all_single_videos(self, tag: TagID | None = None) -> list[VideoMetadata]:
        if tag: return self.db.get_videos([zeroTag,self.db.get_tnumid(tag)])
        else:   return self.db.get_videos([zeroTag])

    def get_all_videos_from_channel(self, cid: ChannelID) -> list[VideoMetadata]:
        return self.db.get_videos_from_channel(cid)

    def get_playlist_videos(self, pid: PlaylistID) -> list[VideoMetadata]:
        info = self.db.get_playlist_info(pid)
        if info is None: return []
        return info.entries

    def get_all_playlists(self, tag: TagID | None = None) -> list[PlaylistMetadata[int]]:
        if tag: return self.db.get_playlists([self.db.get_tnumid(tag)])
        else:   return self.db.get_playlists()

    def download_channel(self, cid: ChannelID, get_playlists: bool = False) -> None:
        self.download_playlist(PlaylistID(f"videos{cid}"))
        self.download_playlist(PlaylistID(f"shorts{cid}"))
        self.download_playlist(PlaylistID(f"streams{cid}"))
        
        if get_playlists:
            playlists = downloader.download_playlist_metadata(cid.playlists_url)
            if playlists is not None:
                playlist_count = len(playlists['entries'])
                for i in range(playlist_count):
                    self.download_playlist(playlists['entries'][i]['id'])

    def download_playlist(self, pid: PlaylistID) -> None:
        playlist_metadata = downloader.download_playlist_metadata(pid.url)
        if playlist_metadata is None: return None
        self.save_channel_info(ChannelID(playlist_metadata['uploader_id']))
        videos_ = [VideoID(x['id']) for x in playlist_metadata['entries']]
        videos = []
        for x in videos_:
            if x not in videos:
                videos.append(x)
        for v in videos: self.download_video(v,False)
        self.db.write_playlist_info(
            PlaylistMetadata(
                id=pid,
                title=playlist_metadata['title'],
                description=playlist_metadata['description'],
                channel=playlist_metadata['uploader_id'],
                channel_name=playlist_metadata['channel'],
                epoch=playlist_metadata['epoch'],
                entries=[v for v in videos if self.db.get_video_info(v)]
            )
        )
        self.db.add_tag_to_playlist(zeroTag, self.db.get_pnumid(pid))
    
    def download_video(self, vid: VideoID, add_tag: bool = True) -> None:
        db_entry = self.db.get_video_info(vid)
        if db_entry is None:
            video_metadata = downloader.download_video(self.media_dir, vid, self.max_video_resolution)
            if video_metadata is not None:
                if type(video_metadata['uploader_id']) != str or len(video_metadata['uploader_id'])==0 or video_metadata['uploader_id'][0] != "@":
                    if 'channel_id' in video_metadata and type(video_metadata['channel_id']) == str and video_metadata['channel_id'][0]=='U':
                        print("Returned channel UUID instead of handle, resolving...")
                        data = downloader.download_playlist_metadata(f"https://youtube.com/channel/{video_metadata['channel_id']}",True)
                        if data is None or 'uploader_id' not in data:
                            raise Exception(f"Fatal error: attempted to resolve channel UUID {video_metadata['channel_id']} failed")
                        else:
                            video_metadata['uploader_id'] = data['uploader_id']
                    else:
                        raise Exception("Fatal error: no handle or Channel UUID returned, cannot continue")
                self.save_channel_info(ChannelID(video_metadata['uploader_id']))
                self.db.write_video_info(VideoMetadata(
                    id=vid,
                    title=video_metadata['title'],
                    description=video_metadata['description'],
                    channel=ChannelID(video_metadata['uploader_id']),
                    channel_name=video_metadata['channel'],
                    upload_date=video_metadata['upload_date'],
                    duration=video_metadata['duration'],
                    epoch=video_metadata['epoch'],
                ))
                if add_tag: self.db.add_tag_to_video(zeroTag, self.db.get_vnumid(vid))
        else: self.save_channel_info(db_entry.channel)

    def save_channel_info(self, cid: ChannelID) -> None:
        db_entry = self.db.get_channel_info(cid)
        if db_entry is None:
            print(f"Downloading channel metadata: {cid}")
            data = downloader.download_playlist_metadata(cid.about_url,True)
            if data is None:
                raise Exception(f"Error: unable to get channel info from {cid}")
            self.db.write_channel_info(ChannelMetadata(
                id=cid,
                title=data['channel'],
                description=data['description'],
                epoch=data['epoch']
            ))