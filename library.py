from dbconnection import *
import downloader
from datatypes import *

class Library:
    def __init__(self, db_filename: str, media_dir: str, max_resolution: int | None, print_db_log: bool):
        self.media_dir = media_dir
        self.db = Database(db_filename,print_db_log)
        self.closed = False
        self.online = True
        self.max_video_resolution = max_resolution

    def exit(self) -> None: self.db.exit()

    def create_playlist_m3u8(self, pid: str | None, invert: bool = False) -> str:
        if pid is None: return ""
        data = self.db.get_playlist_info(pid)
        if data is None: return ""
        m3ustring = "#EXTM3U\n#EXTENC:UTF-8\n"
        m3ustring += f"#PLAYLIST:{data.title}\n"
        for item in (list(reversed(data.entries)) if invert else data.entries):
            m3ustring+=f"#EXTINF:{item.duration},{item.title}\n{VideoID(item.id).filename(self.media_dir)}\n"
        return m3ustring

    def add_tag_to_video(self, tag: str, vid: str) -> None:
        self.db.add_tag_to_video(
            self.db.get_tnumid(tag),
            self.db.get_vnumid(vid)
        )
    
    def add_tag_to_playlist(self, tag: str, pid: str) -> None:
        self.db.add_tag_to_playlist(
            self.db.get_tnumid(tag),
            self.db.get_pnumid(pid)
        )

    def get_all_videos(self, tag: str | None = None) -> list[VideoMetadataWithChannelName]:
        if tag: return self.db.get_videos(self.db.get_tnumid(tag))
        else:   return self.db.get_videos()

    def get_all_single_videos(self, tag: str | None = None) -> list[VideoMetadataWithChannelName]:
        if tag: return self.db.get_videos([0,self.db.get_tnumid(tag)])
        else:   return self.db.get_videos(0)

    def get_all_videos_from_channel(self, cid: ChannelID) -> list[VideoMetadataWithChannelName]:
        return self.db.get_videos_from_channel(cid)

    def get_playlist_videos(self, pid: str) -> list[VideoMetadataWithIndexAndChannelName]:
        info = self.db.get_playlist_info(pid)
        if info is None: return []
        return info.entries

    def get_all_playlists(self, tag: str | None = None) -> list[PlaylistMetadataVCountWithChannelName]:
        if tag: return self.db.get_playlists(self.db.get_tnumid(tag))
        else:   return self.db.get_playlists()

    def download_channel(self, cid: str, get_playlists: bool = False) -> None:
        if cid[0] != "@": raise Exception("Invalid CID")

        self.download_playlist(f"videos{cid}")
        self.download_playlist(f"shorts{cid}")
        self.download_playlist(f"streams{cid}")
        
        if get_playlists:
            playlists = downloader.download_playlist_metadata(PlaylistID(f"playlists{cid}").url)
            if playlists is not None:
                playlist_count = len(playlists['entries'])
                for i in range(playlist_count):
                    self.download_playlist(playlists['entries'][i]['id'])

    def download_playlist(self, pid: str) -> None:
        playlist_metadata = downloader.download_playlist_metadata(PlaylistID(pid).url)
        if playlist_metadata is None: return None
        self.save_channel_info(ChannelID(playlist_metadata['uploader_id']))
        videos_ = [x['id'] for x in playlist_metadata['entries']]
        videos = []
        for x in videos_:
            if x not in videos:
                videos.append(x)
        for v in videos: self.download_video(v,False)
        self.db.write_playlist_info(
            PlaylistMetadataVIDs(
                id=pid,
                title=playlist_metadata['title'],
                description=playlist_metadata['description'],
                channel=playlist_metadata['uploader_id'],
                epoch=playlist_metadata['epoch'],
                entries=[v for v in videos if self.db.get_video_info(v)]
            )
        )
        self.db.add_tag_to_playlist(0, self.db.get_pnumid(pid))
    
    def download_video(self, vid: str, add_tag: bool = True) -> None:
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
                    channel=video_metadata['uploader_id'],
                    upload_date=video_metadata['upload_date'],
                    duration=video_metadata['duration'],
                    epoch=video_metadata['epoch']
                ))
                if add_tag: self.db.add_tag_to_video(0, self.db.get_vnumid(vid))
        else: self.save_channel_info(ChannelID(db_entry.channel))

    def save_channel_info(self, cid: ChannelID) -> None:
        db_entry = self.db.get_channel_info(str(cid))
        if db_entry is None:
            print(f"Downloading channel metadata: {cid}")
            data = downloader.download_playlist_metadata(cid.url,True)
            if data is None:
                raise Exception(f"Error: unable to get channel info from {cid}")
            self.db.write_channel_info(ChannelMetadata(
                id=str(cid),
                title=data['channel'],
                description=data['description'],
                epoch=data['epoch']
            ))