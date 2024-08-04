from dbconnection import *
import downloader

class Library:
    @staticmethod
    def convert_video_id_to_url(vid): return f"https://www.youtube.com/watch?v={vid}"
    
    @staticmethod
    def convert_playlist_id_to_url(pid):
        pid = pid.split("@")
        if len(pid) == 1: return f"https://www.youtube.com/playlist?list={pid[0]}"
        else: return f"https://www.youtube.com/@{pid[1]}/{pid[0]}"
    
    def __init__(self, db_filename: str, media_dir: str, max_resolution: int | None, print_db_log: bool | None):
        self.media_dir = media_dir
        self.db = Database(db_filename,print_db_log)
        self.closed = False
        self.online = True
        self.max_video_resolution = max_resolution

    def exit(self): self.db.exit()

    def create_playlist_m3u8(self, pid: str | None, invert=False):
        if pid is None: return None
        data = self.db.get_playlist_info(pid)
        if data is None: return ""
        m3ustring = "#EXTM3U\n#EXTENC:UTF-8\n"
        m3ustring += f"#PLAYLIST:{data.title}\n"
        for item in (list(reversed(data.entries)) if invert else data.entries):
            m3ustring+=f"#EXTINF:{item.duration},{item.title}\n{get_file_name(self.media_dir,item.id)}\n"
        return m3ustring

    def add_tag_to_video(self, tag, vid):
        self.db.add_tag_to_video(
            self.db.get_tid(tag),
            self.db.get_vnumid(vid)
        )

    def get_all_videos(self, tag: str | None = None):
        if tag: return self.db.get_videos(self.db.get_tid(tag))
        else:   return self.db.get_videos()

    def get_all_single_videos(self, tag: str | None = None):
        if tag: return self.db.get_videos([0,self.db.get_tid(tag)])
        else:   return self.db.get_videos(0)

    def get_all_videos_from_channel(self, cid):
        return self.db.get_videos_from_channel(cid)

    def get_playlist_videos(self, pid):
        return self.db.get_playlist_info(pid).entries

    def get_all_playlists(self, tag=None):
        if tag: return self.db.get_playlists(self.db.get_tid(tag))
        else:   return self.db.get_playlists()

    def download_channel(self, cid: str, get_playlists=False):
        if cid[0] != "@": raise Exception("Invalid CID")

        self.download_playlist(f"videos{cid}")
        self.download_playlist(f"shorts{cid}")
        self.download_playlist(f"streams{cid}")
        
        if get_playlists:
            playlists_grabber = PlaylistMetadataGrabber(self.convert_playlist_id_to_url(f"playlists{cid}"))
            playlists_grabber.start()
            playlists_grabber.join()
            playlists = playlists_grabber.ret
            if not (playlists is None):
                playlist_count = len(playlists['entries'])
                for i in range(playlist_count):
                    self.download_playlist(playlists['entries'][i]['id'])

    def download_playlist(self, pid: str):
        playlist_metadata = downloader.download_playlist_metadata(Library.convert_playlist_id_to_url(pid))
        if playlist_metadata is None: return None
        self.save_channel_info(playlist_metadata['uploader_id'])
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
    
    def download_video(self, vid: str, add_tag=True):
        db_entry = self.db.get_video_info(vid)
        if db_entry is None:
            video_metadata = downloader.download_video(self.media_dir, vid, self.max_video_resolution)
            if video_metadata is not None:
                self.save_channel_info(video_metadata['uploader_id'])
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
        else: self.save_channel_info(db_entry.channel)

    def save_channel_info(self, cid: str):
        db_entry = self.db.get_channel_info(cid)
        if db_entry is None:
            print(f"Downloading channel metadata: {cid}")
            data = downloader.download_playlist_metadata(self.convert_playlist_id_to_url(f"about{cid}"),True)
            self.db.write_channel_info(ChannelMetadata(
                id=cid,
                title=data['channel'],
                description=data['description'],
                epoch=data['epoch']
            ))