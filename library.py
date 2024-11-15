import urllib.request
import urllib.error
import os

from downloader import ytdlp_download_video, ytdlp_download_playlist_metadata
from dbconnection import Database
from datatypes import VideoID, PlaylistID
from datatypes import ChannelHandle, ChannelUUID, TagID, TagNumID
from datatypes import VideoMetadata, PlaylistMetadata, ChannelMetadata
from media_filesystem import MediaFilesystem, StorageClass

zero_tag = TagNumID(0)

class Library:
    def __init__(
        self, library_path: str, media_fs: MediaFilesystem,
        max_resolution: int | None, print_db_log: bool
    ):
        self.media_fs = media_fs
        self.db = Database(f"{library_path}.db",print_db_log)
        self.max_video_resolution = max_resolution
        try:
            os.path.isfile(f"{library_path}.cjar")
            os.path.isfile(f"{library_path}.pot")
            self.login_data_path = library_path
        except (FileNotFoundError, IsADirectoryError):
            pass

    def exit(self) -> None:
        self.db.exit()

    def write_log(self, category: str, contents: str) -> None:
        self.db.write_log(category,contents)

    def download_thumbnail(self, video: VideoID) -> None:
        fileloc = f"/tmp/thumb.{video}.jpg"
        match self.media_fs.thumbnail_status(video):
            case StorageClass.LOCAL | StorageClass.REMOTE:
                return
            case StorageClass.OFFLINE:
                pass
        download_paths: list[tuple[str,str|None]] = [
            (f"https://i.ytimg.com/vi/{video}/maxresdefault.jpg",None),
            (f"https://i.ytimg.com/vi/{video}/hq720.jpg",None),
            (f"https://i.ytimg.com/vi/{video}/sddefault.jpg",'SD'),
            # Don't ask my why SD Default is higher quality than HQ default
            (f"https://i.ytimg.com/vi/{video}/hqdefault.jpg",'SD'),
            (f"https://i.ytimg.com/vi/{video}/mqdefault.jpg","LQ"),
            (f"https://i.ytimg.com/vi/{video}/default.jpg",'basic')
        ]
        for url, thumb_type in download_paths:
            try:
                urllib.request.urlretrieve(url, fileloc)
                self.media_fs.write_thumbnail(video,fileloc)
                if thumb_type:
                    print(f"[INFO] {video} only had {thumb_type} thumbnail")
                return
            except urllib.error.HTTPError:
                pass
        print(f"[WARN] {video} has no thumbnails!")

    def update_thumbnails(self) -> None:
        all_videos = self.media_fs.list_all_videos()
        for i,video in enumerate(all_videos):
            self.download_thumbnail(video)
            print(f"Updating thumbnails: {i+1} / {len(all_videos)}\r",end='')
        print("\nFinished")

    def create_playlist_m3u8(self, pid: PlaylistID | None, invert: bool = False) -> str:
        if pid is None:
            return ""
        data = self.db.get_playlist_info(pid)
        if data is None:
            return ""
        m3ustring = "#EXTM3U\n#EXTENC:UTF-8\n"
        m3ustring += f"#PLAYLIST:{data.title}\n"
        for item in (list(reversed(data.entries)) if invert else data.entries):
            m3ustring+=f"#EXTINF:{item.duration},{item.title}\n{self.media_fs.get_video_url(item.id,False)}\n"
        return m3ustring

    def create_tag(self, tag: TagID, description: str) -> None:
        self.db.create_tag(tag,description)

    def add_tag(self, tag: TagID, content_id: VideoID | PlaylistID) -> None:
        match content_id:
            case VideoID():
                self.db.add_tag(
                    tag,
                    content_id
                )
            case PlaylistID():
                self.db.add_tag(
                    tag,
                    content_id
                )

    def get_all_videos(self, tag: TagID | None = None) -> list[VideoMetadata]:
        if tag:
            return self.db.get_videos([self.db.get_tnumid(tag)])
        return self.db.get_videos([])

    def get_all_playlists(self, tag: TagID | None = None) -> list[PlaylistMetadata[int]]:
        if tag:
            return self.db.get_playlists([self.db.get_tnumid(tag)])
        return self.db.get_playlists([])

    def get_all_videos_from_channel(self, cid: ChannelUUID) -> list[VideoMetadata]:
        return self.db.get_videos_from_channel(cid)

    def get_all_playlists_from_channel(self, cid: ChannelUUID) -> list[PlaylistMetadata[int]]:
        return self.db.get_playlists_from_channel(cid)

    def get_playlist_videos(self, pid: PlaylistID) -> list[VideoMetadata]:
        info = self.db.get_playlist_info(pid)
        if info is None:
            return []
        return info.entries

    def convert_handle_to_uuid(self, cid: ChannelUUID | ChannelHandle) -> ChannelUUID:
        match cid:
            case ChannelUUID(): return cid
            case ChannelHandle():
                db_entry = self.db.get_channel_info(cid)
                if db_entry is not None:
                    return db_entry.id
                print(f"Downloading channel metadata from handle {cid}")
                data = ytdlp_download_playlist_metadata(f"{cid.about_url}",True)
                if data is None:
                    raise IOError(f"Error: unable to get channel info from {cid}")
                if data['uploader_id'] != cid.value:
                    raise IOError(f"Error: Got data about channel {data['uploader_id']} instead of {cid}")
                self.db.write_channel_info(ChannelMetadata(
                    id=data['channel_id'],
                    handle=data['uploader_id'],
                    title=data['channel'],
                    description=data['description'],
                    epoch=data['epoch']
                ))
                return ChannelUUID(data['channel_id'])

    def download_channel(self, cid_: ChannelUUID | ChannelHandle, get_playlists: bool = False) -> None:
        cid = self.convert_handle_to_uuid(cid_)
        self.download_playlist(PlaylistID(f"${cid}.videos"))
        self.download_playlist(PlaylistID(f"${cid}.shorts"))
        self.download_playlist(PlaylistID(f"${cid}.streams"))

        if get_playlists:
            playlists = ytdlp_download_playlist_metadata(cid.playlists_url)
            if playlists is not None:
                for entry in playlists['entries']:
                    self.download_playlist(PlaylistID(entry['id']))

    def download_playlist(self, pid: PlaylistID) -> None:
        playlist_metadata = ytdlp_download_playlist_metadata(pid.url)
        if playlist_metadata is None:
            return
        self.save_channel_info(ChannelUUID(playlist_metadata['channel_id']))
        videos_ = [VideoID(x['id']) for x in playlist_metadata['entries']]
        videos = []
        for x in videos_:
            if x not in videos:
                videos.append(x)
        for v in videos:
            self.download_video(v,False)
        self.db.write_playlist_info(
            PlaylistMetadata(
                id=pid,
                title=playlist_metadata['title'],
                description=playlist_metadata['description'],
                channel_handle=ChannelHandle(playlist_metadata['uploader_id']),
                channel_id=ChannelUUID(playlist_metadata['channel_id']),
                channel_name=playlist_metadata['channel'],
                epoch=playlist_metadata['epoch'],
                entries=[v for v in videos if self.db.get_video_info(v)]
            )
        )

    def download_video(self, vid: VideoID, add_tag: bool = True) -> None:
        db_entry = self.db.get_video_info(vid)
        if db_entry is None:
            self.download_thumbnail(vid)
            video_metadata = ytdlp_download_video(self.media_fs, vid, self.max_video_resolution, None)
            if video_metadata is None and self.login_data_path:
                print("Attempting logged in")
                video_metadata = ytdlp_download_video(self.media_fs, vid, self.max_video_resolution, self.login_data_path)

            if video_metadata is not None:
                self.save_channel_info(ChannelUUID(video_metadata['channel_id']))
                self.db.write_video_info(VideoMetadata(
                    id=vid,
                    title=video_metadata['title'],
                    description=video_metadata['description'],
                    channel_id=ChannelUUID(video_metadata['channel_id']),
                    channel_handle=ChannelHandle(video_metadata['uploader_id']),
                    channel_name=video_metadata['channel'],
                    upload_timestamp=video_metadata['timestamp'],
                    duration=video_metadata['duration'],
                    epoch=video_metadata['epoch'],
                ), add_tag)
        else:
            self.save_channel_info(db_entry.channel_id)

    def save_channel_info(self, cid: ChannelUUID) -> None:
        db_entry = self.db.get_channel_info(cid)
        if db_entry is None:
            print(f"Downloading channel metadata: {cid}")
            data = ytdlp_download_playlist_metadata(cid.about_url,True)
            if data is None:
                raise IOError(f"Error: unable to get channel info from {cid}")
            if data['channel_id'] != cid.value:
                raise IOError(f"Error: Got data about channel {data['channel_id']} instead of {cid}")
            self.db.write_channel_info(ChannelMetadata(
                id=data['channel_id'],
                handle=data['uploader_id'],
                title=data['channel'],
                description=data['description'],
                epoch=data['epoch']
            ))

    def integrity_check(self) -> None:
        for db_vid in [x.id for x in self.get_all_videos()]:
            video_tags = len(self.db.get_tags(db_vid))
            video_playlists = len(self.db.get_video_playlists(db_vid))
            if video_tags == 0 and video_playlists == 0:
                print(f"Orphaned video: {db_vid}")
        database_videos: list[VideoID] = [x.id for x in self.get_all_videos()]
        print(self.media_fs)
        self.media_fs.integrity_check(database_videos)