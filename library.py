import os
import time
import urllib.request
import urllib.error

from downloader import ytdlp_download_video, ytdlp_download_playlist_metadata
from dbconnection import Database
from datatypes import VideoID, PlaylistID
from datatypes import ChannelHandle, ChannelUUID, TagID, TagNumID
from datatypes import VideoMetadata, PlaylistMetadata, ChannelMetadata
from datatypes import convert_file_size

zero_tag = TagNumID(0)

class Library:
    def __init__(
        self, db_filename: str, media_dir: str,
        max_resolution: int | None, print_db_log: bool,
        login_data_path: str | None
    ):
        self.media_dir = media_dir
        self.db = Database(db_filename,print_db_log)
        self.max_video_resolution = max_resolution
        self.login_data_path = login_data_path

    def exit(self) -> None:
        self.db.exit()

    def write_log(self, category: str, contents: str) -> None:
        self.db.write_log(category,contents)

    def get_all_filesystem_videos(self) -> list[VideoID]:
        start = time.perf_counter_ns()
        videos_list = []
        dir0list = [x for x in os.scandir(f"{self.media_dir}") if x.is_dir() and x.name != "thumbs"]
        for dir0idx, dir0item in enumerate(dir0list):
            dir1list = [x for x in os.scandir(dir0item) if x.is_dir()]
            for dir1item in dir1list:
                dir2list = [
                    VideoID(x.name[:-4]) for x in
                    os.scandir(dir1item)
                    if not x.is_dir() and x.name[-4:]=='.mkv' and x.name[0] != "."
                ]
                videos_list += dir2list
            print(
                f"Enumerating directories: {dir0idx+1} / {len(dir0list)} "
                f"({len(videos_list)} items)",end="\r"
            )
        end = time.perf_counter_ns()
        print(f"\nEnumerated {len(videos_list)} videos in {(end-start)/1_000_000_000:.2f} seconds")
        return videos_list

    def download_thumbnail(self, video: VideoID) -> None:
        fileloc = f"{video.foldername(f"{self.media_dir}/thumbs")}/{video}.jpg"
        if os.path.isfile(fileloc):
            return
        os.makedirs(video.foldername(f"{self.media_dir}/thumbs"), exist_ok=True)
        try:
            urllib.request.urlretrieve(f"https://i.ytimg.com/vi/{video}/maxresdefault.jpg", fileloc)
            return
        except urllib.error.HTTPError:
            pass
        try:
            urllib.request.urlretrieve(f"https://i.ytimg.com/vi/{video}/sddefault.jpg", fileloc)
            print(f"{video} only had SD thumbnail")
            return
        except urllib.error.HTTPError:
            try:
                # Don't ask my why SD Default is higher quality than HQ default
                urllib.request.urlretrieve(f"https://i.ytimg.com/vi/{video}/hqdefault.jpg", fileloc)
                print(f"{video} only had SD thumbnail")
                return
            except urllib.error.HTTPError:
                pass
        try:
            urllib.request.urlretrieve(f"https://i.ytimg.com/vi/{video}/mqdefault.jpg", fileloc)
            print(f"{video} only had LQ thumbnail")
            return
        except urllib.error.HTTPError:
            pass
        try:
            urllib.request.urlretrieve(f"https://i.ytimg.com/vi/{video}/mqdefault.jpg", fileloc)
            print(f"{video} only had basic thumbnail")
            return
        except urllib.error.HTTPError:
            pass
        print(f"[ERROR] {video} has no thumbnails!")

    def update_thumbnails(self) -> None:
        all_videos = self.get_all_filesystem_videos()
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
            m3ustring+=f"#EXTINF:{item.duration},{item.title}\n{item.id.filename(self.media_dir)}\n"
        return m3ustring

    def create_tag(self, tag: TagID, description: str) -> None:
        self.db.create_tag(tag,description)

    def add_tag(self, tag: TagID, content_id: VideoID | PlaylistID) -> None:
        match content_id:
            case VideoID():
                self.db.add_tag_to_video(
                    self.db.get_tnumid(tag),
                    self.db.get_vnumid(content_id)
                )
            case PlaylistID():
                self.db.add_tag_to_playlist(
                    self.db.get_tnumid(tag),
                    self.db.get_pnumid(content_id)
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
        self.db.add_tag_to_playlist(zero_tag, self.db.get_pnumid(pid))

    def download_video(self, vid: VideoID, add_tag: bool = True) -> None:
        db_entry = self.db.get_video_info(vid)
        if db_entry is None:
            self.download_thumbnail(vid)
            video_metadata = ytdlp_download_video(self.media_dir, vid, self.max_video_resolution, None)
            if video_metadata is None and self.login_data_path:
                print("Attempting logged in")
                video_metadata = ytdlp_download_video(self.media_dir, vid, self.max_video_resolution, self.login_data_path)

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
                ))
                if add_tag:
                    self.db.add_tag_to_video(zero_tag, self.db.get_vnumid(vid))
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

    def prune(self) -> None:
        for db_vid in [x.id for x in self.get_all_videos()]:
            video_tags = len(self.db.get_video_tags(db_vid))
            video_playlists = len(self.db.get_video_playlists(db_vid))
            if video_tags == 0 and video_playlists == 0:
                print(f"Removing orphaned video: {db_vid}")
                self.db.remove_video(db_vid)

    def purge(self) -> int:
        videos_database = [x.id for x in self.get_all_videos()]
        total_size = 0
        for fs_vid in self.get_all_filesystem_videos():
            if fs_vid not in videos_database:
                fname = fs_vid.filename(self.media_dir)
                size = os.path.getsize(fname)
                total_size += size
                os.remove(fname)
        return total_size

    def integrity_check(self) -> None:
        videos_filesystem: list[VideoID] = self.get_all_filesystem_videos()
        videos_database: list[VideoID] = [x.id for x in self.get_all_videos()]
        total_size = 0
        for vid in videos_filesystem:
            if vid not in videos_database:
                size = os.path.getsize(vid.filename(self.media_dir))
                total_size += size
                print(f"Orphaned file: {vid.filename()} | {convert_file_size(size)}")
        print(f"Total orphaned file size: {convert_file_size(total_size)}")
        total_size = 0
        for vid in videos_database:
            if vid not in videos_filesystem:
                print(f"ERROR: Missing file: {vid.fileloc}")
            video_tags = len(self.db.get_video_tags(vid))
            video_playlists = len(self.db.get_video_playlists(vid))
            if video_tags == 0 and video_playlists == 0:
                size = os.path.getsize(vid.filename(self.media_dir))
                total_size += size
                print(f"Orphaned video: {vid} | {convert_file_size(size)}")
        print(f"Total orphaned video size: {convert_file_size(total_size)}")

    def get_largest_videos(self) -> list[tuple[VideoID,int]]:
        video_sizes = []
        for vid in [x.id for x in self.get_all_videos()]:
            is_single_video = len([
                x for x in self.db.get_video_tags(vid) if x != TagNumID(0)
            ]) <= 1
            if is_single_video and len(self.db.get_video_playlists(vid)) == 0:
                try:
                    video_sizes.append((vid,os.path.getsize(vid.filename(self.media_dir))))
                except FileNotFoundError:
                    print(f"ERROR: Missing file: {vid.fileloc}")
        video_sizes.sort(key=lambda x: -x[1])
        return video_sizes