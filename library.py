""" The library object: coordinates the database, yt-dlp, and user commands """
import urllib.request
import urllib.error
import os
from typing import Any
from argparse import Namespace
import hashlib
import json

from downloader import ytdlp_download_video, ytdlp_download_playlist_metadata
from dbconnection import Database
from datatypes import VideoID, PlaylistID
from datatypes import ChannelHandle, ChannelUUID, TagID, TagNumID
from datatypes import VideoMetadata, PlaylistMetadata, ChannelMetadata
from media_filesystem import MediaFilesystem, StorageClass

class Library:
    """ The library object """
    def __init__(self, library_path: str, media_fs: MediaFilesystem, args: Namespace):
        self.expect_many_failures = args.expect_many_failures
        self.media_fs = media_fs
        self.db = Database(f"{library_path}.db",args.print_db_log)
        self.args = args
        try:
            os.path.isfile(f"{library_path}.cjar")
            os.path.isfile(f"{library_path}.pot")
            self.login_data_path: str | None = library_path
        except (FileNotFoundError, IsADirectoryError):
            self.login_data_path = None

    def exit(self) -> None:
        """ Close the database """
        self.db.exit()

    def write_log(self, category: str, contents: str) -> None:
        """ Write an entry to the in-database audit log """
        self.db.write_log(category,contents)

    def download_thumbnail(self, video: VideoID, force=False) -> str | None:
        """ Download a video's thumbnail"""
        # IDEA: maybe move to downloader.py ?
        fileloc = f"/tmp/thumb.{video}.jpg"
        if not force:
            match self.media_fs.thumbnail_status(video):
                case StorageClass.LOCAL | StorageClass.REMOTE:
                    return None
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
                sha512 = hashlib.sha512()
                with open(fileloc, 'rb') as f:
                    while chunk := f.read(16384):
                        sha512.update(chunk)
                self.media_fs.write_thumbnail(video,fileloc)
                os.remove(fileloc)
                if thumb_type:
                    print(f"[INFO] {video} only had {thumb_type} thumbnail")
                return sha512.hexdigest()
            except urllib.error.HTTPError:
                pass
        print(f"[WARN] {video} has no thumbnails!")
        return None

    def update_thumbnails(self) -> None:
        """ Verify all thumbnails are downloaded """
        all_videos = self.media_fs.list_all_videos()
        for i,video in enumerate(all_videos):
            self.download_thumbnail(video)
            print(f"Updating thumbnails: {i+1} / {len(all_videos)}\r",end='')
        print("\nFinished")

    def create_playlist_m3u8(self, pid: PlaylistID | None, invert: bool = False) -> str:
        """ Create an m3u8 playlist from a given YouTube playlist """
        if pid is None:
            return ""
        data = self.db.get_playlist_info(pid)
        if data is None:
            return ""
        m3ustring = "#EXTM3U\n#EXTENC:UTF-8\n"
        m3ustring += f"#PLAYLIST:{data.title}\n"
        for item in (list(reversed(data.entries)) if invert else data.entries):
            m3ustring += f"#EXTINF:{item.duration},{item.title}\n"
            m3ustring += f"{self.media_fs.get_video_url(item.id,False)}\n"
        return m3ustring

    def create_tag(self, tag: TagID, description: str) -> None:
        """ Create a tag """
        self.db.create_tag(tag,description)

    def add_tag(self, tag: TagID, content_id: VideoID | PlaylistID) -> None:
        """ Add a tag to an item (video or playlist) """
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
        """ Get all videos from the database, with an optional filter on tags """
        if tag:
            return self.db.get_videos([self.db.get_tnumid(tag)])
        return self.db.get_videos([])

    def get_all_playlists(self, tag: TagID | None = None) -> list[PlaylistMetadata[int]]:
        """ Get all playlists from the database, with an optional filter on tags """
        if tag:
            return self.db.get_playlists([self.db.get_tnumid(tag)])
        return self.db.get_playlists([])

    def get_all_videos_from_channel(self, cid: ChannelUUID) -> list[VideoMetadata]:
        """ Get all videos uploaded by a certain channel """
        return self.db.get_videos_from_channel(cid)

    def get_all_playlists_from_channel(self, cid: ChannelUUID) -> list[PlaylistMetadata[int]]:
        """ Get all playlists created by a certain channel """
        return self.db.get_playlists_from_channel(cid)

    def get_playlist_videos(self, pid: PlaylistID) -> list[VideoMetadata]:
        """ Get metadata about a playlist from its ID """
        info = self.db.get_playlist_info(pid)
        if info is None:
            return []
        return info.entries

    def convert_handle_to_uuid(self, cid: ChannelUUID | ChannelHandle) -> ChannelUUID:
        """ Convert a channel @handle to its UUID """
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
                    raise IOError(
                        f"Error: Got data about channel {data['uploader_id']} instead of {cid}"
                    )
                self.db.write_channel_info(ChannelMetadata(
                    id=data['channel_id'],
                    handle=data['uploader_id'],
                    title=data['channel'],
                    description=data['description'],
                    epoch=data['epoch']
                ))
                return ChannelUUID(data['channel_id'])

    def download_channel(self, cid: ChannelUUID|ChannelHandle, get_playlists: bool = False) -> None:
        """ Download all videos from a channel, and optionally all playlists made by the channel """
        cid_ = self.convert_handle_to_uuid(cid)
        self.download_playlist(PlaylistID(f"${cid_}.videos"))
        self.download_playlist(PlaylistID(f"${cid_}.shorts"))
        self.download_playlist(PlaylistID(f"${cid_}.streams"))

        if get_playlists:
            playlists = ytdlp_download_playlist_metadata(cid_.playlists_url)
            if playlists is not None:
                for entry in playlists['entries']:
                    self.download_playlist(PlaylistID(entry['id']))

    def download_playlist(self, pid: PlaylistID) -> None:
        """ Download a playlist """
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

    @staticmethod
    def serialize_info_json(video_metadata: dict[str,Any]) -> str:
        " Remove long strings from the info_json and stringify it "
        info_json = video_metadata.copy()
        if 'subtitles' in info_json:
            del info_json['subtitles']
        if 'automatic_captions' in info_json:
            del info_json['automatic_captions']
        if 'thumbnails' in info_json:
            del info_json['thumbnails']
        if 'formats' in info_json:
            del info_json['formats']
        if 'requested_downloads' in info_json:
            del info_json['requested_downloads']
        if 'requested_formats' in info_json:
            for x in info_json['requested_formats']:
                if 'fragments' in x:
                    del x["fragments"]
        return json.dumps(info_json)

    def download_video(self, vid: VideoID, add_tag: bool = True) -> None:
        """ Download a video """
        db_entry = self.db.get_video_info(vid)
        chk_thm, chk_vid = self.db.get_checksums(vid)
        if db_entry is None or chk_thm is None or chk_vid is None:
            if chk_thm:
                thumbnail_checksum: str | None = chk_thm
            else:
                if self.expect_many_failures:
                    try:
                        urllib.request.urlretrieve(
                            f"https://i.ytimg.com/vi/{vid}/default.jpg",
                            f"/tmp/_thumb_tmp_{vid}.jpg"
                        )
                        os.remove(f"/tmp/_thumb_tmp_{vid}.jpg")
                    except urllib.error.HTTPError:
                        print(f"Video {vid} has no basic thumbnail, assuming video non-existant")
                        return
                thumbnail_checksum = self.download_thumbnail(vid,True)
                print(thumbnail_checksum)
            should_download = (
                self.media_fs.video_status(vid) == StorageClass.OFFLINE
                or chk_vid is None
            )

            video_metadata, video_download_path = ytdlp_download_video(
                vid, self.args.max_resolution, None,
                should_download, self.args.format_override
            )
            if video_metadata is None and self.login_data_path:
                print("Attempting logged in")
                video_metadata, video_download_path = ytdlp_download_video(
                    vid, self.args.max_resolution, self.login_data_path,
                    should_download, self.args.format_override
                )
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
                ), add_tag, Library.serialize_info_json(video_metadata))
            if video_download_path:
                sha512 = hashlib.sha512()
                with open(video_download_path, 'rb') as f:
                    while chunk := f.read(65536):
                        sha512.update(chunk)
                video_checksum = sha512.hexdigest()
                self.media_fs.write_video(vid,video_download_path)
                os.remove(video_download_path)
                self.db.set_checksums(vid, thumbnail_checksum, video_checksum)
        else:
            self.save_channel_info(db_entry.channel_id)

    def save_channel_info(self, cid: ChannelUUID) -> None:
        """ Download metadata about a channel """
        db_entry = self.db.get_channel_info(cid)
        if db_entry is None:
            print(f"Downloading channel metadata: {cid}")
            data = ytdlp_download_playlist_metadata(cid.about_url,True)
            if data is None:
                raise IOError(f"Error: unable to get channel info from {cid}")
            if data['channel_id'] != cid.value:
                raise IOError(f"Error: Got channel {data['channel_id']}, not {cid}")
            self.db.write_channel_info(ChannelMetadata(
                id=data['channel_id'],
                handle=data['uploader_id'],
                title=data['channel'],
                description=data['description'],
                epoch=data['epoch']
            ))

    def _get_cached_content(self) -> set[VideoID]:
        cached_videos = set()
        for video in self.db.get_videos([TagNumID(0)]):
            cached_videos.add(video.id)
        for playlist in self.db.get_playlists([TagNumID(0)]):
            if playlist_info := self.db.get_playlist_info(playlist.id):
                for video in playlist_info.entries:
                    cached_videos.add(video.id)
        return cached_videos

    def integrity_check(self) -> None:
        """ Verify all videos actually exist in the underlying storage """
        cached_videos = self._get_cached_content()
        database_videos: list[VideoID] = [x.id for x in self.get_all_videos()]
        self.media_fs.integrity_check(database_videos,cached_videos)
