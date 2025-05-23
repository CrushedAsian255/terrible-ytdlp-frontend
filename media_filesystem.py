""" Different media storage backends """
import os
import time
import shutil
from enum import Enum

import boto3 # type: ignore
from botocore.config import Config # type: ignore
from botocore.exceptions import ClientError # type: ignore

from datatypes import VideoID, convert_file_size

AWS_URL_KEEPALIVE=60*60*24 # 1 day

class StorageClass(Enum):
    """ Closest location an item is currently stored """
    OFFLINE = 0 # Asset is not stored anywhere
    LOCAL = 1   # Asset is stored in cache
    REMOTE = 2  # Asset is not cached

class MediaFilesystem:
    """ Abstract media backend class """
    def __init__(self):
        raise NotImplementedError()
    def write_video(self, vid: VideoID, src_path: str) -> None:
        """ Write a video file to the filesystem """
        raise NotImplementedError()
    def get_video_url(self, vid: VideoID, force_download: bool) -> str:
        """ Get either a local path or remote URL for a video file """
        raise NotImplementedError()
    def video_status(self, vid: VideoID) -> StorageClass:
        """ Check which storage class a video file is currently stored in """
        raise NotImplementedError()
    def video_cached(self, vid: VideoID) -> bool:
        """ Check if a video is currently cached locally """
        raise NotImplementedError()
    def write_thumbnail(self, vid: VideoID, src_path: str) -> None:
        """ Write a thumbnail file to the filesystem """
        raise NotImplementedError()
    def get_thumbnail_url(self, vid: VideoID) -> str:
        """ Get either a local path or remote URL for a thumbnail file """
        raise NotImplementedError()
    def thumbnail_status(self, vid: VideoID) -> StorageClass:
        """ Check which storage class a thumbnail file is currently stored in """
        raise NotImplementedError()
    def delete_video(self, vid: VideoID):
        """ Delete a video file from the filesystem """
        raise NotImplementedError()
    def list_all_videos(self) -> list[VideoID]:
        """ Get all videos stored in the filesystem """
        raise NotImplementedError()
    def integrity_check(self, database_videos: list[VideoID], cached_videos: set[VideoID]) -> None:
        """ Verify all videos exist and all force-cached videos are cached """
        raise NotImplementedError()

# pylint: disable=too-many-instance-attributes,missing-function-docstring

class LocalFilesystem(MediaFilesystem):
    """ Stores all files locally """
    def _foldername(self, vid: VideoID) -> str:
        return f"{self.path}/{ord(vid.value[0])-32}/{ord(vid.value[1])-32}"
    def _filename(self, vid: VideoID) -> str:
        return f"{self._foldername(vid)}/{vid.value}.mkv"
    def _thumbnail_foldername(self, vid: VideoID) -> str:
        return f"{self.path}/thumbs/{ord(vid.value[0])-32}/{ord(vid.value[1])-32}"
    def _thumbnail_filename(self, vid: VideoID) -> str:
        return f"{self._thumbnail_foldername(vid)}/{vid.value}.jpg"

    def __init__(self, path: str):
        self.path = path
        os.makedirs(path,exist_ok=True)
    def write_video(self, vid: VideoID, src_path: str) -> None:
        os.makedirs(self._foldername(vid),exist_ok=True)
        src_size = os.stat(src_path).st_size
        dest_path = self._filename(vid)
        with open(src_path, "rb") as src:
            with open(f"{dest_path}.tmp", "wb") as dest:
                copied = 0
                while True:
                    blk = src.read(2**23) # 8mb blocks
                    if not blk:
                        break
                    dest.write(blk)
                    copied += len(blk)
                    print(
                        f"Copying file: {convert_file_size(copied)} / "
                        f"{convert_file_size(src_size)}, "
                        f"{copied*100/src_size:.01f}%",end="\r"
                    )
        if os.stat(f"{dest_path}.tmp").st_size != src_size:
            raise ValueError("\nError: file sizes don't match")
        print(f"\nCopied {convert_file_size(src_size)}")
        os.rename(f"{dest_path}.tmp", dest_path)
    def get_video_url(self, vid: VideoID, force_download: bool) -> str:
        return self._filename(vid)
    def video_status(self, vid: VideoID) -> StorageClass:
        return StorageClass.LOCAL if os.path.isfile(self._filename(vid)) else StorageClass.OFFLINE
    def video_cached(self, vid: VideoID) -> bool:
        return True
    def write_thumbnail(self, vid: VideoID, src_path: str) -> None:
        os.makedirs(self._thumbnail_foldername(vid), exist_ok=True)
        shutil.copyfile(src_path,self._thumbnail_filename(vid))
    def get_thumbnail_url(self, vid: VideoID) -> str:
        return self._thumbnail_filename(vid)
    def thumbnail_status(self, vid: VideoID) -> StorageClass:
        if os.path.isfile(self._thumbnail_filename(vid)):
            return StorageClass.LOCAL
        return StorageClass.OFFLINE
    def delete_video(self, vid: VideoID):
        os.remove(self._filename(vid))
        os.remove(self._thumbnail_filename(vid))
    def list_all_videos(self) -> list[VideoID]:
        start = time.perf_counter_ns()
        videos_list = []
        dir0list = [x for x in os.scandir(f"{self.path}") if x.is_dir() and x.name != "thumbs"]
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
    def integrity_check(self, database_videos: list[VideoID], cached_videos: set[VideoID]) -> None:
        filesystem_videos: list[VideoID] = self.list_all_videos()
        for vid in filesystem_videos:
            if vid not in database_videos:
                print(f"Orphaned file: {vid}")
        for vid in database_videos:
            if vid not in filesystem_videos:
                print(f"ERROR: Missing file: {vid}")

class AWSFilesystem(MediaFilesystem):
    """ Store files in an AWS bucket, with local caching """
    def _foldername(self, vid: VideoID) -> str:
        return f"{self.path}/{ord(vid.value[0])-32}/{ord(vid.value[1])-32}"
    def _filename(self, vid: VideoID) -> str:
        return f"{self._foldername(vid)}/{vid.value}.mkv"
    def _thumbnail_foldername(self, vid: VideoID) -> str:
        return f"{self.path}/thumbs/{ord(vid.value[0])-32}/{ord(vid.value[1])-32}"
    def _thumbnail_filename(self, vid: VideoID) -> str:
        return f"{self._thumbnail_foldername(vid)}/{vid.value}.jpg"
    def _aws_filename(self, vid: VideoID) -> str:
        return f"{self.prefix}{vid.value}.mkv"
    def _aws_thumbnail_filename(self, vid: VideoID) -> str:
        return f"{self.prefix}{vid.value}.jpg"
    def _local_video_exists(self, vid: VideoID) -> bool:
        return os.path.isfile(self._filename(vid))
    def _aws_video_exists(self, vid: VideoID) -> bool:
        try:
            _obj = self.s3.meta.client.head_object(
                Bucket=self.bucket_name,
                Key=self._aws_filename(vid)
            )
            return True
        except ClientError as exc:
            if exc.response['Error']['Code'] == '404':
                return False
            raise ValueError() from exc
    def _local_thumbnail_exists(self, vid: VideoID) -> bool:
        return os.path.isfile(self._thumbnail_filename(vid))
    def _aws_thumbnail_exists(self, vid: VideoID) -> bool:
        try:
            _obj = self.s3.meta.client.head_object(
                Bucket=self.bucket_name,
                Key=self._aws_thumbnail_filename(vid)
            )
            return True
        except ClientError as exc:
            if exc.response['Error']['Code'] == '404':
                return False
            raise ValueError() from exc
    def _local_video_list(self) -> list[VideoID]:
        start = time.perf_counter_ns()
        videos_list = []
        dir0list = [x for x in os.scandir(self.path) if x.is_dir() and x.name != "thumbs"]
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
    def _aws_content_list(self) -> dict[VideoID,list[int | None]]:
        response = self.s3.meta.client.list_objects_v2(Bucket=self.bucket_name,Prefix=self.prefix)
        content_list = response['Contents']
        while response['IsTruncated']:
            response = self.s3.meta.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.prefix,
                ContinuationToken=response['NextContinuationToken']
            )
            content_list += response['Contents']
        videos: list[tuple[VideoID,int]] = [
            (VideoID(x['Key'][-15:-4]),x['Size'])
            for x in content_list if x['Key'][-4:] == '.mkv'
        ]
        thumbnails: list[tuple[VideoID,int]] = [
            (VideoID(x['Key'][-15:-4]),x['Size'])
            for x in content_list if x['Key'][-4:] == '.jpg'
        ]
        output: dict[VideoID,list[int | None]] = {}
        for vid,size in videos:
            if vid not in output:
                output[vid] = [None,None]
            output[vid][0]=size
        for vid,size in thumbnails:
            if vid not in output:
                output[vid] = [None,None]
            output[vid][1]=size
        return output

    def __init__(self, local_path: str, bucket_name: str, prefix: str | None):
        self.s3 = boto3.resource(
            service_name="s3",
            endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            config = Config(signature_version='s3v4')
        )
        self.path = local_path
        os.makedirs(self.path,exist_ok=True)
        self.bucket_name = bucket_name
        self.prefix = "" if prefix is None else f"{prefix}/"
        self.uploaded = 0
        self.downloaded = 0
        self.total = 0
    def _upload_callback(self, size):
        self.uploaded += size
        print(
            f"Uploading file: {convert_file_size(self.uploaded)} / "
            f"{convert_file_size(self.total)}, "
            f"{self.uploaded*100/self.total:.01f}%",end="\r"
        )
    def _download_callback(self, size):
        self.downloaded += size
        print(
            f"Downloading file: {convert_file_size(self.downloaded)} / "
            f"{convert_file_size(self.total)}, "
            f"{self.downloaded*100/self.total:.01f}%",end="\r"
        )
    def write_video(self, vid: VideoID, src_path: str) -> None:
        self.total = os.stat(src_path).st_size
        self.uploaded = 0
        print("Uploading file",end="\r")
        self.s3.Bucket(self.bucket_name).upload_file(
            src_path, self._aws_filename(vid),
            Callback=self._upload_callback
        )
        print("\nUploaded!")
        print("Moving locally")
        os.makedirs(self._foldername(vid),exist_ok=True)
        shutil.copy(src_path, self._filename(vid))
    def get_video_url(self, vid: VideoID, force_download: bool) -> str:
        if self._local_video_exists(vid):
            return self._filename(vid)
        if force_download:
            try:
                obj = self.s3.meta.client.head_object(
                    Bucket=self.bucket_name,
                    Key=self._aws_filename(vid)
                )
                self.total = obj['ContentLength']
                self.uploaded = 0
            except ClientError as exc:
                raise ValueError() from exc
            os.makedirs(self._foldername(vid),exist_ok=True)
            self.s3.Bucket(self.bucket_name).download_file(
                self._aws_filename(vid),
                f"{self._filename(vid)}.dl_tmp_1",
                Callback=self._download_callback
            )
            os.rename(f"{self._filename(vid)}.dl_tmp_1", self._filename(vid))
            return self._filename(vid)
        return self.s3.meta.client.generate_presigned_url(
            ClientMethod='get_object',
            ExpiresIn=AWS_URL_KEEPALIVE,
            Params={'Bucket': self.bucket_name,'Key': self._aws_filename(vid)}
        )
    def video_status(self, vid: VideoID) -> StorageClass:
        if self._local_video_exists(vid):
            return StorageClass.LOCAL
        if self._aws_video_exists(vid):
            return StorageClass.REMOTE
        return StorageClass.OFFLINE
    def video_cached(self, vid: VideoID) -> bool:
        return self._local_video_exists(vid)
    def write_thumbnail(self, vid: VideoID, src_path: str) -> None:
        print("Uploading thumbnail")
        self.s3.Bucket(self.bucket_name).upload_file(src_path, self._aws_thumbnail_filename(vid))
        print("Uploaded!")
        os.makedirs(self._thumbnail_foldername(vid), exist_ok=True)
        shutil.copy(src_path, self._thumbnail_filename(vid))
    def get_thumbnail_url(self, vid: VideoID) -> str:
        if self._local_thumbnail_exists(vid):
            return self._thumbnail_filename(vid)
        return self.s3.meta.client.generate_presigned_url(
            ClientMethod='get_object',
            ExpiresIn=AWS_URL_KEEPALIVE,
            Params={'Bucket': self.bucket_name,'Key': self._aws_thumbnail_filename(vid)}
        )
    def thumbnail_status(self, vid: VideoID) -> StorageClass:
        if self._local_thumbnail_exists(vid):
            return StorageClass.LOCAL
        if self._aws_thumbnail_exists(vid):
            return StorageClass.REMOTE
        return StorageClass.OFFLINE
    def delete_video(self, vid: VideoID):
        raise NotImplementedError()
    def list_all_videos(self) -> list[VideoID]:
        raise NotImplementedError()
    def integrity_check(self, database_videos: list[VideoID], cached_videos: set[VideoID]) -> None:
        aws_videos = self._aws_content_list()
        print(f"Total database video count: {len(database_videos)}")
        print(f"Total AWS video count: {len(aws_videos)}")
        print(f"Force-cached video count: {len(cached_videos)}")
        for vid in database_videos:
            local_video = self._filename(vid)
            if self._local_video_exists(vid):
                local_video_size = os.stat(local_video).st_size
                if vid not in aws_videos or aws_videos[vid][0] != local_video_size:
                    remote_video = self._aws_filename(vid)
                    print(f"Uploading video {vid}")
                    self.total = local_video_size
                    self.uploaded = 0
                    self.s3.Bucket(self.bucket_name).upload_file(
                        local_video, remote_video, Callback=self._upload_callback
                    )
                    print("")
            else:
                if vid not in aws_videos or aws_videos[vid][0] is None:
                    print(f"ERROR: Missing video: {vid}")
            if self._local_thumbnail_exists(vid):
                local_thumbnail = self._thumbnail_filename(vid)
                local_thumbnail_size = os.stat(local_thumbnail).st_size
                if vid not in aws_videos or aws_videos[vid][1] != local_thumbnail_size:
                    remote_thumbnail = self._aws_thumbnail_filename(vid)
                    print(f"Uploading thumbnail {vid}")
                    self.s3.Bucket(self.bucket_name).upload_file(local_thumbnail, remote_thumbnail)
            else:
                if vid not in aws_videos or aws_videos[vid][1] is None:
                    print(f"ERROR: Missing video: {vid}")
            if vid in cached_videos:
                if not self.video_cached(vid):
                    print(f"Caching video: {vid}")
                    os.makedirs(self._foldername(vid),exist_ok=True)
                    self.total = aws_videos[vid][0] or 0
                    self.downloaded = 0
                    self.s3.meta.client.download_file(
                        self.bucket_name,
                        self._aws_filename(vid), f"{self._filename(vid)}.dl_tmp_2",
                        Callback=self._download_callback,
                        Config=boto3.s3.transfer.TransferConfig(
                            multipart_threshold=2**30,
                            multipart_chunksize=2**30,
                            use_threads=False
                        )
                    )
                    os.rename(f"{self._filename(vid)}.dl_tmp_2", self._filename(vid))
                    print()
            else:
                if self.video_cached(vid):
                    print(f"Evicting video: {vid}")
                    os.remove(self._filename(vid))
