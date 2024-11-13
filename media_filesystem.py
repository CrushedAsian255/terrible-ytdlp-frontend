import os
import time
import shutil
from enum import Enum

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from datatypes import VideoID, convert_file_size

AWS_URL_KEEPALIVE=60*60*24 # 1 day

class StorageClass(Enum):
    OFFLINE = 0 # Asset is not stored anywhere
    LOCAL = 1   # Asset is stored in cache
    REMOTE = 2  # Asset is not cached

class MediaFilesystem:
    def __init__(self):
        raise NotImplementedError()
    def write_video(self, vid: VideoID, src_path: str) -> bool:
        raise NotImplementedError()
    def get_video_url(self, vid: VideoID, force_download: bool) -> str:
        raise NotImplementedError()
    def video_status(self, vid: VideoID) -> StorageClass:
        raise NotImplementedError()
    def write_thumbnail(self, vid: VideoID, src_path: str) -> bool:
        raise NotImplementedError()
    def get_thumbnail_url(self, vid: VideoID) -> str:
        raise NotImplementedError()
    def thumbnail_status(self, vid: VideoID) -> StorageClass:
        raise NotImplementedError()
    def delete_video(self, vid: VideoID):
        raise NotImplementedError()
    def list_all_videos(self) -> list[VideoID]:
        raise NotImplementedError()

class LocalFilesystem(MediaFilesystem):
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
    def write_video(self, vid: VideoID, src_path: str) -> bool:
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
                        f"Copying file: {convert_file_size(copied)} / {convert_file_size(src_size)}, "
                        f"{copied*100/src_size:.01f}%",end="\r"
                    )
        if os.stat(f"{dest_path}.tmp").st_size != src_size:
            raise ValueError("\nError: file sizes don't match")
        print(f"\nCopied {convert_file_size(src_size)}")
        os.rename(f"{dest_path}.tmp", dest_path)
        return True
    def get_video_url(self, vid: VideoID, force_download: bool) -> str:
        return self._filename(vid)
    def video_status(self, vid: VideoID) -> StorageClass:
        return StorageClass.LOCAL if os.path.isfile(self._filename(vid)) else StorageClass.OFFLINE
    def write_thumbnail(self, vid: VideoID, src_path: str) -> bool:
        os.makedirs(self._thumbnail_foldername(vid), exist_ok=True)
        shutil.copyfile(src_path,self._thumbnail_filename(vid))
        return True
    def get_thumbnail_url(self, vid: VideoID) -> str:
        return self._thumbnail_filename(vid)
    def thumbnail_status(self, vid: VideoID) -> StorageClass:
        return StorageClass.LOCAL if os.path.isfile(self._thumbnail_filename(vid)) else StorageCLASS.OFFLINE
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

class AWSFilesystem(MediaFilesystem):
    def _filename(self, vid: VideoID) -> str:
        return f"{self.prefix}{vid.value}.mkv"
    def _thumbnail_filename(self, vid: VideoID) -> str:
        return f"{self.prefix}{vid.value}.jpg"
    def __init__(self, bucket_name: str, prefix: str | None):
        self.s3 = boto3.resource(
            service_name="s3",
            endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            config = Config(signature_version='s3v4')
        )
        self.bucket_name = bucket_name
        self.prefix = "" if prefix is None else f"{prefix}/"
        self.uploaded = 0
        self.total = 0
    def _upload_callback(self, size):
        self.uploaded += size
        print(
            f"Uploading file: {convert_file_size(self.uploaded)} / {convert_file_size(self.total)}, "
            f"{self.uploaded*100/self.total:.01f}%",end="\r"
        )
    def write_video(self, vid: VideoID, src_path: str) -> bool:
        self.total = os.stat(src_path).st_size
        self.uploaded = 0
        print("Uploading file",end="\r")
        self.s3.Bucket(self.bucket_name).upload_file(src_path, self._filename(vid),Callback=self._upload_callback)
        print("\nUploaded!")
    def get_video_url(self, vid: VideoID, force_download: bool) -> str:
        if force_download:
            raise ValueError()
        return self.s3.meta.client.generate_presigned_url(
            ClientMethod='get_object',
            ExpiresIn=AWS_URL_KEEPALIVE,
            Params={'Bucket': self.bucket_name,'Key': self._filename(vid)}
        )
    def video_status(self, vid: VideoID) -> StorageClass:
        try:
            obj = self.s3.meta.client.head_object(Bucket=self.bucket_name, Key=self._filename(vid))
            return StorageClass.REMOTE
        except ClientError as exc:
            if exc.response['Error']['Code'] == '404':
                return StorageClass.OFFLINE
            raise ValueError()
    def write_thumbnail(self, vid: VideoID, src_path: str) -> bool:
        print("Uploading thumbnail")
        self.s3.Bucket(self.bucket_name).upload_file(src_path, self._thumbnail_filename(vid))
        print("Uploaded!")
    def get_thumbnail_url(self, vid: VideoID) -> str:
        return self.s3.meta.client.generate_presigned_url(
            ClientMethod='get_object',
            ExpiresIn=AWS_URL_KEEPALIVE,
            Params={'Bucket': self.bucket_name,'Key': self._thumbnail_filename(vid)}
        )
    def thumbnail_status(self, vid: VideoID) -> StorageClass:
        try:
            obj = self.s3.meta.client.head_object(Bucket=self.bucket_name, Key=self._thumbnail_filename(vid))
            return StorageClass.REMOTE
        except ClientError as exc:
            if exc.response['Error']['Code'] == '404':
                return StorageClass.LOCAL
            raise ValueError()
    def delete_video(self, vid: VideoID):
        raise NotImplementedError()
    def list_all_videos(self) -> list[VideoID]:
        raise NotImplementedError()

class CacheAWSFilesystem(MediaFilesystem):
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
    def _local_video_exists(self, vid: VideoID) -> StorageClass:
        return os.path.isfile(self._filename(vid))
    def _aws_video_exists(self, vid: VideoID) -> StorageClass:
        try:
            obj = self.s3.meta.client.head_object(Bucket=self.bucket_name, Key=self._aws_filename(vid))
            return True
        except ClientError as exc:
            if exc.response['Error']['Code'] == '404':
                return False
            raise ValueError()
    def _local_thumbnail_exists(self, vid: VideoID) -> StorageClass:
        return os.path.isfile(self._thumbnail_filename(vid))
    def _aws_thumbnail_exists(self, vid: VideoID) -> StorageClass:
        try:
            obj = self.s3.meta.client.head_object(Bucket=self.bucket_name, Key=self._aws_thumbnail_filename(vid))
            return True
        except ClientError as exc:
            if exc.response['Error']['Code'] == '404':
                return False
            raise ValueError()
    
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
            f"Uploading file: {convert_file_size(self.uploaded)} / {convert_file_size(self.total)}, "
            f"{self.uploaded*100/self.total:.01f}%",end="\r"
        )
    def _download_callback(self, size):
        self.downloaded += size
        print(
            f"Downloading file: {convert_file_size(self.downloaded)} / {convert_file_size(self.total)}, "
            f"{self.downloaded*100/self.total:.01f}%",end="\r"
        )
    def write_video(self, vid: VideoID, src_path: str) -> bool:
        print("Moving locally")
        os.makedirs(self._foldername(vid),exist_ok=True)
        shutil.copy(src_path, self._filename(vid))
        self.total = os.stat(src_path).st_size
        self.uploaded = 0
        print("Uploading file",end="\r")
        self.s3.Bucket(self.bucket_name).upload_file(src_path, self._aws_filename(vid),Callback=self._upload_callback)
        print("\nUploaded!")
    def get_video_url(self, vid: VideoID, force_download: bool) -> str:
        if self._local_video_exists(vid):
            return self._filename(vid)
        if force_download:
            try:
                obj = self.s3.meta.client.head_object(Bucket=self.bucket_name, Key=self._aws_filename(vid))
                self.total = obj['ContentLength']
            except ClientError as exc:
                raise ValueError()
            self.s3.Bucket(self.bucket_name).download_file(self._aws_filename(vid),self._filename(vid),Callback=self._download_callback)
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
    def write_thumbnail(self, vid: VideoID, src_path: str) -> bool:
        print("Moving locally")
        os.makedirs(self._thumbnail_foldername(vid), exist_ok=True)
        shutil.copy(src_path, self._thumbnail_filename(vid))
        print("Uploading thumbnail")
        self.s3.Bucket(self.bucket_name).upload_file(src_path, self._aws_thumbnail_filename(vid))
        print("Uploaded!")
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