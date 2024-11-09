import os
import time
import shutil

from datatypes import VideoID, convert_file_size

class MediaFilesystem:
    def __init__(self):
        raise NotImplementedError()
    def write_video(self, vid: VideoID, src_path: str) -> bool:
        raise NotImplementedError()
    def get_video_url(self, vid: VideoID) -> str:
        raise NotImplementedError()
    def video_exists(self, vid: VideoID) -> bool:
        raise NotImplementedError()
    def write_thumbnail(self, vid: VideoID, src_path: str) -> bool:
        raise NotImplementedError()
    def get_thumbnail_url(self, vid: VideoID) -> str:
        raise NotImplementedError()
    def thumbnail_exists(self, vid: VideoID) -> bool:
        raise NotImplementedError()
    def delete_video(self, vid: VideoID):
        raise NotImplementedError()
    def list_all_videos(self) -> list[VideoID]:
        raise NotImplementedError()

class LocalFilesystem(MediaFilesystem):
    def foldername(self, vid: VideoID) -> str:
        return f"{self.path}/{ord(vid.value[0])-32}/{ord(vid.value[1])-32}"
    def filename(self, vid: VideoID) -> str:
        return f"{self.foldername(vid)}/{vid.value}.mkv"
    def thumbnail_foldername(self, vid: VideoID) -> str:
        return f"{self.path}/thumbs/{ord(vid.value[0])-32}/{ord(vid.value[1])-32}"
    def thumbnail_filename(self, vid: VideoID) -> str:
        return f"{self.thumbnail_foldername(vid)}/{vid.value}.jpg"

    def __init__(self, path: str):
        self.path = path
        os.makedirs(path,exist_ok=True)
    def write_video(self, vid: VideoID, src_path: str) -> bool:
        os.makedirs(self.foldername(vid),exist_ok=True)
        src_size = os.stat(src_path).st_size
        dest_path = self.filename(vid)
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
    def get_video_url(self, vid: VideoID) -> str:
        return self.filename(vid)
    def video_exists(self, vid: VideoID) -> bool:
        return os.path.isfile(self.filename(vid))
    def write_thumbnail(self, vid: VideoID, src_path: str) -> bool:
        os.makedirs(self.thumbnail_foldername(vid), exist_ok=True)
        shutil.copyfile(src_path,self.thumbnail_filename(vid))
        return True
    def get_thumbnail_url(self, vid: VideoID) -> str:
        return self.thumbnail_filename(vid)
    def thumbnail_exists(self, vid: VideoID) -> bool:
        return os.path.isfile(self.thumbnail_filename(vid))
    def delete_video(self, vid: VideoID):
        os.remove(self.filename(vid))
        os.remove(self.thumbnail_filename(vid))
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
