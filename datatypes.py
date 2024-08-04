import re
import typing

class VideoID:
    __slots__ = ('value',)
    @property
    def fileloc(self) -> str: return f"{ord(self.value[0])-32}/{ord(self.value[1])-32}/{self.value}.mkv"
    def filename(self, media_path: str = "") -> str: return f"{media_path}/{self.fileloc}"
    @property
    def url(self) -> str: return f"https://www.youtube.com/watch?v={self.value}"
    def __str__(self) -> str: return self.value
    def __init__(self, value: str) -> None:
        if re.match(r"^[a-zA-Z0-9_-]{11}$",value) is None:
            raise ValueError(f"Error: Invalid VideoID {value}")
        self.value = value

class PlaylistID:
    __slots__ = ('value',)
    @property
    def url(self) -> str:
        value = self.value.split("@")
        if len(value) == 1: return f"https://www.youtube.com/playlist?list={value[0]}"
        else: return f"https://www.youtube.com/@{value[1]}/{value[0]}"
    def __str__(self) -> str: return self.value
    def __init__(self, value: str) -> None:
        if re.match(r"^(videos|streams|shorts)@.*|PL[a-zA-Z0-9_-]{32}$",value) is None:
            raise ValueError(f"Error: Invalid PlaylistID {value}")
        self.value = value

class ChannelID:
    __slots__ = ('value',)
    @property
    def url(self) -> str: return f"https://www.youtube.com/{self.value}"
    def __str__(self) -> str: return self.value
    def __init__(self, value: str) -> None:
        if re.match(r"^@.*$",value) is None:
            raise ValueError(f"Error: Invalid ChannelID {value}")
        self.value = value

class ChannelUUID:
    __slots__ = ('value',)
    @property
    def url(self) -> str: return f"https://www.youtube.com/channel/{self.value}"
    def __str__(self) -> str: return self.value
    def __init__(self, value: str) -> None:
        if re.match(r"^@UC[a-zA-Z0-9_-]{22}$",value) is None:
            raise ValueError(f"Error: Invalid ChannelUUID {value}")
        self.value = value

VideoNumID = typing.NewType('VideoNumID',int)
PlaylistNumID = typing.NewType('PlaylistNumID',int)
ChannelNumID = typing.NewType('ChannelNumID',int)
TagNumID = typing.NewType('TagNumID',int)

def convert_file_size(size: int) -> str:
    size=int(size)
    if size < 2**10: return f"{size           } B"
    if size < 2**20: return f"{size/(2**10):.02f} KiB"
    if size < 2**30: return f"{size/(2**20):.02f} MiB"
    if size < 2**40: return f"{size/(2**30):.02f} GiB"
    if size < 2**50: return f"{size/(2**40):.02f} TiB"
    if size < 2**60: return f"{size/(2**50):.02f} PiB"
    if size < 2**70: return f"{size/(2**60):.02f} EiB"
    else:            return f"{int(size/(2**60))} EiB"
