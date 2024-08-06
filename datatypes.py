import re
import typing

class VideoID:
    __slots__ = ('value',)
    @property
    def fileloc(self) -> str: return f"{ord(self.value[0])-32}/{ord(self.value[1])-32}/{self.value}.mkv"
    def filename(self, media_path: str = "") -> str: return f"{media_path}/{self.fileloc}"
    def foldername(self, media_path: str = "") -> str: return f"{media_path}/{ord(self.value[0])-32}/{ord(self.value[1])-32}"
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VideoID):
            raise NotImplemented
        return self.value == other.value
    
    @property
    def url(self) -> str: return f"https://www.youtube.com/watch?v={self.value}"
    def __str__(self) -> str: return self.value
    def __repr__(self) -> str: return f"VideoID<{self.value}>"
    
    def __init__(self, value: str | None) -> None:
        if value is None: raise ValueError("Value does not exist")
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
    def __repr__(self) -> str: return f"PlaylistID<{self.value}>"
    def __init__(self, value: str | None) -> None:
        if value is None: raise ValueError("Value does not exist")
        if re.match(r"^(videos|streams|shorts)@.*|PL[a-zA-Z0-9_-]{32}$",value) is None:
            raise ValueError(f"Error: Invalid PlaylistID {value}")
        self.value = value

class ChannelID:
    __slots__ = ('value',)
    @property
    def url(self) -> str: return f"https://www.youtube.com/{self.value}"
    @property
    def playlists_url(self) -> str: return f"https://www.youtube.com/{self.value}/playlists"
    @property
    def about_url(self) -> str: return f"https://www.youtube.com/{self.value}/about"
    def __str__(self) -> str: return self.value
    def __repr__(self) -> str: return f"ChannelID<{self.value}>"
    def __init__(self, value: str | None) -> None:
        if value is None: raise ValueError("Value does not exist")
        if re.match(r"^@.*$",value) is None:
            raise ValueError(f"Error: Invalid ChannelID {value}")
        self.value = value

class TagID:
    __slots__ = ('value',)
    def __str__(self) -> str: return self.value
    def __repr__(self) -> str: return f"TagID<{self.value}>"
    def __init__(self, value: str | None) -> None:
        if value is None: raise ValueError("Value does not exist")
        if re.match(r"^#[a-zA-Z0-9-_]+$",value) is None:
            raise ValueError(f"Error: Invalid TagID {value}")
        self.value = value


class ChannelUUID:
    __slots__ = ('value',)
    @property
    def url(self) -> str: return f"https://www.youtube.com/channel/{self.value}"
    def __str__(self) -> str: return self.value
    def __repr__(self) -> str: return f"ChannelUUID<{self.value}>"
    def __init__(self, value: str) -> None:
        if re.match(r"^@UC[a-zA-Z0-9_-]{22}$",value) is None:
            raise ValueError(f"Error: Invalid ChannelUUID {value}")
        self.value = value

class VideoNumID:
    __slots__ = ('value',)
    def __str__(self) -> str: return f"{self.value}"
    def __int__(self) -> int: return self.value
    def __repr__(self) -> str: return f"VideoNumID<{self.value}>"
    def __init__(self, value: int) -> None: self.value = value

class PlaylistNumID:
    __slots__ = ('value',)
    def __str__(self) -> str: return f"{self.value}"
    def __int__(self) -> int: return self.value
    def __repr__(self) -> str: return f"PlaylistNumID<{self.value}>"
    def __init__(self, value: int) -> None: self.value = value

class ChannelNumID:
    __slots__ = ('value',)
    def __str__(self) -> str: return f"{self.value}"
    def __int__(self) -> int: return self.value
    def __repr__(self) -> str: return f"ChannelNumID<{self.value}>"
    def __init__(self, value: int) -> None: self.value = value

class TagNumID:
    __slots__ = ('value',)
    def __str__(self) -> str: return f"{self.value}"
    def __int__(self) -> int: return self.value
    def __repr__(self) -> str: return f"TagNumID<{self.value}>"
    def __init__(self, value: int) -> None: self.value = value

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
