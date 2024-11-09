# pylint: disable=too-many-instance-attributes
import re
import typing
from dataclasses import dataclass

class VideoID:
    __slots__ = ('value',)
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VideoID):
            return NotImplemented
        return self.value == other.value
    @property
    def url(self) -> str:
        return f"https://www.youtube.com/watch?v={self.value}"
    def __str__(self) -> str:
        return self.value
    def __repr__(self) -> str:
        return f"VideoID<{self.value}>"
    def __init__(self, value: str | None) -> None:
        if value is None:
            raise ValueError("Value does not exist")
        if re.match(r"^[a-zA-Z0-9_-]{11}$",value) is None:
            raise ValueError(f"Error: Invalid VideoID {value}")
        self.value = value

class PlaylistID:
    __slots__ = ('value',)
    @property
    def url(self) -> str:
        if self.value[0] == "$":
            v = self.value[1:].split(".")
            return f"https://www.youtube.com/channel/{v[0]}/{v[1]}"
        return f"https://www.youtube.com/playlist?list={self.value}"
    def __str__(self) -> str:
        return self.value
    def __repr__(self) -> str:
        return f"PlaylistID<{self.value}>"
    def __init__(self, value: str | None) -> None:
        if value is None:
            raise ValueError("Value does not exist")
        if re.match(
            r"^\$UC[a-zA-Z0-9_-]{22}\.(videos|streams|shorts)|"
            r"PL[a-zA-Z0-9_-]{32}|"
            r"PL[a-zA-Z0-9_-]{16}|"
            r"FL[a-zA-Z0-9_-]{22}$"
        ,value) is None:
            raise ValueError(f"Error: Invalid PlaylistID {value}")
        self.value = value

class ChannelHandle:
    __slots__ = ('value',)
    @property
    def url(self) -> str:
        return f"https://www.youtube.com/{self.value}"
    @property
    def about_url(self) -> str:
        return f"https://www.youtube.com/{self.value}/about"
    def __str__(self) -> str:
        return self.value
    def __repr__(self) -> str:
        return f"ChannelHandle<{self.value}>"
    def __init__(self, value: str | None) -> None:
        if value is None:
            raise ValueError("Value does not exist")
        if re.match(r"^@.*$",value) is None:
            raise ValueError(f"Error: Invalid ChannelHandle {value}")
        self.value = value

class TagID:
    __slots__ = ('value',)
    def __str__(self) -> str:
        return self.value
    def __repr__(self) -> str:
        return f"TagID<{self.value}>"
    def __init__(self, value: str | None) -> None:
        if value is None:
            raise ValueError("Value does not exist")
        if re.match(r"^([a-zA-Z0-9-_.]+/)*[a-zA-Z0-9-_.]+$",value) is None:
            raise ValueError(f"Error: Invalid TagID {value}")
        self.value = value

class ChannelUUID:
    __slots__ = ('value',)
    @property
    def playlists_url(self) -> str:
        return f"https://www.youtube.com/channel/{self.value}/playlists"
    @property
    def about_url(self) -> str:
        return f"https://www.youtube.com/channel/{self.value}/about"
    @property
    def url(self) -> str:
        return f"https://www.youtube.com/channel/{self.value}"
    def __str__(self) -> str:
        return self.value
    def __repr__(self) -> str:
        return f"ChannelUUID<{self.value}>"
    def __init__(self, value: str) -> None:
        if re.match(r"^UC[a-zA-Z0-9_-]{22}$",value) is None:
            raise ValueError(f"Error: Invalid ChannelUUID {value}")
        self.value = value

class VideoNumID:
    __slots__ = ('value',)
    def __str__(self) -> str:
        return f"{self.value}"
    def __int__(self) -> int:
        return self.value
    def __repr__(self) -> str:
        return f"VideoNumID<{self.value}>"
    def __init__(self, value: int) -> None:
        self.value = value

class PlaylistNumID:
    __slots__ = ('value',)
    def __str__(self) -> str:
        return f"{self.value}"
    def __int__(self) -> int:
        return self.value
    def __repr__(self) -> str:
        return f"PlaylistNumID<{self.value}>"
    def __init__(self, value: int) -> None:
        self.value = value

class ChannelNumID:
    __slots__ = ('value',)
    def __str__(self) -> str:
        return f"{self.value}"
    def __int__(self) -> int:
        return self.value
    def __repr__(self) -> str:
        return f"ChannelNumID<{self.value}>"
    def __init__(self, value: int) -> None:
        self.value = value

class TagNumID:
    __slots__ = ('value',)
    def __str__(self) -> str:
        return f"{self.value}"
    def __int__(self) -> int:
        return self.value
    def __repr__(self) -> str:
        return f"TagNumID<{self.value}>"
    def __init__(self, value: int) -> None:
        self.value = value

def convert_file_size(size: int) -> str:
    size=int(size)
    if size < 2**10:
        return f"{size           } B"
    if size < 2**20:
        return f"{size/(2**10):.02f} KiB"
    if size < 2**30:
        return f"{size/(2**20):.02f} MiB"
    if size < 2**40:
        return f"{size/(2**30):.02f} GiB"
    return f"{size/(2**40):.02f} TiB"

@dataclass(slots=True)
class VideoMetadata:
    def to_string(self) -> str:
        def convert_duration(dur: int) -> str:
            return f"{int(dur/3600)}:{int(dur/60)%60:02d}:{dur%60:02d}"
        return (
            f"{self.id} | "
            f"{convert_duration(self.duration)} | "
            f"{self.channel_name} ({self.channel_handle}): "
            f"{self.title}"
        )

    id: VideoID
    title: str
    description: str
    upload_timestamp: int
    duration: int
    epoch: int
    channel_id: ChannelUUID
    channel_handle: ChannelHandle
    channel_name: str

PlaylistEntriesT=typing.TypeVar('PlaylistEntriesT')
@dataclass(slots=True)
class PlaylistMetadata(typing.Generic[PlaylistEntriesT]):
    @property
    def entry_count(self) -> int:
        match self.entries:
            case int(): return self.entries
            case list(): return len(self.entries)
            case _: raise NotImplementedError(type(self.entries))

    def to_string(self) -> str:
        return (
            f"{self.id} | "
            f"{self.entry_count} video{"s" if self.entry_count > 1 else ""} | "
            f"{self.channel_name} ({self.channel_handle}): "
            f"{self.title}"
        )

    id: PlaylistID
    title: str
    description: str
    channel_id: ChannelUUID
    channel_handle: ChannelHandle
    channel_name: str
    epoch: int
    entries: PlaylistEntriesT

@dataclass(slots=True)
class ChannelMetadata:
    id: ChannelUUID
    handle: ChannelHandle
    title: str
    description: str
    epoch: int

@dataclass(slots=True)
class TagMetadata:
    num_id: TagNumID
    id: TagID
    long_name: str
