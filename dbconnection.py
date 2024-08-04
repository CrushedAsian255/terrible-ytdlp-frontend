import sqlite3
import re
import time
from dataclasses import dataclass
from typing import Any, cast
from datatypes import *

@dataclass(slots=True)
class VideoMetadata:
    id: str
    title: str
    description: str
    upload_date: int
    duration: int
    epoch: int
    channel: str
    channel_name: str

@dataclass(slots=True)
class Tag:
    num_id: int
    id: str
    long_name: str

@dataclass(slots=True)
class PlaylistMetadata:
    id: str
    title: str
    description: str
    channel: str
    epoch: int

@dataclass(slots=True)
class PlaylistMetadataVideoInfo(PlaylistMetadata): entries: list[VideoMetadata]

@dataclass(slots=True)
class PlaylistMetadataVCount(PlaylistMetadata): entries: int

@dataclass(slots=True)
class PlaylistMetadataVCountWithChannelName(PlaylistMetadataVCount): channel_name: str

@dataclass(slots=True)
class PlaylistMetadataVideoInfoWithChannelName(PlaylistMetadataVideoInfo): channel_name: str

@dataclass(slots=True)
class ChannelMetadata:
    id: str
    title: str
    description: str
    epoch: int

def verify_tid(s: str) -> bool: return len(s) > 0 and (re.match('[^a-z0-9_]', s) is None)
def verify_vid(s: str) -> bool: return len(s) > 0 and len(s) == 11 and (re.match('[^A-Za-z0-9-_]', s) is None)
def verify_pid(s: str) -> bool: return len(s) > 0 and (re.match('[^A-Za-z0-9-_]', s) is None)
def verify_cid(s: str) -> bool: return len(s) > 0 and (s[0] == '@') and (re.match('[^A-Za-z0-9-_]', s[1:]) is None)

class Database:
    def exec(self, sql: str, params: tuple[Any, ...] | None = None, possibly_slow: bool | None = None) -> list[tuple[Any, ...]]:
        command = re.sub(r'[\n\t ]+', ' ', sql).strip()
        if self.print_db_log: print(f"DB command ---------\nSQL:{sum([ord(x) for x in sql])}\n{command}{f"\n{params=}" if params is not None else ""}")
        start = time.perf_counter_ns()
        if params is not None:
            out = self.connection.execute(command, params).fetchall()
        else:
            out = self.connection.execute(command).fetchall()
        end = time.perf_counter_ns()
        if end - start > 15_000_000 and (possibly_slow is not True): print(f"!warning db command took {int((end-start)/1_000_000)} ms [SQL:{sum([ord(x) for x in sql])}]")
        if self.print_db_log: print((
                f"Time took: {(end-start)/1_000_000:.2f}ms\n"
                f"Returned {len(out)} row(s)\n"
                f"--------------------"
        ))
        return out

    def __init__(self, dbfname: str, print_db_log: bool):
        self.print_db_log = print_db_log
        self.db_filename = dbfname
        self.connection = sqlite3.connect(self.db_filename)

        self.exec("PRAGMA foreign_keys=ON")
        self.connection.commit()
        if self.exec("PRAGMA foreign_keys")[0][0] != 1: raise Exception("Build of sqlite3 does not support foreign keys")

        self.exec('''CREATE TABLE IF NOT EXISTS Channel (
            num_id INTEGER PRIMARY KEY,
            id TEXT NOT NULL UNIQUE,

            title TEXT NOT NULL,
            description TEXT NOT NULL,
            epoch INTEGER NOT NULL
        ) STRICT''')
        self.exec("CREATE INDEX IF NOT EXISTS idx_channel_id ON Channel(id)")

        self.exec('''CREATE TABLE IF NOT EXISTS Video (
            num_id INTEGER PRIMARY KEY,
            id TEXT NOT NULL UNIQUE,

            title TEXT NOT NULL,
            description TEXT NOT NULL,
            upload_date INTEGER NOT NULL,
            duration INTEGER NOT NULL,
            epoch INTEGER NOT NULL,
            
            channel_id INTEGER NOT NULL,
            FOREIGN KEY (channel_id) REFERENCES Channel(num_id)      
        ) STRICT''')
        self.exec("CREATE INDEX IF NOT EXISTS idx_video_id ON Video(id)")
        self.exec("CREATE INDEX IF NOT EXISTS idx_video_channel ON Video(channel_id)")

        self.exec('''CREATE TABLE IF NOT EXISTS Playlist (
            num_id INTEGER PRIMARY KEY,
            id TEXT NOT NULL UNIQUE,

            title TEXT NOT NULL,
            description TEXT NOT NULL,
            
            count INTEGER NOT NULL,
            
            epoch INTEGER NOT NULL,

            channel_id INTEGER NOT NULL,
            FOREIGN KEY (channel_id) REFERENCES Channel(num_id)
        ) STRICT''')
        self.exec("CREATE INDEX IF NOT EXISTS idx_playlist_id ON Playlist(id)")
        self.exec("CREATE INDEX IF NOT EXISTS idx_playlist_channel ON Playlist(channel_id)")

        self.exec('''CREATE TABLE IF NOT EXISTS Pointer (
            playlist_id INTEGER NOT NULL,
            video_id INTEGER NOT NULL,
            
            position INTEGER NOT NULL,
            
            PRIMARY KEY (playlist_id, video_id),
            FOREIGN KEY (playlist_id) REFERENCES Playlist(num_id) ON DELETE CASCADE,
            FOREIGN KEY (video_id) REFERENCES Video(num_id) ON DELETE CASCADE
        ) STRICT''')
        self.exec("CREATE INDEX IF NOT EXISTS idx_pointer_playlist ON Pointer(playlist_id)")
        self.exec("CREATE INDEX IF NOT EXISTS idx_pointer_video ON Pointer(video_id)")

        self.exec('''CREATE TABLE IF NOT EXISTS Tag (
            num_id INTEGER PRIMARY KEY,
            id TEXT NOT NULL UNIQUE,
            description TEXT
        ) STRICT''')

        self.exec('''CREATE TABLE IF NOT EXISTS TaggedVideo (
            tag_id INTEGER NOT NULL,
            video_id INTEGER NOT NULL,
            
            PRIMARY KEY (tag_id, video_id),
            
            FOREIGN KEY (tag_id) REFERENCES Tag (num_id) ON DELETE CASCADE,
            FOREIGN KEY (video_id) REFERENCES Video (num_id) ON DELETE CASCADE
        ) STRICT''')
        self.exec("CREATE INDEX IF NOT EXISTS idx_tag_video ON TaggedVideo(tag_id)")
        self.exec("CREATE INDEX IF NOT EXISTS idx_tag_vid ON TaggedVideo(video_id)")

        self.exec('''CREATE TABLE IF NOT EXISTS TaggedPlaylist (
            tag_id INTEGER NOT NULL,
            playlist_id INTEGER NOT NULL,
            
            PRIMARY KEY (tag_id, playlist_id),
            
            FOREIGN KEY (tag_id) REFERENCES Tag (num_id) ON DELETE CASCADE,
            FOREIGN KEY (playlist_id) REFERENCES Playlist (num_id) ON DELETE CASCADE
        ) STRICT''')
        self.exec("CREATE INDEX IF NOT EXISTS idx_tag_playlist ON TaggedPlaylist(tag_id)")
        self.exec("CREATE INDEX IF NOT EXISTS idx_tag_pid ON TaggedPlaylist(playlist_id)")

        self.exec("INSERT OR IGNORE INTO Tag(num_id,id,description) VALUES (?,?,?)",(0,'',None))

        self.connection.commit()

        int_check = self.exec("PRAGMA integrity_check")
        self.connection.commit()

        if int_check[0][0]!='ok': raise Exception(f"FATAL ERROR: Database corrupt: {int_check}")

        self.exec("VACUUM",None,True)
        self.connection.commit()

    def get_channel_info(self, cid: str) -> ChannelMetadata | None:
        if not verify_cid(cid): raise ValueError(f"Invalid CID: {cid}")
        data = self.exec('''
        SELECT
            id, title, description, epoch
        FROM Channel
        WHERE id=?
        ''',(cid,))
        if len(data) == 0: return None
        return ChannelMetadata(
            id=data[0][0],
            title=data[0][1],
            description=data[0][2],
            epoch=int(data[0][3])
        )
    def write_channel_info(self, channel: ChannelMetadata) -> None:
        if not verify_cid(channel.id): raise ValueError(f"Invalid CID: {channel.id}")
        self.exec("INSERT OR REPLACE INTO Channel(id,title,description,epoch) VALUES (?,?,?,?)",(channel.id, channel.title, channel.description, int(channel.epoch)))
        self.connection.commit()

    def get_playlists(self, tnumid_: int | list[int | None] | None = None) -> list[PlaylistMetadataVCountWithChannelName]:
        tnumid: list[int] = []
        if type(tnumid_) is int: tnumid = [tnumid_]
        if type(tnumid_) is list[int | None]: tnumid = [x for x in tnumid_ if type(x) is int]

        return [PlaylistMetadataVCountWithChannelName(
            id=playlist[0],
            title=playlist[1],
            description=playlist[2],
            epoch=int(playlist[4]),
            channel=playlist[5],
            entries=int(playlist[3]),
            channel_name=playlist[6]
        ) for playlist in self.exec(f'''
            SELECT
                Playlist.id, Playlist.title, Playlist.description, Playlist.count, Playlist.epoch, Channel.id, Channel.title
            FROM Playlist
            INNER JOIN Channel ON Playlist.channel_id=Channel.num_id
            {f'''JOIN (
                SELECT playlist_id
                FROM TaggedPlaylist
                WHERE tag_id IN ({",".join([str(x) for x in tnumid])})
                GROUP BY playlist_id
                HAVING COUNT(DISTINCT tag_id) = {len(tnumid)}
            ) AS tagged ON Playlists.num_id = tagged.playlist_id;''' if len(tnumid) > 0 else ""}
        ''')]
    def get_pnumid(self, pid: str) -> int | None:
        if not verify_pid(pid): raise ValueError(f"Invalid PID: {pid}")
        data = self.exec("SELECT num_id FROM Playlist WHERE id=?",(pid,))
        if len(data) == 0: return None
        return cast(int | None,data[0][0])

    def get_playlist_info(self, pid: str) -> PlaylistMetadataVideoInfoWithChannelName | None:
        if not verify_pid(pid): raise ValueError(f"Invalid PID: {pid}")
        data = self.exec('''
        SELECT
            Playlist.id, Playlist.title, Playlist.description, Playlist.epoch, Channel.id, Playlist.num_id, Channel.title
        FROM Playlist
        INNER JOIN Channel ON Playlist.channel_id=Channel.num_id
        WHERE Playlist.id=?
        ''',(pid,))
        if len(data) == 0: return None
        return PlaylistMetadataVideoInfoWithChannelName(
            id=data[0][0],
            title=data[0][1],
            description=data[0][2],
            channel=data[0][4],
            epoch=int(data[0][3]),
            entries=[VideoMetadata(
                id=x[0],
                title=x[1],
                description=x[2],
                upload_date=int(x[3]),
                duration=int(x[4]),
                epoch=int(x[5]),
                channel=x[6],
                channel_name=x[7]
            ) for x in self.exec('''
                SELECT
                    Video.id,Video.title,Video.description,Video.upload_date,Video.duration,Video.epoch,
                    Channel.id,Channel.title
                FROM Video
                RIGHT JOIN Pointer ON Video.num_id=Pointer.video_id
                INNER JOIN Channel ON Video.channel_id=Channel.num_id
                WHERE Pointer.playlist_id=?
                ORDER BY Pointer.position ASC
            ''',(data[0][5],))],
            channel_name=data[0][6]
        )
    def write_playlist_info(self, playlist: PlaylistMetadata, entries: list[VideoID]) -> PlaylistNumID:
        if not verify_pid(playlist.id): raise ValueError(f"Invalid PID: {playlist.id}")
        if not verify_cid(playlist.channel): raise ValueError(f"Invalid CID: {playlist.channel}")
        db_out = self.exec('''
        INSERT OR REPLACE INTO Playlist(id,title,description,epoch,count,channel_id)
        VALUES (?,?,?,?,?,(SELECT num_id FROM Channel WHERE id=?)) RETURNING (num_id)''',(playlist.id, playlist.title, playlist.description, int(playlist.epoch), len(entries), playlist.channel))
        pnumid = self.exec("SELECT num_id FROM Playlist WHERE id=?",(playlist.id,))[0][0]
        self.exec("DELETE FROM Pointer WHERE playlist_id=?",(pnumid,))
        for x in enumerate(entries):
            self.exec(f"INSERT INTO Pointer(playlist_id, video_id, position) VALUES (?,(SELECT num_id FROM Video WHERE id=?),?)",(pnumid,x[1],x[0]))
        self.connection.commit()
        return cast(PlaylistNumID,db_out[0][0])

    def get_videos(self, tnumid_: int | list[int | None] | None = None) -> list[VideoMetadata]:
        tnumid: list[int] = []
        if type(tnumid_) is int: tnumid = [tnumid_]
        if type(tnumid_) is list: tnumid = [x for x in tnumid_ if type(x) is int]

        return [VideoMetadata(
            id=data[0],
            title=data[1],
            description=data[2],
            upload_date=int(data[3]),
            duration=int(data[4]),
            epoch=data[5],
            channel=data[6],
            channel_name=data[7]
        ) for data in self.exec(f'''
            SELECT
                Video.id, Video.title, Video.description, Video.upload_date, Video.duration, Video.epoch, Channel.id, Channel.title
            FROM Video
            INNER JOIN Channel ON Video.channel_id=Channel.num_id
            {f'''JOIN (
                SELECT video_id
                FROM TaggedVideo
                WHERE tag_id IN ({",".join([str(x) for x in tnumid])})
                GROUP BY video_id
                HAVING COUNT(DISTINCT tag_id) = {len(tnumid)}
            ) AS tagged ON Video.num_id = tagged.video_id;''' if len(tnumid) > 0 else ""}
        ''')]

    def get_videos_from_channel(self, cid: ChannelID) -> list[VideoMetadata]:
        return [VideoMetadata(
            id=data[0],
            title=data[1],
            description=data[2],
            upload_date=int(data[3]),
            duration=int(data[4]),
            epoch=data[5],
            channel=data[6],
            channel_name=data[7]
        ) for data in self.exec('''
            SELECT
                Video.id, Video.title, Video.description, Video.upload_date, Video.duration, Video.epoch, Channel.id, Channel.title
            FROM Video
            INNER JOIN Channel ON Video.channel_id=Channel.num_id
            WHERE Video.channel_id=(SELECT num_id FROM Channel WHERE id='?')
        ''',(cid,))]
    def get_vnumid(self, vid: str) -> VideoNumID | None:
        if not verify_vid(vid): raise ValueError(f"Invalid VID: {vid}")
        data = self.exec("SELECT num_id FROM Video WHERE id=?",(vid,))
        if len(data)==0: return None
        return cast(VideoNumID,data[0][0])
    def get_video_info(self, vid: str) -> VideoMetadata | None:
        if not verify_vid(vid): raise ValueError(f"Invalid VID: {vid}")
        data = self.exec('''
        SELECT
            Video.id,Video.title,Video.description,Video.upload_date,Video.duration,Video.epoch,Channel.id,Channel.title
        FROM Video
        INNER JOIN Channel ON Video.channel_id=Channel.num_id
        WHERE Video.id=?
        ''',(vid,))
        if len(data) == 0: return None
        return VideoMetadata(
            id=data[0][0],
            title=data[0][1],
            description=data[0][2],
            upload_date=int(data[0][3]),
            duration=int(data[0][4]),
            epoch=data[0][5],
            channel=data[0][6],
            channel_name=data[0][7]
        )
    def write_video_info(self, video: VideoMetadata) -> VideoNumID:
        if not verify_vid(video.id): raise ValueError(f"Invalid VID: {video.id}")
        if not verify_cid(video.channel): raise ValueError(f"Invalid CID: {video.channel}")
        db_out = self.exec('''
        INSERT OR REPLACE INTO Video(id,title,description,upload_date,duration,epoch,channel_id)
        VALUES (
            ?,?,?,?,?,?,
            (SELECT num_id FROM Channel WHERE id=?)
        ) RETURNING (num_id)
        ''',(video.id,
            video.title,
            video.description,
            int(video.upload_date),
            int(video.duration),
            int(video.epoch),
            video.channel
        ))
        self.connection.commit()
        return cast(VideoNumID,db_out[0][0])

    def create_tag(self, tid: str, description: str) -> TagNumID:
        if not verify_tid(tid): raise ValueError(f"Invalid TID: {tid}")
        db_out = self.exec("INSERT INTO Tag(id,long_name) VALUES (?,?) RETURNING (num_id)",(tid,description))
        self.connection.commit()
        return cast(TagNumID,db_out[0][0])
    
    def get_tnumid(self, tid: str) -> TagNumID | None:
        if not verify_tid(tid): return None
        output = self.exec("SELECT num_id FROM Tag WHERE id=?",(tid,))
        if len(output) == 0: return None
        return cast(TagNumID,output[0][0])

    def get_tag_info(self, tid: str) -> Tag | None:
        if not verify_tid(tid): return None
        output = self.exec("SELECT num_id,id,long_name FROM Tag WHERE id=?",(tid,))
        if len(output) == 0: return None
        return Tag(
            num_id=output[0][0],
            id=output[0][1],
            long_name=output[0][2]
        )
    def delete_tag(self, tid: str) ->  None:
        if not verify_tid(tid): return None
        self.exec("DELETE FROM Tag WHERE id=?",(tid,))
        self.connection.commit()

    def add_tag_to_video(self, tnumid: int | None, vnumid: int | None) -> bool:
        if tnumid is None: return False
        if vnumid is None: return False
        self.exec("INSERT OR REPLACE INTO TaggedVideo(tag_id,video_id) VALUES (?,?)", (tnumid, vnumid))
        self.connection.commit()
        return True
    def add_tag_to_playlist(self, tnumid: int | None, pnumid: int | None) -> bool:
        if tnumid is None: return False
        if pnumid is None: return False
        self.exec("INSERT OR REPLACE INTO TaggedPlaylist(tag_id,playlist_id) VALUES (?,?)",(tnumid,pnumid))
        self.connection.commit()
        return True

    def get_video_tags(self, vid: str) -> list[TagNumID]:
        return [x[0] for x in self.exec("SELECT tag_id FROM TaggedVideo WHERE video_id = (SELECT num_id FROM Video WHERE id = ?)", (vid,))]

    def get_playlist_tags(self, pid: str) -> list[TagNumID]:
        return [x[0] for x in self.exec("SELECT tag_id FROM TaggedPlaylist WHERE playlist_id = (SELECT num_id FROM Playlist WHERE id = ?)", (pid,))]

    def get_playlist_tags_from_num_id(self, pnumid: int) -> list[TagNumID]:
        return [x[0] for x in self.exec("SELECT tag_id FROM TaggedPlaylist WHERE playlist_id = ?", (pnumid,))]

    def get_video_playlists(self, vid: str) -> list[tuple[PlaylistNumID,int]]:
        return self.exec("SELECT playlist_id, position FROM Pointer WHERE video_id = (SELECT num_id FROM Video WHERE id = ?)", (vid,))

    def exit(self) -> None:
        self.connection.commit()
        self.connection.close()
