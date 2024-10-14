import sqlite3
import re
import time
from typing import Any, cast
import sys

from datatypes import VideoID, PlaylistID, ChannelUUID, TagID, ChannelHandle
from datatypes import VideoNumID, PlaylistNumID, ChannelNumID, TagNumID
from datatypes import VideoMetadata, PlaylistMetadata, ChannelMetadata, TagMetadata

class Database:
    def exec(self, sql: str, params: tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
        if params is None:
            params = ()
        new_params: list[Any] = []
        for param in params:
            match param:
                case str() | int():
                    new_params.append(param)
                case VideoNumID() | PlaylistNumID() | ChannelNumID() | TagNumID():
                    new_params.append(int(param))
                case VideoID() | PlaylistID() | ChannelHandle() | ChannelUUID() | TagID():
                    new_params.append(str(param))
                case None:
                    new_params.append(None)
                case _:
                    raise NotImplementedError(f"Error: Did not expect {type(param)} | {param}")
        params = tuple(new_params)
        command = re.sub(r'[\n\t ]+', ' ', sql).strip()
        cmdref=f"{command.split(" ")[0]}:{sum((ord(x)*i)&0x2a for i,x in enumerate(command))&0xff}"
        if self.print_db_log:
            print(f"[DEBUG] {cmdref} {command} \n{params=}")
        start = time.perf_counter_ns()
        out = self.connection.execute(command, params).fetchall()
        end = time.perf_counter_ns()
        if end - start > 10_000_000:
            print(f"[WARNING] Command {int((end-start)/1_000_000)} ms [{cmdref}]",file=sys.stderr)
        if self.print_db_log:
            print(
                f"Time took: {(end-start)/1_000_000:.2f}ms\n"
                f"Returned {len(out)} row(s)\n"
                f"--------------------"
            )
        return out

    def __init__(self, dbfname: str, print_db_log: bool):
        self.print_db_log = print_db_log
        self.db_filename = dbfname
        self.connection = sqlite3.connect(self.db_filename)
        try:
            int_check = self.exec("PRAGMA integrity_check")
        except sqlite3.DatabaseError as e:
            raise IOError("Invalid database") from e
        self.connection.commit()

        if int_check[0][0]!='ok':
            raise IOError(f"FATAL ERROR: Database corrupt: {int_check}")

        self.exec("PRAGMA foreign_keys=ON")
        self.connection.commit()
        if self.exec("PRAGMA foreign_keys")[0][0] != 1:
            raise OSError("Build of sqlite3 does not support foreign keys")

        self.exec('''CREATE TABLE IF NOT EXISTS Log (
            ts INTEGER PRIMARY KEY,
            category TEXT NOT NULL,
            content TEXT NOT NULL
        ) STRICT''')
        self.exec("CREATE INDEX IF NOT EXISTS idx_log_category ON Log(category)")

        self.exec('''CREATE TABLE IF NOT EXISTS Channel (
            num_id INTEGER PRIMARY KEY,
            id TEXT NOT NULL UNIQUE,
            handle TEXT NOT NULL UNIQUE,

            title TEXT NOT NULL,
            description TEXT NOT NULL,
            epoch INTEGER NOT NULL,
            removed INTEGER NOT NULL DEFAULT 0,
            aux_data TEXT
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
            removed INTEGER NOT NULL DEFAULT 0,
            aux_data TEXT,
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
            removed INTEGER NOT NULL DEFAULT 0,
            aux_data TEXT,
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
            description TEXT,
            aux_data TEXT
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

    def write_log(self, category: str, contents: str) -> None:
        self.exec('''
            INSERT INTO Log(ts,category,content) VALUES (?,?,?)
        ''',(int(time.time()*1000000), category, contents))
        self.connection.commit()

    def get_video_info(self, vid: VideoID) -> VideoMetadata | None:
        data = self.exec('''
        SELECT
            Video.id,Video.title,Video.description,Video.upload_date,
            Video.duration,Video.epoch,Channel.handle,Channel.id,Channel.title
        FROM Video
        INNER JOIN Channel ON Video.channel_id=Channel.num_id
        WHERE Video.id=?
        ''',(vid,))
        if len(data) == 0:
            return None
        return VideoMetadata(
            id=VideoID(data[0][0]),
            title=data[0][1],
            description=data[0][2],
            upload_date=int(data[0][3]),
            duration=int(data[0][4]),
            epoch=data[0][5],
            channel_handle=ChannelHandle(data[0][6]),
            channel_id=ChannelUUID(data[0][7]),
            channel_name=data[0][8]
        )
    def write_video_info(self, video: VideoMetadata) -> VideoNumID:
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
            video.channel_id
        ))
        self.connection.commit()
        return cast(VideoNumID,db_out[0][0])

    def get_playlist_info(self, pid: PlaylistID) -> PlaylistMetadata[list[VideoMetadata]] | None:
        data = self.exec('''
        SELECT
            Playlist.id, Playlist.title, Playlist.description, Playlist.epoch,
            Channel.id, Channel.title, Channel.handle
        FROM Playlist
        INNER JOIN Channel ON Playlist.channel_id=Channel.num_id
        WHERE Playlist.id=?
        ''',(pid,))
        if len(data) == 0:
            return None
        return PlaylistMetadata[list[VideoMetadata]](
            id=PlaylistID(data[0][0]),
            title=data[0][1],
            description=data[0][2],
            epoch=int(data[0][3]),
            entries=[VideoMetadata(
                id=VideoID(x[0]),
                title=x[1],
                description=x[2],
                upload_date=int(x[3]),
                duration=int(x[4]),
                epoch=int(x[5]),
                channel_id=ChannelUUID(x[6]),
                channel_handle=ChannelHandle(x[8]),
                channel_name=x[7]
            ) for x in self.exec('''
                SELECT
                    Video.id, Video.title, Video.description,
                    Video.upload_date, Video.duration, Video.epoch,
                    Channel.id,Channel.title,Channel.handle
                FROM Video
                RIGHT JOIN Pointer ON Video.num_id=Pointer.video_id
                INNER JOIN Channel ON Video.channel_id=Channel.num_id
                WHERE Pointer.playlist_id=?
                ORDER BY Pointer.position ASC
            ''',(data[0][5],))],
            channel_name=data[0][5],
            channel_id=ChannelUUID(data[0][4]),
            channel_handle=ChannelHandle(data[0][6])
        )
    def write_playlist_info(self, playlist: PlaylistMetadata[list[VideoID]]) -> PlaylistNumID:
        db_out = self.exec(
            '''INSERT OR REPLACE INTO Playlist(id,title,description,epoch,count,channel_id)
            VALUES (?,?,?,?,?,(SELECT num_id FROM Channel WHERE id=?)) RETURNING (num_id)''',
            (
                playlist.id, playlist.title, playlist.description, int(playlist.epoch),
                playlist.entry_count, playlist.channel_id
            )
        )
        pnumid = self.exec("SELECT num_id FROM Playlist WHERE id=?",(playlist.id,))[0][0]
        self.exec("DELETE FROM Pointer WHERE playlist_id=?",(pnumid,))
        for x in enumerate(playlist.entries):
            self.exec(
                '''INSERT INTO Pointer(playlist_id, video_id, position)
                VALUES (?,(SELECT num_id FROM Video WHERE id=?),?)''',(pnumid,x[1],x[0])
            )
        self.connection.commit()
        return PlaylistNumID(db_out[0][0])

    def get_channel_info(self, cid: ChannelUUID | ChannelHandle) -> ChannelMetadata | None:
        match cid:
            case ChannelUUID():
                data = self.exec('''
                SELECT
                    id, handle, title, description, epoch
                FROM Channel
                WHERE id=?
                ''',(cid,))
                if len(data) == 0:
                    return None
                return ChannelMetadata(
                    id=ChannelUUID(data[0][0]),
                    handle=ChannelHandle(data[0][1]),
                    title=data[0][2],
                    description=data[0][3],
                    epoch=int(data[0][4])
                )
            case ChannelHandle():
                data = self.exec('''
                SELECT
                    id, handle, title, description, epoch
                FROM Channel
                WHERE handle=?
                ''',(cid,))
                if len(data) == 0:
                    return None
                return ChannelMetadata(
                    id=ChannelUUID(data[0][0]),
                    handle=ChannelHandle(data[0][1]),
                    title=data[0][2],
                    description=data[0][3],
                    epoch=int(data[0][4])
                )
    def write_channel_info(self, channel: ChannelMetadata) -> None:
        self.exec(
            "INSERT OR REPLACE INTO Channel(id,handle,title,description,epoch) VALUES (?,?,?,?,?)",
            (channel.id, channel.handle, channel.title, channel.description, int(channel.epoch))
        )
        self.connection.commit()

    def get_videos(self, tnumid: list[TagNumID | None]) -> list[VideoMetadata]:
        return [VideoMetadata(
            id=VideoID(data[0]),
            title=data[1],
            description=data[2],
            upload_date=int(data[3]),
            duration=int(data[4]),
            epoch=data[5],
            channel_id=ChannelUUID(data[6]),
            channel_handle=ChannelHandle(data[8]),
            channel_name=data[7]
        ) for data in self.exec(f'''
            SELECT
                Video.id, Video.title, Video.description, Video.upload_date,
                Video.duration, Video.epoch, Channel.id, Channel.title, Channel.handle
            FROM Video
            INNER JOIN Channel ON Video.channel_id=Channel.num_id
            {f'''JOIN (
                SELECT video_id
                FROM TaggedVideo
                WHERE tag_id IN ({",".join([str(x) for x in tnumid if isinstance(x,TagNumID)])})
                GROUP BY video_id
                HAVING COUNT(DISTINCT tag_id) = {len(tnumid)}
            ) AS tagged ON Video.num_id = tagged.video_id;''' if len(tnumid) > 0 else ""}
        ''')]
    def get_playlists(self, tnumid: list[TagNumID | None]) -> list[PlaylistMetadata[int]]:
        return [PlaylistMetadata[int](
            id=PlaylistID(playlist[0]),
            title=playlist[1],
            description=playlist[2],
            epoch=int(playlist[4]),
            channel_id=ChannelUUID(playlist[5]),
            entries=int(playlist[3]),
            channel_name=playlist[6],
            channel_handle=ChannelHandle(playlist[7])
        ) for playlist in self.exec(f'''
            SELECT
                Playlist.id, Playlist.title, Playlist.description,
                Playlist.count, Playlist.epoch,
                Channel.id, Channel.title, Channel.handle
            FROM Playlist
            INNER JOIN Channel ON Playlist.channel_id=Channel.num_id
            {f'''JOIN (
                SELECT playlist_id
                FROM TaggedPlaylist
                WHERE tag_id IN ({",".join([str(x) for x in tnumid if isinstance(x,TagNumID)])})
                GROUP BY playlist_id
                HAVING COUNT(DISTINCT tag_id) = {len(tnumid)}
            ) AS tagged ON Playlist.num_id = tagged.playlist_id;''' if len(tnumid) > 0 else ""}
        ''')]

    def get_video_playlists(self, vid: VideoID) -> list[tuple[PlaylistNumID,int]]:
        return [
            (PlaylistNumID(a),b) for a,b in
            self.exec(
                "SELECT playlist_id, position FROM Pointer WHERE video_id = ?",
                (self.get_vnumid(vid),)
            )
        ]

    def get_videos_from_channel(self, cid: ChannelUUID) -> list[VideoMetadata]:
        return [VideoMetadata(
            id=VideoID(data[0]),
            title=data[1],
            description=data[2],
            upload_date=int(data[3]),
            duration=int(data[4]),
            epoch=data[5],
            channel_id=ChannelUUID(data[6]),
            channel_handle=ChannelHandle(data[8]),
            channel_name=data[7]
        ) for data in self.exec('''
            SELECT
                Video.id, Video.title, Video.description,
                Video.upload_date, Video.duration, Video.epoch,
                Channel.id, Channel.title, Channel.handle
            FROM Video
            INNER JOIN Channel ON Video.channel_id=Channel.num_id
            WHERE Video.channel_id=(SELECT num_id FROM Channel WHERE id=?)
        ''',(cid,))]
    def get_playlists_from_channel(self, cid: ChannelUUID) -> list[PlaylistMetadata[int]]:
        return [PlaylistMetadata[int](
            id=PlaylistID(playlist[0]),
            title=playlist[1],
            description=playlist[2],
            epoch=int(playlist[4]),
            channel_id=ChannelUUID(playlist[5]),
            channel_handle=ChannelHandle(playlist[7]),
            entries=int(playlist[3]),
            channel_name=playlist[6]
        ) for playlist in self.exec('''
            SELECT
                Playlist.id, Playlist.title, Playlist.description,
                Playlist.count, Playlist.epoch,
                Channel.id, Channel.title, Channel.handle
            FROM Playlist
            INNER JOIN Channel ON Playlist.channel_id=Channel.num_id
            WHERE Playlist.channel_id=(SELECT num_id FROM Channel WHERE id=?)
        ''',(cid,))]

    def get_vnumid(self, vid: VideoID | None) -> VideoNumID | None:
        if vid is None:
            return None
        data = self.exec("SELECT num_id FROM Video WHERE id=?",(vid,))
        if len(data)==0:
            return None
        return VideoNumID(data[0][0])
    def get_pnumid(self, pid: PlaylistID) -> PlaylistNumID | None:
        data = self.exec("SELECT num_id FROM Playlist WHERE id=?",(pid,))
        if len(data) == 0:
            return None
        return PlaylistNumID(data[0][0])
    def get_tnumid(self, tid: TagID) -> TagNumID | None:
        output = self.exec("SELECT num_id FROM Tag WHERE id=?",(tid,))
        if len(output) == 0:
            return None
        return TagNumID(output[0][0])

    def get_tag_info(self, tid: TagID) -> TagMetadata | None:
        output = self.exec("SELECT num_id,id,long_name FROM Tag WHERE id=?",(tid,))
        if len(output) == 0:
            return None
        return TagMetadata(
            num_id=TagNumID(output[0][0]),
            id=TagID(output[0][1]),
            long_name=output[0][2]
        )
    def create_tag(self, tid: TagID, description: str) -> TagNumID:
        db_out = self.exec(
            "INSERT INTO Tag(id,description) VALUES (?,?) RETURNING (num_id)",(
            tid,description))
        self.connection.commit()
        return TagNumID(db_out[0][0])
    def delete_tag(self, tid: TagID) ->  None:
        self.exec("DELETE FROM Tag WHERE id=?",(tid,))
        self.connection.commit()

    def add_tag_to_video(self, tnumid: TagNumID | None, vnumid: VideoNumID | None) -> bool:
        if tnumid is None or vnumid is None:
            return False
        self.exec(
            "INSERT OR REPLACE INTO TaggedVideo(tag_id,video_id) VALUES (?,?)",
            (tnumid, vnumid))
        self.connection.commit()
        return True
    def add_tag_to_playlist(self, tnumid: TagNumID | None, pnumid: PlaylistNumID | None) -> bool:
        if tnumid is None or pnumid is None:
            return False
        self.exec(
            "INSERT OR REPLACE INTO TaggedPlaylist(tag_id,playlist_id) VALUES (?,?)",
            (tnumid,pnumid))
        self.connection.commit()
        return True

    def get_video_tags(self, vid: VideoID | None) -> list[TagNumID]:
        return [
            TagNumID(x[0]) for x in
            self.exec("SELECT tag_id FROM TaggedVideo WHERE video_id = ?", (self.get_vnumid(vid),))
        ]
    def get_playlist_tags(self, pid: PlaylistID) -> list[TagNumID]:
        return [
            TagNumID(x[0]) for x in
            self.exec(
                "SELECT tag_id FROM TaggedPlaylist WHERE playlist_id = ?",
                (self.get_pnumid(pid),)
            )
        ]

    def remove_video(self, vid: VideoID) -> None:
        self.exec("DELETE FROM Video WHERE id=?", (vid,))
        self.connection.commit()

    def exit(self) -> None:
        self.connection.commit()
        self.connection.close()
