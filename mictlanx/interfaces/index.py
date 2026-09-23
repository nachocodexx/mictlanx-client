# import  aiofiles
from typing import List,Dict,Iterator
from option import Option,NONE,Some
import time as T
from xolo.utils.utils import Utils as XoloUtils
import humanfriendly as HF
import mictlanx.interfaces.responses as ResponseModels



class Ball:
    """Logical storage object composed of one or more chunk metadata records.

    A ``Ball`` groups all ``Metadata`` chunks that belong to the same logical
    object (identified by ``ball_id``) inside a bucket.  Call :meth:`build`
    after all chunks have been added to populate the derived fields
    (checksum, size, paths, timestamps).
    """

    def __init__(self,bucket_id:str,chunks:List[ResponseModels.Metadata]=[],ball_id:str="",checksum:str="" , bucket_relative_path:str="",fullname:str=""):
        self.bucket_id            = bucket_id
        self.ball_id              = ball_id
        self.checksum             = checksum
        self.size                 = 0
        self.chunks               = chunks.copy()
        self.bucket_relative_path = bucket_relative_path
        self.full_path            = ""
        self.extension            = ""
        self.filename             = ""
        self.updated_at           = -1
        self.fullname             = fullname
    def __len__(self):
        """Return the number of chunks currently associated with this ball."""
        sizes = map(lambda c: c.size, self.chunks)
        return sum(sizes)
        # return len(s)
    
    def __str__(self):
        return f"Ball(id={self.ball_id}, size = {self.size})"
    def len_chunks(self):
        """Return the number of chunk metadata records currently held.

        Returns:
            Integer chunk count.
        """
        return len(self.chunks)

    def add_chunk(self, chunk:ResponseModels.Metadata):
        """Append a chunk to this ball if it is not already present.

        Deduplication is based on matching ``key`` and ``checksum``.

        Args:
            chunk: ``Metadata`` record for the chunk to add.
        """
        exists = next(filter(lambda x: x.key == chunk.key and chunk.checksum ==x.checksum, self.chunks),-1)
        if exists == -1:
            self.chunks.append(chunk)

    def build(self):
        """Derive ball-level fields from the collected chunk metadata.

        Populates ``checksum``, ``ball_id``, ``bucket_relative_path``,
        ``fullname``, ``full_path``, ``extension``, ``filename``, ``size``,
        and ``updated_at`` by reading the first chunk's tags and summing
        sizes across all chunks.  No-op when ``chunks`` is empty.
        """
        if len(self.chunks) >0:
            c = self.chunks[0] 
            self.checksum             = c.tags.get("full_checksum","")
            self.ball_id              = c.ball_id
            self.bucket_relative_path = c.tags.get("bucket_relative_path","")
            self.fullname             = c.tags.get("fullname","")
            self.full_path            = c.tags.get("full_path","")
            self.extension            = c.tags.get("extension","")
            self.filename             = c.tags.get("filename","")
            # self.updated_at
        self.size = 0
        sum_updated_at = 0
        for c in self.chunks:
            self.size += c.size
            sum_updated_at += int(c.tags.get("updated_at",0))
        self.updated_at = int(sum_updated_at / len(self.chunks))
    
    def merge(self, other: 'Ball'):
        """Merge chunks from another ``Ball`` into this one, skipping duplicates.

        Deduplication is checksum-based.

        Args:
            other: The ``Ball`` whose chunks should be merged in.
        """
        existing_ids = {c.checksum for c in self.chunks}
        for chunk in other.chunks:
            if chunk.checksum not in existing_ids:
                self.chunks.append(chunk)

class Bucket:
    """Namespace grouping many :class:`Ball` objects under a single ``bucket_id``."""

    def __init__(self,bucket_id:str,balls:Dict[str, Ball]):
        self.bucket_id = bucket_id
        self.balls=balls.copy()

    def size_bytes(self)->int:
        """Return the total size of all balls in this bucket, in bytes.

        Returns:
            Aggregate size in bytes.
        """
        size = 0
        for b in self:
            size += b.size
        return size

    def size(self)->str:
        """Return a human-readable string of the total bucket size.

        Returns:
            Size string formatted by ``humanfriendly`` (e.g. ``"1.2 GB"``).
        """
        size = self.size_bytes()
        return HF.format_size(size)
            
    def __len__(self)->int:
        return len(self.balls)
    def __iter__(self) -> Iterator['Ball']:
        return iter(self.balls.values() )

class PeerStats(object):
    """In-memory statistics tracker for a single storage peer.

    Tracks cumulative put/get counters, disk usage, per-key access
    frequencies, and inter-arrival times.  Intended for use by the
    client-side load balancer and monitoring utilities.
    """

    def __init__(self,peer_id:str):
        self.__peer_id                 = peer_id
        self.total_disk:int            = 0
        self.used_disk                 = 0
        self.put_counter:int           = 0
        self.get_counter:int           = 0 
        self.balls                     = set()
        # 
        self.put_last_arrival_time     = -1
        self.put_sum_interarrival_time = 0
        
        self.get_last_arrival_time     = -1
        self.get_sum_interarrival_time = 0
        self.last_access_by_key:Dict[str,int]  = {}
        self.get_counter_per_key:Dict[str,int] = {}

    def put_frequency(self):
        """Return the fraction of all operations that were puts.

        Returns:
            Float in ``[0.0, 1.0]``, or ``0`` when no operations have occurred.
        """
        x =  self.global_counter()
        if  x == 0:
            return 0
        return self.put_counter / x

    def get_frequency(self):
        """Return the fraction of all operations that were gets.

        Returns:
            Float in ``[0.0, 1.0]``, or ``0`` when no operations have occurred.
        """
        x =  self.global_counter()
        if  x == 0:
            return 0
        return self.get_counter / x

    def get_frecuency_per_ball(self):
        """Return per-key get frequency relative to total get operations.

        Returns:
            Dict mapping each key to its fraction of total gets (``[0.0, 1.0]``).
        """
        res = {}
        for key, getcounter in self.get_counter_per_key.items():
            if self.get_counter == 0:
                res[key] = 0
            else:
                res[key] = getcounter / self.get_counter
        return res

    def top_N_by_freq(self,N:int):
        """Return the N most-frequently-accessed keys.

        Args:
            N: Number of top entries to return.

        Returns:
            List of ``(key, frequency)`` tuples sorted by frequency descending.
        """
        xs        = self.get_frecuency_per_ball()
        sorted_xs = list(sorted(xs.items(), key=lambda item: item[1], reverse=True))
        return sorted_xs[:N]

    def get_id(self):
        """Return the peer identifier.

        Returns:
            The ``peer_id`` string passed at construction.
        """
        return self.__peer_id


    
    def put(self,key:str, size:int):
        """Record a put operation for the given key.

        Args:
            key: The object key that was written.
            size: Size in bytes of the written object.
        """
        self.put_counter+=1
        if key not in self.balls:
            self.get_counter_per_key[key] = 0
            self.used_disk+=size
        self.balls.add(key)

    def get(self, key:str, size:int):
        """Record a get operation for the given key.

        Args:
            key: The object key that was read.
            size: Size in bytes of the read object.
        """
        arrival_time = T.time()
        self.get_counter += 1
        self.last_access_by_key.setdefault(key,arrival_time)
        if key not in self.get_counter_per_key:
            self.get_counter_per_key[key] = 1
        else:
            self.get_counter_per_key[key] += 1
        self.balls.add(key)

    def delete(self,key:str,size:int):
        """Record a delete operation and update disk usage.

        Args:
            key: The object key that was deleted.
            size: Size in bytes of the deleted object.
        """
        self.balls.discard(key)
        if self.used_disk >=size:
            self.used_disk-=size
        del self.get_counter_per_key[key]

    def calculate_disk_uf(self,size:int = 0 ):
        """Calculate the disk utilisation factor after a hypothetical write.

        Args:
            size: Hypothetical additional bytes to consider. Defaults to 0.

        Returns:
            Float in ``[0.0, 1.0]`` representing disk fullness.
        """
        return  1 - ((self.total_disk - (self.used_disk + size))/self.total_disk)

    def available_disk(self):
        """Return the number of free bytes on this peer's disk.

        Returns:
            Available bytes (``total_disk - used_disk``).
        """
        return self.total_disk - self.used_disk

    def global_counter(self):
        """Return the total number of put and get operations recorded.

        Returns:
            Integer sum of ``put_counter`` and ``get_counter``.
        """
        return self.put_counter + self.get_counter
    
    def __str__(self):
        

        return "PeerStats(peer_id={}, total_disk={}, used_disk={}, available_disk={}, disk_uf={}, puts={}, gets={}, globals={}, put_feq={}, get_feq={}, topN={})".format(
            self.__peer_id,
            self.total_disk,
            self.used_disk,
            self.available_disk(),
            self.calculate_disk_uf(),
            self.put_counter,
            self.get_counter,
            self.global_counter(),
            self.put_frequency(),
            self.get_frequency(),
            self.top_N_by_freq(3)
        )



