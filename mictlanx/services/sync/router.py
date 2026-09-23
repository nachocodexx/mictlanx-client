
from typing import Dict,Any,List,Iterator,Union,Optional,Generator
import os 
import json as J
# 
from xolo.utils import Utils as XoloUtils
import requests as R
from option import Result,Ok,Err
# 
import ssl
import mictlanx.interfaces.responses as ResponseModels
import humanfriendly as HF
from mictlanx.services.models import PeerModel

VerifyType = Union[ssl.SSLContext,str,bool]


class Router:
    def __init__(self, peer_id: str, ip_addr: str, port: int, protocol: str = "http",api_version:int = 4):
        self.peer_id     = peer_id
        self.ip_addr     = ip_addr
        self.port        = port
        self.protocol    = protocol
        self.api_version = api_version

    def add_peers(self, peers:List[PeerModel],headers:Dict[str,str]={}, timeout:int=120):
        try:
            url = "{}/api/v4/xpeers".format(self.base_url())
            xs =list(map(lambda p: {
                "protocol":p.protocol,
                "hostname":p.ip_addr,
                "port":p.port,
                "peer_id":p.peer_id

            }, peers ))
            response = R.post(url=url, headers=headers,timeout=timeout,json=xs)
            response.raise_for_status()
        except Exception:
            pass

    def update_metadata(self,bucket_id:str, key:str, metadata:ResponseModels.Metadata, headers:Dict[str,str] ={}, timeout:int = 120)->Result[bool, Exception]:
        try:
            url = "{}/api/v4/u/buckets/{}/{}".format(self.base_url(),bucket_id,key)
            data_json = metadata.__dict__
            response = R.post(headers=headers,timeout=timeout,url=url,json=data_json)
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)


    def replication(self,
        rf:int,
        bucket_id:str,
        key:str,
        from_peer_id:Optional[str] ="",
        protocol:Optional[str]="http",
        strategy:Optional[str]="ACTIVE",
        ttl:Optional[int]=1,
        headers:Dict[str,str]={},
        timeout:int =120
    )->Result[ResponseModels.ReplicateResponse,Exception]:
        try:
            url = "{}/api/v4/replication".format(self.base_url())
            data_json = {
                # "id":id,
                "rtype":"DATA",
                "rf":rf,
                "from_peer_id":from_peer_id,
                "bucket_id":bucket_id,
                "key":key,
                # "memory":memory,
                # "disk":disk,
                # "workers":workers,
                "protocol":protocol,
                "strategy":strategy,
                "ttl":ttl
            }
            response = R.post(headers=headers,timeout=timeout,url=url,json=data_json)
            response.raise_for_status()
            return Ok(ResponseModels.ReplicationResponse(**response.json()))
        except Exception as e:
            return Err(e)
    def elastic(self,
            rf:int,
            # rtype:Optional[str] ="DATA",
            # from_peer_id:Optional[str]="",
            # bucket_id:Optional[str]="",
            # key:Optional[str]="",
            memory:Optional[int]=4000000000,
            disk:Optional[int]=40000000000,
            workers:Optional[int]=2,
            protocol:Optional[str]="http",
            strategy:Optional[str]="ACTIVE",
            ttl:Optional[int]=1,
            headers:Dict[str,str]={},
            timeout:int =120
    ):
        try:
            url = "{}/api/v4/elastic".format(self.base_url())
            response = R.post(headers=headers,timeout=timeout,url=url,json={
                "rtype":"SYSTEM",
                "rf":rf,
                # "from_peer_id":from_peer_id,
                # "bucket_id":bucket_id,
                # "key":key,
                "memory":memory,
                "disk":disk,
                "workers":workers,
                "protocol":protocol,
                "strategy":strategy,
                "ttl":ttl

            })
            response.raise_for_status()
            return Ok(ResponseModels.ElasticResponse(**response.json()))
        except Exception as e:
            return Err(e)
    def elastic_async(self, replicationEvent:Any,headers:Dict[str,str]={}, timeout:int =120):
        try:
            url = "{}/elastic/async".format(self.base_url())
            response = R.post(headers=headers,timeout=timeout,url=url)
            response.raise_for_status()
        except Exception as e:
            return Err(e)
        
    def get_replica_map(self, headers:Dict[str,str]={}, timeout:int =120):
        try:
            url = "{}/replicamap".format(self.base_url())
            response = R.post(headers=headers,timeout=timeout,url=url)
            response.raise_for_status()
        except Exception as e:
            return Err(e)
    def delete_by_ball_id(self,ball_id:str,bucket_id:str, timeout:int = 120,headers:Dict[str,str]={})->Result[ResponseModels.DeletedByBallIdResponse,Exception]:
        try:
            response = R.delete("{}/api/v{}/buckets/{}/bid/{}".format(self.base_url(),self.api_version,bucket_id,ball_id),timeout=timeout,headers=headers)
            response.raise_for_status()
            content_data = response.json()
            return Ok(ResponseModels.DeletedByBallIdResponse(**content_data))
        except R.RequestException as e:
            return Err(e)
        except Exception as e:
            return Err(e)

    def get_chunks_metadata(self,ball_id:str,bucket_id:str,timeout:int= 120,headers:Dict[str,str]={})->Result[Iterator[ResponseModels.Metadata],Exception]:

        try:
            response = R.get("{}/api/v{}/buckets/{}/metadata/{}/chunks".format(self.base_url(),self.api_version,bucket_id,ball_id),timeout=timeout,headers=headers)
            response.raise_for_status()
            chunks_metadata_json = map(lambda x: ResponseModels.Metadata(**x) ,response.json())
            return Ok(chunks_metadata_json)
        except R.RequestException as e:
            return Err(e)
        except Exception as e:
            return Err(e)
        
    def delete(self,bucket_id:str,key:str,headers:Dict[str,str]={})->Result[bool,Exception]:
        try:
            response = R.delete(
                "{}/api/v{}/buckets/{}/{}".format(self.base_url(), 4 , bucket_id,key),
                headers=headers
            )
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    def disable(self,bucket_id:str, key:str,headers:Dict[str,str]={})->Result[bool, Exception]:
        try:
            response = R.post(
                "{}/api/v{}/buckets/{}/{}/disable".format(self.base_url(), 4 , bucket_id,key),
                headers=headers
            )
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    
    def put_chuncked(self,task_id:str,chunks:Generator[bytes, None,None],timeout:int= 120,headers:Dict[str,str]={})->Result[ResponseModels.PeerPutChunkedResponse,Exception]:
        try:
            url = "{}/api/v{}/buckets/data/{}/chunked".format(self.base_url(), 4,task_id)
            put_response = R.post(url=url,
                data = chunks,
                timeout = timeout,
                stream=True,
                headers=headers
            )
            put_response.raise_for_status()
            json_response = J.loads(put_response.content)
            data = ResponseModels.PeerPutChunkedResponse(**json_response )
            return  Ok(data)
        except Exception as e:
            return Err(e)
    def empty():
        return Router(peer_id="",ip_addr="",port=-1)
    def get_addr(self)->str :
        return "{}:{}".format(self.ip_addr,self.port)
    def base_url(self):
        if self.port == -1 or self.port==0:
            return "{}://{}".format(self.protocol,self.ip_addr)
        return "{}://{}:{}".format(self.protocol,self.ip_addr,self.port)
    
    def get_metadata(self,bucket_id:str,key:str,timeout:int =300,headers:Dict[str,str]={})->Result[ResponseModels.GetMetadataResponse,Exception]:
        try:
            url = "{}/api/v{}/buckets/{}/metadata/{}".format(self.base_url(),4,bucket_id,key)
            get_metadata_response = R.get(url, timeout=timeout,headers=headers)
            get_metadata_response.raise_for_status()
            response = ResponseModels.GetMetadataResponse(**get_metadata_response.json() )
            return Ok(response)
        except Exception as e:
            return Err(e)
    
    
    def get_streaming(self,bucket_id:str,key:str,timeout:int=300,headers:Dict[str,str]={})->Result[R.Response, Exception]:
        
        try:
            url = "{}/api/v{}/buckets/{}/{}".format(self.base_url(),4,bucket_id,key)
            get_response = R.get(url, timeout=timeout,stream=True,headers=headers)
            get_response.raise_for_status()
            return Ok(get_response)
        except Exception as e:
            return Err(e)

    def get_to_file(self,
                    bucket_id:str,
                    key:str,
                    chunk_size:str="1MB",
                    sink_folder_path:str="/mictlanx/data",
                    timeout:int=300,
                    filename:str="",
                    headers:Dict[str,str]={},
                    extension:str =""
    )->Result[str,Exception]:
        try:
            _chunk_size = HF.parse_size(chunk_size)
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path,exist_ok=True)
            combined_key = XoloUtils.sha256("{}@{}".format(bucket_id,key).encode() ) if filename =="" else filename

            fullpath = "{}/{}{}".format(sink_folder_path,combined_key,extension)

            if os.path.exists(fullpath):
                return Ok(fullpath)
            url = "{}/api/v{}/buckets/{}/{}".format(self.base_url(),4,bucket_id,key)
            get_response = R.get(url, timeout=timeout,stream=True,headers=headers)
            get_response.raise_for_status()
            with open(fullpath,"wb") as f:
                for chunk in get_response.iter_content(chunk_size = _chunk_size):
                    if chunk:
                        f.write(chunk)
            return Ok(fullpath)
        except Exception as e:
            return Err(e)
        
    def get_by_checksum_to_file(self,
                    checksum:str,
                    chunk_size:str="1MB",
                    sink_folder_path:str="/mictlanx/data",
                    timeout:int=300,
                    filename:str="",
                    headers:Dict[str,str]={},
                    extension:str =""
    )->Result[str,Exception]:
        try:
            _chunk_size = HF.parse_size(chunk_size)
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path,exist_ok=True)
       
            combined_key = checksum if filename =="" else filename
            fullpath = "{}/{}{}".format(sink_folder_path,combined_key,extension)
            if os.path.exists(fullpath):
                return Ok(fullpath)
            url = "{}/api/v{}/buckets/checksum/{}".format(self.base_url(),4,checksum)
            get_response = R.get(url, timeout=timeout,stream=True,headers=headers)
            get_response.raise_for_status()
            with open(fullpath,"wb") as f:
                for chunk in get_response.iter_content(chunk_size = _chunk_size):
                    if chunk:
                        f.write(chunk)
            return Ok(fullpath)
        except Exception as e:
            return Err(e)
    def put_metadata(self, 
                     key:str,
                     size:int,
                     checksum:str,
                     producer_id:str,
                     content_type:str,
                     ball_id:str,
                     bucket_id:str,
                     tags:Dict[str,str]={},
                     timeout:int= 60*2,
                     is_disabled:bool = False,
                     replication_factor:int =1,
                     headers:Dict[str,str]={}
    )->Result[ResponseModels.PutMetadataResponse, Exception]:
            try:
                put_metadata_response =R.post("{}/api/v{}/buckets/{}/metadata".format(self.base_url(),4, bucket_id),json={
                    "bucket_id":bucket_id,
                    "key":key,
                    "ball_id":ball_id,
                    "checksum":checksum,
                    "size":size,
                    "tags":tags,
                    "producer_id":producer_id,
                    "content_type":content_type,
                    "is_disabled":is_disabled,
                    "replication_factor":replication_factor
                },
                timeout= timeout,headers=headers)
                put_metadata_response.raise_for_status()
                res_json = put_metadata_response.json()
                
                return Ok(ResponseModels.PutMetadataResponse(
                    bucket_id= res_json.get("bucket_id","BUCKET_ID"),
                    key= res_json.get("key","KEY"),
                    replicas=res_json.get("replicas",[]),
                    service_time=res_json.get("service_time",-1),
                    tasks_ids= res_json.get("tasks_ids","0"),
                 ))
            except Exception as e:
                return Err(e)
    def put_data(self,task_id:str,key:str, value:bytes, content_type:str,timeout:int= 60*2,headers:Dict[str,str]={},file_id:str="data") -> Result[Any, Exception]:
        try:
            put_response = R.post(
                "{}/api/v{}/buckets/data/{}".format(self.base_url(), 4,task_id),
                files= {
                    file_id:(key,value,content_type)
                },
                timeout = timeout,
                stream=True,
                headers=headers
            )
            put_response.raise_for_status()
            return  Ok(())
        except Exception as e:
            return Err(e)

    def get_bucket_metadata(self, bucket_id:str, timeout:int = 60*2,headers:Dict[str,str]={})->Result[ResponseModels.GetRouterBucketMetadataResponse,Exception]:
        try:
                url      = "{}/api/v4/buckets/{}/metadata".format(self.base_url(), bucket_id)
                response = R.get(url=url, timeout=timeout,headers=headers)
                response.raise_for_status()
                x_json = response.json()
                return Ok(ResponseModels.GetRouterBucketMetadataResponse.model_validate(x_json ))
        except Exception as  e:
            return Err(e)
    
    def delete(self,bucket_id:str,key:str, timeout:int = 60*2,headers:Dict[str,str]={})->Result[ResponseModels.DeletedByKeyResponse,Exception]:
        try:
                url      = "{}/api/v4/buckets/{}/{}".format(self.base_url(), bucket_id,key)
                response = R.delete(url=url, timeout=timeout,headers=headers)
                response.raise_for_status()
                json_data = response.json()
                return Ok(
                    ResponseModels.DeletedByKeyResponse(**json_data)
                )
        except Exception as  e:
            return Err(e)

    def get_ufs(self,timeout:int = 60*2,headers:Dict[str,str]={})->Result[ResponseModels.GetUFSResponse, Exception]:
        try:
            response = R.get("{}/api/v4/stats/ufs".format(self.base_url()),timeout=timeout,headers=headers)
            response.raise_for_status()
            return Ok(ResponseModels.GetUFSResponse(**response.json()))
        except Exception as e:
            return Err(e)
    def __eq__(self, __value: "Router") -> bool:
        if not isinstance(__value, Router):
            return False
        return (self.ip_addr == __value.ip_addr and self.port == __value.port) or self.router_id == __value.router_id
    def __str__(self):
        return "Router(id = {}, ip_addr={}, port={})".format(self.router_id, self.ip_addr,self.port)

    @staticmethod
    def from_str(x:str)->Result['Router', Exception]:
        x_splitted = x.split(":")
        n = len(x_splitted)
        if n ==0 or n < 3:
            return Err(Exception("Invalid string: {} - the correct format is <router_id>:<ip_addr:<port>".format(x)))
        port = x_splitted[2]
        return Router(
            router_id=x_splitted[0],
            ip_addr=x_splitted[1],
            port=port,
            protocol= "https" if port <= 0 else "http"
        )