
from typing import Dict,Any,List,Iterator,Union,Generator
import os 
import json as J
# 
from xolo.utils import Utils as XoloUtils
import requests as R
from option import Result,Ok,Err
# 
import ssl
import mictlanx.interfaces.responses as ResponseModels
from retry.api import retry_call
import humanfriendly as HF

VerifyType = Union[ssl.SSLContext,str,bool]


class Peer:
    def __init__(self, peer_id:str, ip_addr:str, port:int,protocol:str="http"):
        self.peer_id = peer_id
        self.ip_addr = ip_addr
        self.port    = port
        self.protocol = protocol
    
    def flush_tasks(self,headers:Dict[str,str ]={}, timeout:int=120)->Result[bool,Exception]:
        try:
            url = "{}/api/v4/tasks".format(self.base_url())
            response = R.delete(url=url,headers=headers,timeout=timeout)
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    def get_all_ball_sizes(self,headers:Dict[str,str ]={}, timeout:int=120,start:int=0, end:int =0):
        try:
            url = "{}/api/v4/xballs/size?start={}{}".format(self.base_url(), start,"" if end <=0 else "&end={}".format(end) )
            response = R.get(url=url,headers=headers,timeout=timeout)
            response.raise_for_status()
            data_json = response.json()
            return Ok([ResponseModels.BallBasicData(*x) for x in data_json])
        except Exception as e:
            return Err(e)

    def get_balls_len(self,headers:Dict[str,str ]={}, timeout:int=120)->Result[int, Exception]:
        try:
            url = "{}/api/v4/balls/len".format(self.base_url())
            response = R.get(url=url,headers=headers,timeout=timeout)
            response.raise_for_status()
            data_json = response.json()
            return Ok(data_json.get("len",0))
        except Exception as e:
            return Err(e)
    

    def get_state(self,headers:Dict[str,str ]={}, timeout:int=120,start:int=0, end:int =0)->Result[ResponseModels.PeerCurrentState, Exception]:
        try:
            url = "{}/api/v4/peers/state?start={}{}".format(self.base_url(), start,"" if end <=0 else "&end={}".format(end) )
            response = R.get(url=url,headers=headers,timeout=timeout)
            response.raise_for_status()
            data_json = response.json()
            body = ResponseModels.PeerCurrentState(
                nodes= list(map(lambda x:ResponseModels.PeerData(**x), data_json.get("nodes",[]))),
                balls=dict(list(map(lambda x : (x[0], ResponseModels.BallContext(**x[1]) ),   data_json.get("balls", {}).items() )))
            )
            return Ok(body)
        except Exception as e :
            return Err(e)
    def get_stats(self,headers:Dict[str,str ]={}, timeout:int=120,start:int=0, end:int =0)->Result[ResponseModels.PeerStatsResponse, Exception]:
        try:
            url = "{}/api/v4/stats?start={}{}".format(self.base_url(), start,"" if end <=0 else "&end={}".format(end) )
            response = R.get(url=url,headers=headers,timeout=timeout)
            response.raise_for_status()
            data_json = response.json()
            body = ResponseModels.PeerStatsResponse(
                available_disk= data_json.get("available_disk",0), 
                balls= [ ResponseModels.Metadata(**b) for b in data_json.get("balls",[])], 
                disk_uf= data_json.get("disk_uf",0.0), 
                peer_id= data_json.get("peer_id","peer-id"),
                peers= data_json.get("peers",[]),
                total_disk= data_json.get("total_disk",0),
                used_disk= data_json.get("used_disk",0),
            )
            return Ok(body)
        except Exception as e :
            return Err(e)
    
    def get_balls(self,start:int=0, end:int=0,headers:Dict[str,str]={}, timeout=120)->Result[List[ResponseModels.BallBasicData], Exception]:
        try:
            url = "{}/api/v4/xballs?start={}{}".format(self.base_url(), start,"" if end <=0 else "&end={}".format(end)  )
            response = R.get(
                url= url, 
                headers=headers,
                timeout=timeout
            )
            response.raise_for_status()
            return Ok(list(map(lambda b: ResponseModels.BallBasicData(**b),response.json())))
        except Exception as e:
            return Err(e)

    def add_peer(self,id:str, disk:int, memory:int, ip_addr:str, port:int, weight:float, used_disk:int = 0, used_memory:int = 0, headers:Dict[str, str]={}, timeout:int = 3600)->Result[ResponseModels.StoragePeerResponse,Exception]:
        try:
            url      = "{}/api/v4/nodes".format(self.base_url())
            response = R.post(url, timeout=timeout, headers=headers, json={
                "id":id,
                "disk":disk,
                "memory":memory,
                "ip_addr":ip_addr,
                "port":port,
                "weight":weight,
                "used_disk":used_disk,
                "used_memory":used_memory,
            })
            response.raise_for_status()
            data_json = response.json()
            return Ok(ResponseModels.StoragePeerResponse(**data_json))
        except R.RequestException as e:
            if not e.response  == None:
                return Err(Exception(e.response.content.decode("utf-8")))
            else:
                return Err(Exception(str(e)))

        except Exception as e:
            return Err(e)
    def __add_peer(self,id:str, disk:int, memory:int, ip_addr:str, port:int, weight:float, used_disk:int = 0, used_memory:int = 0, headers:Dict[str, str]={}, timeout:int = 3600):
        result = self.add_peer(
            id= id,
            disk=disk,
            memory=memory,
            ip_addr=ip_addr,
            port=port,
            weight=weight,
            used_memory=used_memory,
            used_disk=used_disk,
            timeout=timeout,
            headers=headers
        )
        if result.is_err:
            raise result.unwrap_err()
        return result
    def add_peer_with_retry(self,
                            id:str,
                            disk:int,
                            memory:int,
                            ip_addr:str,
                            port:int,
                            weight:float,
                            used_disk:int = 0,
                            used_memory:int = 0,
                            headers:Dict[str, str]={},
                            timeout:int = 3600,
                            tries:int = 100,
                            delay:int = 1,
                            max_delay:int = 5,
                            jitter:float = 0.0,
                            backoff:float =1,
                            logger:Any = None
    )->Result[ResponseModels.StoragePeerResponse, Exception]:
        try:
            result = retry_call(
                self.__add_peer,
                fkwargs={
                    "id":id,
                    "disk":disk,
                    "memory":memory,
                    "ip_addr":ip_addr,
                    "port":port,
                    "weight":weight,
                    "used_disk":used_disk,
                    "used_memory":used_memory,
                    "timeout":timeout,
                    "headers": headers
                },
                tries=tries,
                delay=delay,
                max_delay=max_delay,
                jitter=jitter,
                backoff=backoff,
                logger=logger
            )
            return result
        except Exception as e:
            return Err(e)
    
        
    def replicate(self,bucket_id:str, key:str,timeout:int = 120,headers:Dict[str,str]={})->Result[ResponseModels.ReplicateResponse, Exception]:
        try:
            url      = "{}/api/v4/buckets/{}/{}/replicate".format(self.base_url(), bucket_id,key)
            response = R.post(url, timeout=timeout, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            return Ok(ResponseModels.ReplicateResponse(**data_json))
        except R.RequestException as e:
            if not e.response == None:
                return Err(Exception(e.response.content.decode("utf-8")))
            return Err(Exception(e))
        except Exception as e:
            return Err(e)

    def get_size(self,bucket_id:str, key:str, timeout:int = 120,headers:Dict[str,str]={})->Result[ResponseModels.GetSizeByKey,Exception]:
        try:
            url      = "{}/api/v4/buckets/{}/{}/size".format(self.base_url(), bucket_id,key)
            response = R.get(url, timeout=timeout, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            return Ok(ResponseModels.GetSizeByKey(
                **data_json
            ))
        except Exception as e:
            return Err(e)
    def delete(self,bucket_id:str,key:str, timeout:int = 60*2,headers:Dict[str,str]={})->Result[ResponseModels.DeletedByKeyResponse,Exception]:
        try:
            url      = "{}/api/v4/buckets/{}/{}".format(self.base_url(), bucket_id,key)
            response = R.delete(url=url, timeout=timeout,headers=headers)
            
            response.raise_for_status()
            return Ok(ResponseModels.DeletedByKeyResponse(
                n_deletes=int(response.headers.get("n-deletes",-1)),
                key=key
            ))
        except Exception as  e:
            return Err(e)
    def delete_by_ball_id(self,ball_id:str,bucket_id:str, timeout:int = 120,headers:Dict[str,str]={})->Result[ResponseModels.DeletedByBallIdResponse,Exception]:
        try:
            response = R.delete("{}/api/v{}/buckets/{}/bid/{}".format(self.base_url(),self.api_version,bucket_id,ball_id),timeout=timeout,headers=headers)
            response.raise_for_status()
            return Ok(ResponseModels.DeletedByBallIdResponse(
                n_deletes=int(response.headers.get("n-deletes",-1)),
                ball_id=ball_id
            ))
        except R.RequestException as e:
            return Err(e)
        except Exception as e:
            return Err(e)
    def get_chunks_metadata(self,ball_id:str,bucket_id:str,timeout:int= 60*2,headers:Dict[str,str]={})->Result[Iterator[ResponseModels.Metadata],Exception]:

        try:
            response = R.get("{}/api/v{}/buckets/{}/metadata/{}/chunks".format(self.base_url(),self.api_version,bucket_id,ball_id),timeout=timeout,headers=headers)
            response.raise_for_status()
            chunks_metadata_json = map(lambda x:ResponseModels.Metadata(**x) ,response.json())
            return Ok(chunks_metadata_json)
        except R.RequestException as e:
            return Err(e)
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
    

    def put_chuncked(self,task_id:str,chunks:Generator[bytes, None,None],timeout:int= 60*2,headers:Dict[str,str]={})->Result[ResponseModels.PeerPutChunkedResponse,Exception]:
        try:
            put_response = R.post(
                "{}/api/v{}/buckets/data/{}/chunked".format(self.base_url(), 4,task_id),
                data = chunks,
                timeout = timeout,
                stream=True,
                headers=headers
            )
            put_response.raise_for_status()
            data = ResponseModels.PeerPutChunkedResponse(**J.loads(put_response.content))
            return  Ok(data)
        except Exception as e:
            return Err(e)
    def empty():
        return Peer(peer_id="",ip_addr="",port=-1)
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
    def get_to_file(self,bucket_id:str,key:str,chunk_size:str="1MB",sink_folder_path:str="/mictlanx/data",timeout:int=300,filename:str="",headers:Dict[str,str]={})->Result[str,Exception]:
        try:
            _chunk_size = HF.parse_size(chunk_size)
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path,exist_ok=True)
            combined_key = XoloUtils.sha256("{}@{}".format(bucket_id,key).encode() ) if filename =="" else filename
            fullpath = "{}/{}".format(sink_folder_path,combined_key)
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
                     is_disable:bool = False,
                     headers:Dict[str,str]={}
    )->Result[ResponseModels.PeerPutMetadataResponse, Exception]:
            try:
                put_metadata_response =R.post("{}/api/v{}/buckets/{}/metadata".format(self.base_url(),4, bucket_id),json={
                    "key":key,
                    "size":size,
                    "checksum":checksum,
                    "tags":tags,
                    "producer_id":producer_id,
                    "content_type":content_type,
                    "ball_id":ball_id,
                    "bucket_id":bucket_id,
                    "is_disable":is_disable
                },
                timeout= timeout,headers=headers)
                put_metadata_response.raise_for_status()
                res_json = put_metadata_response.json()
                
                return Ok(ResponseModels.PeerPutMetadataResponse(
                    key= res_json.get("key","KEY"),
                    node_id=res_json.get("node_id","NODE_ID"),
                    service_time=res_json.get("service_time",-1),
                    task_id= res_json.get("task_id","0")
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

    def get_bucket_metadata(self, bucket_id:str, timeout:int = 60*2,headers:Dict[str,str]={})->Result[ResponseModels.GetBucketMetadataResponse,Exception]:
        try:
                url      = "{}/api/v4/buckets/{}/metadata".format(self.base_url(), bucket_id)
                response = R.get(url=url, timeout=timeout,headers=headers)
                response.raise_for_status()
                return Ok(ResponseModels.GetBucketMetadataResponse.model_validate(response.json()))
        except Exception as  e:
            return Err(e)
    

    def get_ufs(self,timeout:int = 60*2,headers:Dict[str,str]={})->Result[ResponseModels.GetUFSResponse, Exception]:
        try:
            response = R.get("{}/api/v4/stats/ufs".format(self.base_url()),timeout=timeout,headers=headers)
            response.raise_for_status()
            return Ok(ResponseModels.GetUFSResponse(**response.json()))
        except Exception as e:
            return Err(e)
    def __get_ufs(self,timeout:int = 60*2,headers:Dict[str,str]={}):
        res = self.get_ufs(timeout=timeout,headers=headers)
        if res.is_err:
            raise res.unwrap_err()
        return res

    def get_ufs_with_retry(self,
                            timeout:int=60,
                            headers:Dict[str,str]={},
                            tries:int = 100,
                            delay:int = 1,
                            max_delay:int = 5,
                            jitter:float = 0.0,
                            backoff:float =1,
                            logger:Any = None
                           ):
        try:
            result = retry_call(
                self.__get_ufs,
                fkwargs={
                "timeout":timeout,
                "headers":headers
                },
                tries=tries,
                delay=delay,
                max_delay=max_delay,
                jitter=jitter,
                backoff=backoff,
                logger=logger,
            )
            return result
        except Exception as e:
            return Err(e)
        
    def __eq__(self, __value: "Peer") -> bool:
        if not isinstance(__value,Peer) :
            return False
        return (self.ip_addr == __value.ip_addr and self.port == __value.port) or self.peer_id == __value.peer_id
    def __str__(self):
        return "Peer(id = {}, ip_addr={}, port={})".format(self.peer_id, self.ip_addr,self.port)