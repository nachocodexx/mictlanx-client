from mictlanx import AsyncClient
from mictlanx.retry import raf,RetryPolicy
import pandas as pd
import numpy as np
import asyncio
uri = "mictlanx://mictlanx-router-0@localhost:63666?api_version=4&protocol=http"

client = AsyncClient(
    client_id = "put_get_dataframe_client",
    uri       = uri

)


async def main_get():
    # GET dataframe as bytes, convert back to dataframe
    df_bytes_result = await raf(
        func   = lambda: client.get(bucket_id="dataframes", ball_id="iris1"),
        policy = RetryPolicy(
            retries= 3
        )
    )
    assert df_bytes_result.is_ok, f"Get operation failed: {df_bytes_result.unwrap_err()}"

    m       = df_bytes_result.unwrap()
    tags    = m.metadatas[0].tags
    shape   = eval(tags.get("shape", "(0,0)"))
    columns = tags.get("columns", "").split(",")

    np_bytes = m.data.tobytes()
    np_array     = np.frombuffer(np_bytes, dtype=np.float64).reshape(shape)
    df_recovered = pd.DataFrame(np_array, columns=columns)
    print(df_recovered.head())


async def main_put():

    df   = pd.read_csv("/source/iris.csv")
    data = df.to_numpy().tobytes()
    cols = ','.join(df.columns.tolist())

    result = await client.put(
        bucket_id = "dataframes",
        ball_id   = "iris1",
        value     = data,
        tags      = {
            "columns": cols, 
            "shape": str(df.shape)
        }
            
    )
    assert result.is_ok, f"Put operation failed: {result.unwrap_err()}"
    # # GET
    # df_bytes_result = await raf(
    #     func   = lambda: client.get(bucket_id="dataframes", ball_id="iris"),
    #     policy = RetryPolicy(
    #         retries= 3
    #     )
    # )
    # assert df_bytes_result.is_ok, f"Get operation failed: {df_bytes_result.unwrap_err()}"
    # np_bytes = df_bytes_result.unwrap().data.tobytes()
    # np_array     = np.frombuffer(np_bytes, dtype=np.float64).reshape(df.shape)
    # df_recovered = pd.DataFrame(np_array, columns=df.columns)
    # print(df_recovered.head())

if __name__ == "__main__":
    # asyncio.run(main_put())
    asyncio.run(main_get())

    # response = client.put("/dataframe", json=df.to_dict())
    # print(response.status_code)
    # print(response.json())