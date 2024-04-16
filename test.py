from concurrent.futures import ThreadPoolExecutor
import random
import string
from time import time
import boto3  # type: ignore
import polars as pl

client = boto3.client(
    "s3",
    region_name="auto",
    endpoint_url="http://127.0.0.1:8000/api/s3",
    aws_access_key_id="abc",
    aws_secret_access_key="123",
)



other_data = ["".join(random.choices(string.ascii_letters, k=1000)) for _ in range(1000)]


def make_f(f: int) -> None:
    df = pl.DataFrame({'a': [random.uniform(0, 1) for _ in range(1000)], 'b': other_data})
    df.write_parquet(f"f_{f}.parquet")
    client.put_object(Bucket="adrian-test-123", Key=f"path/to/f_{f}.parquet", Body=open(f"f_{f}.parquet", "rb").read())


# TO TEST: run this once then uncomment for subsequent runs
with ThreadPoolExecutor(64) as executor:
    for f in range(128):
        executor.submit(make_f, f)
    executor.shutdown(wait=True)


storage_options = {
    "aws_access_key_id": "<secret>",
    "aws_secret_access_key": "<secret>",
    "aws_region": "us-east-1",
    "aws_endpoint_url": "http://127.0.0.1:8000/api/s3",
}
start = time()
df2 = pl.scan_parquet("s3://adrian-test-123/path/to/f_*.parquet", storage_options=storage_options).filter(pl.col("a") < 0).select(["a"]).count().collect()
end = time()
print(f'Time: {end - start:.2f}')
print(df2)
