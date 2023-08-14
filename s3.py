import boto3
import pyarrow as pa
from pyarrow import parquet


def parquet_to_s3(pq, bucket, key):
    parquet_buffer = pa.BufferOutputStream()
    parquet.write_table(pq, parquet_buffer)
    parquet_bytes = parquet_buffer.getvalue().to_pybytes()

    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=bucket, Key=key, Body=parquet_bytes)
    print("Parquet 파일이 성공적으로 S3로 업로드 되었습니다.")
