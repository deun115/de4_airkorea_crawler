import json                     # built-in library

import dotenv                   # installed packages library
import pyarrow as pa

from s3 import parquet_to_s3    # customed codes
from airkorea_api import request_airkorea_api, parse_airdata
from utils import get_datalake_bucket_name, get_datalake_raw_layer_path
from kafka import send_stream

# etl 과정 중 e에 해당하는 과정
# control+alt+l -> 라인 자동 정렬 (=convention sync)


def run_extract(mode="batch"):
    dotenv.load_dotenv()
    # 프로젝트 내부에 .env 파일이 있는지 먼저 검사하고, 해당하는 환경 변수를 가져오게 됨
    response = request_airkorea_api(
        station_name="마포구",
        page_no=1,
        data_term="MONTH"
    )

    print(response)

    if response.status_code != 200:
        return json.dumps(response)

    parsed_airdata = parse_airdata(response.content)
    print(parsed_airdata)

    if mode == "batch":
        # 배치 레이어 관련 코드
        pq = pa.Table.from_pydict({
            "event_time": [item["event_time"] for item in parsed_airdata],
            "pm_10": [item["pm_10"] for item in parsed_airdata],
            "o3": [item["o3"] for item in parsed_airdata],
            "no2": [item["no2"] for item in parsed_airdata],
            "co": [item["co"] for item in parsed_airdata],
            "so2": [item["so2"] for item in parsed_airdata]
        })
        bucket = get_datalake_bucket_name(
            layer="raw",
            company="de415",
            region="apnortheast2",
            account="073658113926",
            env="dev"
        )

        key = get_datalake_raw_layer_path(
            source="airkorea",
            source_region="kr",
            table="airdata",
            year=2023,
            month=9,
            day=9,
            hour=10
        )

        parquet_to_s3(pq=pq, bucket=bucket, key=f"{key}/airdata.parquet")

    elif mode == "streaming":
        send_stream(topic="stream-test", data=parsed_airdata, wait_for_seconds=10)
    else:   # mode 에러
        raise AttributeError(f"{mode}: 잘못된 모드입니다. mode 명을 확인해주세요!")


if __name__ == '__main__':
    run_extract(mode="streaming")
