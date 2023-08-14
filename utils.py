import re
import datetime


def get_hour(dt: str):
    """2023-08-08 11:00"""
    pattern = "\d{4}-\d{2}-\d{2} (\d{2}):\d{2}"
    result = re.findall(pattern, dt)

    return result[-1]


def convert_dt(dt: str) -> datetime.datetime:
    dt_format = "%Y-%m-%d %H:%M"
    pattern = "(\d{4}-\d{2}-\d{2}) \d{2}(:\d{2})"

    prev_dt = re.sub(pattern, r"\1 23\2", dt)

    result = datetime.datetime.strptime(prev_dt, dt_format) \
             + datetime.timedelta(hours=1)
    return result


def safe_cast(val, to_type, default="null"):
    try:
        return to_type(val)  #int(pm_10)
    except (ValueError, TypeError):
        return default


def get_datalake_bucket_name(layer, company, region, account, env):
    return f"{company}-{layer}-{region}-{account}-{env}"


def get_datalake_raw_layer_path(
        source, source_region, table,
        year=None, month=None, day=None, hour=None):
    path = f"{source}/{source_region}/{table}"

    if year:
        path += f"/year={year}"

    if month:
        path += f"/month={month}"

    if day:
        path += f"/day={day}"

    if hour:
        path += f"/hour={hour}"

    return path


