import datetime as dt
import math
import shutil
import os
import re


def get_current_datetime():
    return dt.datetime.now().replace(microsecond=0)


def format_messages(message_list):
    final_str = []
    for i, msg in enumerate(message_list):
        final_str.append(f"{i+1}. {msg}")

    return "\n".join(final_str)


def format_instance_name(container_id, container_name, path_backed):
    # forbidden characters retrieved from https://stackoverflow.com/questions/1976007/what-characters-are-forbidden-in-windows-and-linux-directory-names
    # hyphens converted to % as well so as to not interfere with naming scheme

    path_backed_formatted = re.sub(r"[\\/:*?\"<>|]", "%", path_backed).replace("-", "%")
    container_name_formatted = re.sub(r"[\\/:*?\"<>|]", "%", container_name).replace(
        "-", "%"
    )

    return f"{container_id}-{container_name_formatted}-{path_backed_formatted}"


date_format_string = "%y%m%d"
df_dtypes = {
    "container_id": "str",
    "container_name": "str",
    "path_backed": "str",
    "container_dt_last_alive": "datetime64[ns]",
    "dt_last_backed": "datetime64[ns]",
    "size_last_backed": "int64",
    "dt_deleted": "datetime64[ns]",
}


def construct_backup_name(instance_name, date):
    date_formatted = date.strftime(date_format_string)
    backup_name = f"{instance_name}-{date_formatted}"

    return backup_name


def parse_to_datetime(date_string):
    # Use regex to truncate the string to second precision
    truncated_string = re.sub(r"(\.\d+)?Z$", "Z", date_string)

    # Parse the truncated string
    return dt.datetime.strptime(truncated_string, "%Y-%m-%dT%H:%M:%SZ")


def parse_filename(
    filename_w_extension,
):
    backup_name, extension = os.path.splitext(filename_w_extension)
    (
        container_id,
        container_name,
        path_backed_formatted,
        date_formatted,
    ) = backup_name.split("-")
    instance_name = format_instance_name(
        container_id, container_name, path_backed_formatted
    )
    date = dt.datetime.strptime(date_formatted, date_format_string)

    return instance_name, date


def uniquify(full_path):
    # Function from https://stackoverflow.com/a/57896232
    filename, extension = os.path.splitext(full_path)
    counter = 1

    while os.path.exists(full_path):
        full_path = f"{filename} ({counter}){extension}"
        counter += 1

    return full_path


def short(container_id):
    return container_id[:12]


def convert_bytes_to_readable(num_bytes):
    # Function from https://stackoverflow.com/a/14822210
    if num_bytes == 0:
        return "0 B"

    size_name = ["B", "KiB", "MiB", "GiB", "TiB"]
    i = int(math.floor(math.log(num_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(num_bytes / p, 2)
    size_str = f"{s} {size_name[i]}"

    return size_str


def check_available_space(path):
    total, _, free = shutil.disk_usage(path)
    free_percent = round(free / total * 100, 1)
    free_str = convert_bytes_to_readable(free)

    return free_percent, free_str
