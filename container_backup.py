import logging
import os
import telegram
from telegram.constants import ParseMode
import datetime as dt
import traceback
import pandas as pd
import asyncio
import helpers
import yaml
import docker
import numpy as np

# Script configuration
# Constants
curr_path = os.path.dirname(__file__)
dt_current = dt.datetime.now()

# Load backup settings
custom_backup_config = None
default_backup_config = {
    "min_backup_interval": 0,
    "ghost_backup_keep_days": float("inf"), # mapped to -1
    "backup_keep_num": float("inf"), # mapped to -1
    "warn_large_backup_mb": 1024,
    "backup_by_default": True,
    "container_paths": {},
    "instance_info_dir_path": curr_path,
    "backup_dir_path": os.path.join(curr_path, "backups"),
    "log_dir_path": os.path.join(curr_path, "logs"),
    "telegram_chat_id": None,
    "telegram_bot_token": None,
}
backup_config = default_backup_config
backup_config_path = os.path.abspath(f"{curr_path}/backup_config.yaml")
if os.path.exists(backup_config_path):
    
    # Read from custom config and validate
    try:
        with open(backup_config_path, "r") as file:
            custom_backup_config = yaml.safe_load(file)

        if custom_backup_config is not None:
            for param, value in custom_backup_config.items():
                if value is None:
                    custom_backup_config[param] = default_backup_config[param]

            if custom_backup_config:
                backup_config = {**default_backup_config, **custom_backup_config}

            if backup_config["backup_keep_num"] == 0:
                raise ValueError("backup_keep_num cannot be 0!")
    except:
        print(f"Error reading from {backup_config_path}!")
        raise

    # Map arguments in validated custom config
    if backup_config["ghost_backup_keep_days"] == -1:
        backup_config["ghost_backup_keep_days"] = default_backup_config["ghost_backup_keep_days"]
    if backup_config["backup_keep_num"] == -1:
        backup_config["backup_keep_num"] = default_backup_config["backup_keep_num"]
    if backup_config["instance_info_dir_path"] == -1:
        backup_config["backup_keep_num"] = default_backup_config["backup_keep_num"]

else:
    print(
        f"Backup configuration not found at {backup_config_path}, using default settings..."
    )

# Configure logger and output log file
invalid_logfile_dir_path = None
if not os.path.exists(backup_config["log_dir_path"]):
    invalid_logfile_dir_path = backup_config["log_dir_path"]
    backup_config["log_dir_path"] = curr_path
logfile_path = helpers.uniquify(
    os.path.join(
        backup_config["log_dir_path"],
        f"container_backup_log_{dt_current.strftime(helpers.date_format_string)}.log",
    )
)
logging.basicConfig(
    filename=logfile_path,
    filemode="w",
    format="%(asctime)s, %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
if invalid_logfile_dir_path:
    logger.info(
        f"Log directory not found at {invalid_logfile_dir_path}, using {backup_config['log_dir_path']} instead..."
    )

# Load instance_info.csv
if not os.path.exists(backup_config["instance_info_dir_path"]):
    logger.warning(
        f"Instance information directory not found at {backup_config['instance_info_dir_path']}, using {curr_path} instead..."
    )
    backup_config["instance_info_dir_path"] = curr_path
instance_info_path = os.path.join(
    backup_config["instance_info_dir_path"], "instance_info.csv"
)
if os.path.exists(instance_info_path):
    try:
        df = pd.read_csv(
            instance_info_path,
            parse_dates=["container_dt_last_alive", "dt_last_backed", "dt_deleted"],
            date_format="%Y-%m-%d %H:%M:%S.%f",
        )
        logger.info(f"Found {len(df)} instances to track from {instance_info_path}.")
    except:
        logger.error(f"Error reading from {instance_info_path}!")
        raise
else:
    logger.warning(
        f"Instance information not found at {instance_info_path}, creating new file... (Ignore above message if this is the first time the script is run, or if you planned for a new tracking file to be created.)"
    )
    df = pd.DataFrame(
        data={c: pd.Series(dtype=t) for c, t in helpers.df_dtypes.items()}
    )


# Workflow-specific functions

def update_dt_deleted(row, not_deleted_instances):
    if (
        row["container_id"] in not_deleted_instances
        and row["path_backed"] in not_deleted_instances[row["container_id"]]
    ):
        return pd.NaT
    elif pd.isna(row["dt_deleted"]):
        return dt_current
    return row["dt_deleted"]


def get_container_dt_last_alive(container_id, dt_current, client):
    inspection = client.inspect_container(container_id)
    dt_started_at = dt.datetime.strptime(
        inspection["State"]["StartedAt"][:-4], "%Y-%m-%dT%H:%M:%S.%f"
    )
    dt_finished_at = dt.datetime.strptime(
        inspection["State"]["FinishedAt"][:-4], "%Y-%m-%dT%H:%M:%S.%f"
    )

    if dt_started_at >= dt_finished_at:
        return dt_current
    return dt_finished_at


def update_instances(df, client, container_paths, backup_by_default):
    container_list = client.containers(all=True)

    not_deleted_instances = {}
    new_rows = []
    current_instance_names = set(
        df.apply(
            lambda row: helpers.format_instance_name(
                row["container_id"], row["container_name"], row["path_backed"]
            ),
            axis=1,
        )
    )
    for container in container_list:
        short_id = helpers.short(container["Id"])
        container_name = container["Names"][0][1:]

        # If container_id is specified in container_paths, and list is not empty, its not-deleted instances are only those specified
        if short_id in container_paths and len(container_paths[short_id]):
            path_list = container_paths[short_id]

        # Else if backup_by_default=True, then the instance corresponding to the root path is considered not-deleted
        elif backup_by_default:
            path_list = ["/"]

        else:
            continue

        not_deleted_instances[short_id] = set(path_list)

        # Update df with new instances ie. not backed up before
        for path in path_list:
            instance_name = helpers.format_instance_name(
                short_id, container_name, path
            )
            if instance_name not in current_instance_names:
                new_rows.append(
                    {
                        "container_id": short_id,
                        "container_name": container_name,
                        "path_backed": path,
                        "container_dt_last_alive": pd.NaT,
                        "dt_last_backed": pd.NaT,
                        "size_last_backed": np.nan,
                        "dt_deleted": pd.NaT,
                    }
                )
    if len(new_rows):
        df = pd.concat(
            [df, pd.DataFrame(new_rows, columns=df.columns)],
            ignore_index=True,
        )

    # Update date_deleted based on not_deleted_instances
    initial_len = df["dt_deleted"].notna().sum()
    df["dt_deleted"] = df.apply(
        update_dt_deleted, axis=1, not_deleted_instances=not_deleted_instances
    )
    final_len = df["dt_deleted"].notna().sum()
    logger.info(f"Marked {final_len - initial_len} instances as deleted.")

    # Update container_dt_last_alive based on current containers
    containers_dt_last_alive = {
        helpers.short(container["Id"]): get_container_dt_last_alive(
            container["Id"], dt_current, client
        )
        for container in container_list
    }
    df["container_dt_last_alive"] = df.apply(
        lambda row: containers_dt_last_alive[row["container_id"]], axis=1
    )
    
    return df



def prune_ghost_backups(df, backup_dir_path, ghost_backup_keep_days):
    total_prune_size = 0

    backup_filenames = []
    try:
        backup_filenames = os.listdir(backup_dir_path)
    except:
        logger.warning(
            f"Backup directory not found at {backup_dir_path}! Instance information will be updated but no ghost backups will be pruned."
        )
        logger.warning(traceback.format_exc())

    # Get instances where it has been at least more than ghost_backup_keep_days days since it was marked as deleted
    deleted_df = df[(df["dt_deleted"].notna())]
    instances_to_prune = deleted_df[
        (dt_current - deleted_df["dt_deleted"]).dt.days >= ghost_backup_keep_days
    ]
    logger.info(
        f"Backups of {len(instances_to_prune)} ghost instances to be pruned."
    )

    if not len(instances_to_prune):
        return df, total_prune_size

    instance_names = instances_to_prune.apply(
        lambda row: helpers.format_instance_name(
            row["container_id"], row["container_name"], row["path_backed"]
        ),
        axis=1,
    )
    labels = instances_to_prune.index
    parsed_filename_list = [
        (filename, helpers.parse_filename(filename)[0])
        for filename in backup_filenames
    ]

    pruned_instance_label_list = []
    for instance_name_to_prune, label in zip(instance_names, labels):
        # Filter list of backups for the current iteration's instance
        filenames = [
            filename
            for filename, instance_name in parsed_filename_list
            if instance_name == instance_name_to_prune
        ]
        if not len(filenames):
            logger.warning(f"Cannot find backups for {instance_name_to_prune}.")
            continue

        # Delete found backups
        pruned_backups_count = 0
        for filename in filenames:
            logger.info(f"Pruning backup: {filename}...")
            filesize = os.path.getsize(os.path.join(backup_dir_path, filename))
            try:
                os.remove(os.path.join(backup_dir_path, filename))
                pruned_backups_count += 1
                total_prune_size += filesize
            except:
                logger.warning(f"Cannot prune backup: {filename}!")
                logger.warning(traceback.format_exc())

        # Only drop instance when all its corresponding backups are pruned
        if pruned_backups_count == len(filenames):
            pruned_instance_label_list.append(label)
    logger.info(
        f"{len(pruned_instance_label_list)} out of {len(instance_names)} instances successfully pruned!"
    )
    df = df.drop(pruned_instance_label_list)

    return df, total_prune_size


def find_instances_to_backup(df, min_backup_interval):
    container_ids_to_backup = set()

    # Only consider not-deleted instances
    df_not_deleted = df[df["dt_deleted"].isna()]
    for row in df_not_deleted.itertuples(index=False):
        if row.container_id in container_ids_to_backup:
            continue

        # Backup container if instance is new ie. has not been backup up before
        if pd.isna(row.dt_last_backed):
            container_ids_to_backup.add(row.container_id)
            continue

        # Backup container if it has undergone changes since its last backup, and if at least min_backup_interval days has passed
        if (
            row.container_dt_last_alive > row.dt_last_backed
            and (dt_current - row.dt_last_backed).days >= min_backup_interval
        ):
            container_ids_to_backup.add(row.container_id)
            continue
    logger.info(f"{len(container_ids_to_backup)} container(s) require backup.")

    return df, container_ids_to_backup


def prune_extra_and_create_backups(
    df, client, container_ids_to_backup, backup_dir_path, backup_keep_num
):
    total_prune_size = 0
    total_backup_size = 0
    try:
        backup_filenames = os.listdir(backup_dir_path)
    except:
        logger.error(
            f"Backup directory not found at {backup_dir_path}! Cannot prune extra or create backups!"
        )
        raise

    parsed_filename_list = [
        (filename, helpers.parse_filename(filename))
        for filename in backup_filenames
    ]
    for short_id in container_ids_to_backup:
        paths = df[df["container_id"] == short_id]["path_backed"]
        container_name = df[df["container_id"] == short_id][
            "container_name"
        ].values[0]
        assert (
            df[df["container_id"] == short_id]["container_name"] == container_name
        ).all(), "Containers with the same ID have different name!"
        instance_prune_size = 0
        for container_path in paths:
            instance_name = helpers.format_instance_name(
                short_id, container_name, container_path
            )
            backups = [
                (filename, backup_date)
                for filename, (
                    backup_instance_name,
                    backup_date,
                ) in parsed_filename_list
                if backup_instance_name == instance_name
            ]

            # Count existing backups to see if pruning is required
            if len(backups) >= backup_keep_num:
                logger.info(
                    f"{instance_name} has {len(backups)}/{backup_keep_num} backups..."
                )

                # Prune oldest first, until number of backups is one less than backup_keep_num
                backups.sort(key=lambda x: x[0])
                pruned_backups_count = 0
                for i in range(len(backups) - backup_keep_num + 1):
                    filename = backups[i][0]
                    logger.info(f"Pruning backup: {filename}...")
                    filesize = os.path.getsize(
                        os.path.join(backup_dir_path, filename)
                    )
                    try:
                        os.remove(os.path.join(backup_dir_path, filename))
                        pruned_backups_count += 1
                        total_prune_size += filesize
                        instance_prune_size += filesize
                    except:
                        logger.warning(f"Cannot prune backup: {filename}!")
                        logger.warning(traceback.format_exc())

            # Create new backup of instance
            backup_name = helpers.construct_backup_name(instance_name, dt_current)
            logger.info(f"Creating backup: {backup_name}...")
            try:
                bits, _ = client.get_archive(
                    short_id, container_path, encode_stream=True)
                with open(
                    f"{os.path.join(backup_dir_path, backup_name)}.gz", "wb"
                ) as file:
                    for chunk in bits:
                        file.write(chunk)
                filesize = os.path.getsize(f"{os.path.join(backup_dir_path, backup_name)}.gz")
                total_backup_size += filesize

                # Update dt_last_backed and size_last_backed
                label_to_update = df[
                    df.apply(
                        lambda row: helpers.format_instance_name(
                            row["container_id"],
                            row["container_name"],
                            row["path_backed"],
                        ),
                        axis=1,
                    )
                    == instance_name
                ].index
                assert (
                    len(label_to_update) == 1
                ), "Multiple rows with the same instance name found!"
                label_to_update = label_to_update[0]
                df.loc[label_to_update, "dt_last_backed"] = dt_current
                df.loc[label_to_update, "size_last_backed"] = filesize

                # Check if difference between pruned and created is more than warn_large_backup_mb
                if filesize - instance_prune_size > backup_config[
                    "warn_large_backup_mb"
                ] * (1024**2):
                    logger.warning(
                        f"Large backup of {instance_name} detected! Pruned: {helpers.convert_bytes_to_readable(instance_prune_size)}, Created: {helpers.convert_bytes_to_readable(filesize)}"
                    )

                logger.info(
                    f"Backup created: {helpers.convert_bytes_to_readable(filesize)}"
                )
            except:
                logger.warning(f"Cannot create backup: {backup_name}!")
                logger.warning(traceback.format_exc())

    return df, total_prune_size, total_backup_size

async def main(df, bot):
    total_prune_size = 0
    total_backup_size = 0
    
    # Conduct disk usage check
    if os.path.exists(backup_config["backup_dir_path"]):
        free_percent, free_str = helpers.check_available_space(
            backup_config["backup_dir_path"]
        )
        message = (
            f"Starting backup script...\n"
            f"Remaining disk space: {free_percent}%, {free_str}"
        )
        logger.info(message)
        if bot:
            await bot.send_message(
                text=message, chat_id=telegram_chat_id, parse_mode=ParseMode.HTML
            )

    client = None
    try:
        client = docker.from_env().api
        logger.info("Docker client loaded!")
    except:
        logger.error("Cannot access docker service!")
        logger.error(traceback.format_exc())

    if client:
        try:
            df = update_instances(
                df.copy(),
                client,
                backup_config["container_paths"],
                backup_config["backup_by_default"],
            )
        except:
            logger.error("Error while updating instances information! Stopping other actions for safety reasons...")
            logger.error(traceback.format_exc())
            df = None

    if df is not None:
        try:
            df, prune_size = prune_ghost_backups(
                df.copy(),
                backup_config["backup_dir_path"],
                backup_config["ghost_backup_keep_days"],
            )
            total_prune_size += prune_size
        except:
            logger.warning("Error while pruning ghost backups!")
            logger.warning(traceback.format_exc())

        if client:
            try:
                df, container_ids_to_backup = find_instances_to_backup(
                    df.copy(), backup_config["min_backup_interval"]
                )
            except:
                logger.warning("Error while finding containers to backup!")
                logger.warning(traceback.format_exc())
                container_ids_to_backup = set()

            if len(container_ids_to_backup):
                try:
                    df, prune_size, total_backup_size = prune_extra_and_create_backups(
                        df.copy(),
                        client,
                        container_ids_to_backup,
                        backup_config["backup_dir_path"],
                        backup_config["backup_keep_num"],
                    )
                    total_prune_size += prune_size
                except:
                    logger.error("Error either while pruning extra or making backups!")
                    logger.error(traceback.format_exc())

        df.to_csv(instance_info_path, index=False)

    error_message = []
    if len(logLevelCountHandler.level_messages["CRITICAL"]) > 0:
        error_message.append(
            (
                f"Critical error(s) encountered!\n"
                f"{helpers.format_messages(logLevelCountHandler.level_messages['CRITICAL'])}"
            )
        )
    if len(logLevelCountHandler.level_messages["ERROR"]) > 0:
        error_message.append(
            (
                f"Error(s) encountered!\n"
                f"{helpers.format_messages(logLevelCountHandler.level_messages['ERROR'])}"
            )
        )
    if len(logLevelCountHandler.level_messages["WARNING"]) > 0:
        error_message.append(
            (
                f"Warning(s) encountered!\n"
                f"{helpers.format_messages(logLevelCountHandler.level_messages['WARNING'])}"
            )
        )
    error_message = "\n\n".join(error_message)

    print(error_message)

    if os.path.exists(backup_config["backup_dir_path"]):
        free_percent, free_str = helpers.check_available_space(
            backup_config["backup_dir_path"]
        )
        final_str = (
            f"{error_message}\n\n"
            f"Pruned: {helpers.convert_bytes_to_readable(total_prune_size)}, Created: {helpers.convert_bytes_to_readable(total_backup_size)}\n"
            f"Remaining disk space: {free_percent}%, {free_str}"
        )
        if bot:
            await bot.send_message(
                text=final_str, chat_id=telegram_chat_id, parse_mode=ParseMode.HTML
            )

async def main_bot_wrapped(df, bot):
    async with bot:
        await main(df, bot)
    

if __name__ == "__main__":
    logLevelCountHandler = helpers.LogLevelHandler()
    logger.addHandler(logLevelCountHandler)

    # Loading secrets
    telegram_chat_id = backup_config["telegram_chat_id"]
    bot_token = backup_config["telegram_bot_token"]

    if telegram_chat_id and bot_token:
        bot = telegram.Bot(bot_token)
        logger.info("Connected to Telegram!")
        asyncio.run(main_bot_wrapped(df, bot))
    
    else:
        asyncio.run(main(df, None))
    
    logger.info("Gracefully exiting...")