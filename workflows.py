import os
import helpers
import pandas as pd
import custom_logging
import traceback
import numpy as np

logger, _ = custom_logging.get_logger(__name__)


def update_dt_deleted(row, instances_to_backup, dt_current):
    # If the instance is to be backed up, set the delete time to NaT
    # - possible for an instance marked as deleted to be un-deleted (ie. dt_deleted reset to NaT)
    # Else set the delete time to the current time
    if (
        row["container_id"] in instances_to_backup
        and row["path_backed"] in instances_to_backup[row["container_id"]]
    ):
        return pd.NaT
    elif pd.isna(row["dt_deleted"]):
        return dt_current
    return row["dt_deleted"]


def update_dt_last_alive(row, containers_dt_last_alive):
    if row["container_id"] in containers_dt_last_alive:
        return containers_dt_last_alive[row["container_id"]]
    else:
        return row["container_dt_last_alive"]


def get_container_dt_last_alive(container_id, dt_current, client):
    # We consider a container's last alive time to be the current time if it is still running,
    # or the time it finished if it has stopped
    inspection = client.inspect_container(container_id)
    dt_started_at = helpers.parse_to_datetime(inspection["State"]["StartedAt"])
    dt_finished_at = helpers.parse_to_datetime(inspection["State"]["FinishedAt"])

    if dt_started_at >= dt_finished_at:
        return dt_current
    return dt_finished_at


def update_instances(df, docker_client, container_paths, backup_by_default):
    instances_to_backup = {}
    new_rows = []
    # Get names of instances from dataframe
    current_instance_names = set(
        df.apply(
            lambda row: helpers.format_instance_name(
                row["container_id"], row["container_name"], row["path_backed"]
            ),
            axis=1,
        )
    )
    # Get shortened container IDs from config
    container_paths = {
        container_id[:12]: paths for container_id, paths in container_paths.items()
    }

    # Get names and IDs of all containers (both running and stopped)
    container_list = docker_client.containers(all=True)
    for container in container_list:
        short_id = helpers.short(container["Id"])
        container_name = container["Names"][0][1:]

        # Check for whether current container is specified in the config, and if so, retrieve the specified paths
        if short_id in container_paths and len(container_paths[short_id]):
            instances_to_backup[short_id] = container_paths[short_id]

        # Else if backup_by_default=True, then the entire filesystem is to be backed up
        elif backup_by_default:
            instances_to_backup[short_id] = ["/"]

        else:
            continue

        # Prepare data for new instances ie. those not backed up before
        for path in instances_to_backup[short_id]:
            instance_name = helpers.format_instance_name(short_id, container_name, path)
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
    # Actually append data of new instances to dataframe
    if len(new_rows):
        df = pd.concat(
            [df, pd.DataFrame(new_rows, columns=df.columns)],
            ignore_index=True,
        )

    # Update date_deleted column based on instances_to_backup
    initial_len = df["dt_deleted"].notna().sum()
    df["dt_deleted"] = df.apply(
        update_dt_deleted,
        axis=1,
        instances_to_backup=instances_to_backup,
        dt_current=helpers.get_current_datetime(),
    )
    final_len = df["dt_deleted"].notna().sum()
    if initial_len == final_len:
        logger.info(f"No. of instances to be backed up: {final_len}")
    else:
        logger.info(
            f"No. of instances to be backed up has changed from: {initial_len} --> {final_len}"
        )
    # Update container_dt_last_alive column for all containers
    containers_dt_last_alive = {
        helpers.short(container["Id"]): get_container_dt_last_alive(
            container["Id"], helpers.get_current_datetime(), docker_client
        )
        for container in container_list
    }
    df["container_dt_last_alive"] = df.apply(
        update_dt_last_alive, containers_dt_last_alive=containers_dt_last_alive, axis=1
    )

    return df


def prune_ghost_backups(df, backup_dir_path, ghost_backup_keep_days):
    total_prune_size = 0

    backup_filenames = os.listdir(backup_dir_path)

    # Get instances where it has been at least more than ghost_backup_keep_days days since it was marked as deleted
    deleted_df = df[(df["dt_deleted"].notna())]
    instances_to_prune = deleted_df[
        (helpers.get_current_datetime() - deleted_df["dt_deleted"]).dt.days
        >= ghost_backup_keep_days
    ]
    logger.info(f"Backups of {len(instances_to_prune)} ghost instances to be pruned.")

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
        (filename, helpers.parse_filename(filename)[0]) for filename in backup_filenames
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


def prune_extra_and_create_backups(
    df,
    client,
    backup_dir_path,
    min_backup_interval,
    backup_keep_num,
    warn_large_backup_mb
):
    total_prune_size = 0
    total_backup_size = 0

    backup_filenames = os.listdir(backup_dir_path)
    parsed_filename_list = [
        (filename, helpers.parse_filename(filename)) for filename in backup_filenames
    ]

    # Instance must be
    # - not marked as deleted
    # - either not backed up yet OR
    # - undergone changes since its last backup date AND backed up at least min_backup_interval days ago
    df_instances_to_backup = df[
        df["dt_deleted"].isna()
        & (
            df["dt_last_backed"].isna()
            | (
                (df["container_dt_last_alive"] > df["dt_last_backed"])
                & (
                    (helpers.get_current_datetime() - df["dt_last_backed"]).dt.days
                    >= min_backup_interval
                )
            )
        )
    ]

    containers_to_backup = set(df_instances_to_backup["container_id"].tolist())
    logger.info(
        f"{len(df_instances_to_backup)} instance(s) from {len(containers_to_backup)} container(s) require backup."
    )

    for row in df_instances_to_backup.itertuples():
        short_id, container_name, container_path = (
            row.container_id,
            row.container_name,
            row.path_backed,
        )
        instance_name = helpers.format_instance_name(
            short_id, container_name, container_path
        )
        instance_prune_size = 0
        instance_prune_count = 0

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
            backups.sort(key=lambda x: x[1])
            for i in range(len(backups) - backup_keep_num + 1):
                filename = backups[i][0]
                logger.info(f"Pruning backup: {filename}...")
                filesize = os.path.getsize(os.path.join(backup_dir_path, filename))
                try:
                    os.remove(os.path.join(backup_dir_path, filename))
                    total_prune_size += filesize
                    instance_prune_size += filesize
                    instance_prune_count += 1
                except:
                    logger.warning(f"Cannot prune backup: {filename}!")
                    logger.warning(traceback.format_exc())

        # Create backup of instance
        dt_backed = helpers.get_current_datetime()
        backup_name = helpers.construct_backup_name(instance_name, dt_backed)
        logger.info(f"Creating backup: {backup_name}...")
        try:
            bits, _ = client.get_archive(short_id, container_path, encode_stream=True)
            with open(f"{os.path.join(backup_dir_path, backup_name)}.gz", "wb") as file:
                for chunk in bits:
                    file.write(chunk)
            filesize = os.path.getsize(
                f"{os.path.join(backup_dir_path, backup_name)}.gz"
            )
            total_backup_size += filesize
            logger.info(
                f"Backup created: {helpers.convert_bytes_to_readable(filesize)}"
            )

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
            df.loc[label_to_update, "dt_last_backed"] = dt_backed
            df.loc[label_to_update, "size_last_backed"] = filesize

            # Check if size difference between pruned and created is more than warn_large_backup_mb
            if filesize - instance_prune_size > warn_large_backup_mb * (1024**2):
                logger.warning(
                    f"Large backup of {instance_name} detected! Pruned {instance_prune_count} backups: {helpers.convert_bytes_to_readable(instance_prune_size)}, Created: {helpers.convert_bytes_to_readable(filesize)}"
                )

        except:
            logger.warning(f"Error during backup creation: {backup_name}!")
            logger.warning(traceback.format_exc())

    return df, total_prune_size, total_backup_size
