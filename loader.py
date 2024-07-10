import os
import yaml
import logging
import custom_logging
import helpers
import pandas as pd

CONFIG_FILENAME = "config.yaml"
BACKUPS_DIR_FILENAME = "backups"
LOGS_DIR_FILENAME = "logs"
INSTANCE_INFO_FILENAME = "instance_info.csv"


def load_and_parse_args(default_config, curr_path):
    config = default_config.copy()

    config_path = os.path.abspath(f"{curr_path}/{CONFIG_FILENAME}")

    if os.path.exists(config_path):
        # Read from custom config and validate
        try:
            with open(config_path, "r") as file:
                custom_config = yaml.safe_load(file)

            if custom_config is not None:
                for key, value in custom_config.items():
                    if value is None:
                        raise ValueError("Do not leave any parameters blank! Comment them out if you want to use default values.")
                config = {**default_config, **custom_config}

            if not (
                isinstance(config["min_backup_interval"], int)
                and (config["min_backup_interval"] >= 0)
            ):
                raise ValueError(
                    f"min_backup_interval expected a non-negative integer but got {type(config['min_backup_interval']).__name__}:{config['min_backup_interval']}. 0 means that if the container is currently running, backups are made every time that the script is run."
                )
            if not (
                isinstance(config["ghost_backup_keep_days"], int)
                and (config["ghost_backup_keep_days"] >= -1)
            ):
                raise ValueError(
                    f"ghost_backup_keep_days expected either -1 (disable ghost backup pruning) or a non-negative integer, but got {type(config['ghost_backup_keep_days']).__name__}:{config['ghost_backup_keep_days']}"
                )
            if not (
                isinstance(config["backup_keep_num"], int)
                and (config["backup_keep_num"] == -1 or config["backup_keep_num"] > 0)
            ):
                raise ValueError(
                    f"backup_keep_num expected either -1 (disable old backup pruning) or a positive integer, but got {type(config['backup_keep_num']).__name__}:{config['backup_keep_num']}"
                )
            if not (
                isinstance(config["warn_large_backup_mb"], int)
                and (config["warn_large_backup_mb"] >= 0)
            ):
                raise ValueError(
                    f"warn_large_backup_mb expected a non-negative integer, but got {type(config['warn_large_backup_mb']).__name__}:{config['warn_large_backup_mb']}"
                )
            if not isinstance(config["backup_by_default"], bool):
                raise ValueError(
                    f"backup_by_default expected a boolean value, but got {type(config['backup_by_default']).__name__}:{config['backup_by_default']}"
                )
            if not isinstance(config["container_paths"], dict):
                raise ValueError(
                    f"container_paths expected a dictionary, but got {type(config['container_paths']).__name__}:{config['container_paths']}"
                )
            for container_id, paths in config["container_paths"].items():
                if not (isinstance(container_id, str) or container_id.is_numeric()):
                    raise ValueError(
                        f"Error for container {container_id}: Container ID expected a string (or numeric), but got {type(container_id).__name__}:{container_id}"
                    )
                if not isinstance(paths, list):
                    raise ValueError(
                        f"Error for container {container_id}: Expected a list, but got {type(paths).__name__}:{paths}"
                    )
                elif len(paths) == 0:
                    raise ValueError(
                        f"Error for container {container_id}: List of paths cannot be empty! Place a '/' if you wish to indicate the root path"
                    )
                for path in paths:
                    if not isinstance(path, str):
                        raise ValueError(
                            f"Error for container {container_id}: Expected a string path, but got {type(path).__name__}:{path}"
                        )
            if not isinstance(config["archive_dir_path"], str):
                raise ValueError(
                    f"archive_dir_path expected a string, but got {type(config['archive_dir_path']).__name__}:{config['archive_dir_path']}"
                )

        except yaml.YAMLError as e:
            print(f"Error parsing YAML in {config_path}: {e}")
            raise
        except ValueError as e:
            print(f"Invalid configuration in {config_path}: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error reading {config_path}: {e}")
            raise

        # Map arguments in validated custom config
        if config["ghost_backup_keep_days"] == -1:
            config["ghost_backup_keep_days"] = default_config["ghost_backup_keep_days"]
        if config["backup_keep_num"] == -1:
            config["backup_keep_num"] = default_config["backup_keep_num"]

    else:
        print(
            f"Backup configuration not found at {config_path}, using default settings..."
        )

    # Check archive_dir_path
    archive_dir_exists = True
    if not os.path.exists(config["archive_dir_path"]):
        archive_dir_exists = False
        os.makedirs(config["archive_dir_path"])

    # Check log directory
    log_dir_path = os.path.join(config["archive_dir_path"], LOGS_DIR_FILENAME)
    if not os.path.exists(log_dir_path):
        os.makedirs(log_dir_path)

    # Set path of log file for current run
    logfile_path = helpers.uniquify(
        os.path.join(
            log_dir_path,
            f"container_backup_log_{helpers.get_current_datetime().strftime(helpers.date_format_string)}.log",
        )
    )
    # Configure logging parameters
    custom_logging.init_logging(logfile_path)
    logger, _ = custom_logging.get_logger(__name__)
    if not archive_dir_exists:
        logger.warning(
            f"Creating new archive directory at {config['archive_dir_path']}"
        )
    else:
        logger.info(f"Archiving at {config['archive_dir_path']}")

    # Load instance_info
    instance_info_path = os.path.join(
        config["archive_dir_path"], INSTANCE_INFO_FILENAME
    )
    if os.path.exists(instance_info_path):
        try:
            df = pd.read_csv(
                instance_info_path,
                parse_dates=["container_dt_last_alive", "dt_last_backed", "dt_deleted"],
                date_format="%Y-%m-%d %H:%M:%S",
            )
            df["container_dt_last_alive"] = pd.to_datetime(
                df["container_dt_last_alive"]
            )
            df["dt_last_backed"] = pd.to_datetime(df["dt_last_backed"])
            df["dt_deleted"] = pd.to_datetime(df["dt_deleted"])
            logger.info(
                f"Found {len(df)} instances to track from {instance_info_path}."
            )
        except:
            logger.error(f"Error reading from {instance_info_path}!")
            raise
    else:
        logger.info(
            f"Creating new {INSTANCE_INFO_FILENAME} file at {instance_info_path}..."
        )
        df = pd.DataFrame(
            data={c: pd.Series(dtype=t) for c, t in helpers.df_dtypes.items()}
        )

    # Check backups directory
    backup_dir_path = os.path.join(config["archive_dir_path"], BACKUPS_DIR_FILENAME)
    if not os.path.exists(backup_dir_path):
        os.makedirs(backup_dir_path)

    return config, instance_info_path, df, backup_dir_path
