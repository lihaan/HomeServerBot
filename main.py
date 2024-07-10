import os
import telegram
from telegram.constants import ParseMode
import datetime as dt
import traceback
import custom_logging
import asyncio
import helpers
import docker
import loader
import workflows

# Constants
curr_path = os.path.dirname(__file__)

# Default backup settings
default_config = {
    "min_backup_interval": 0,
    "ghost_backup_keep_days": float("inf"),  # mapped to -1
    "backup_keep_num": float("inf"),  # mapped to -1
    "warn_large_backup_mb": 1024,
    "backup_by_default": True,
    "container_paths": {},
    "archive_dir_path": curr_path,
    "telegram_chat_id": None,
    "telegram_bot_token": None,
}


async def main(df, bot):
    total_prune_size = 0
    total_backup_size = 0

    # Conduct disk usage check and send start message on telegram
    free_percent, free_str = helpers.check_available_space(backup_dir_path)
    message = (
        f"Starting backup script...\n"
        f"Remaining disk space: {free_percent}%, {free_str}"
    )
    logger.info(message)
    if bot:
        await bot.send_message(
            text=message,
            chat_id=config["telegram_chat_id"],
            parse_mode=ParseMode.HTML,
        )

    # Get docker client
    docker_client = None
    try:
        docker_client = docker.from_env().api
        logger.info("Docker client loaded!")
    except:
        logger.error("Cannot access docker service!")
        logger.error(traceback.format_exc())

    # If docker client is available, update information of all tracked instances
    if docker_client:
        try:
            df = workflows.update_instances(
                df.copy(),
                docker_client,
                config["container_paths"],
                config["backup_by_default"]
            )
        except:
            logger.error(
                "Error while updating instances information! Stopping other actions for safety reasons..."
            )
            logger.error(traceback.format_exc())
            df = None

    # Prune backups of instances that are marked as deleted
    try:
        df, prune_size = workflows.prune_ghost_backups(
            df.copy(), backup_dir_path, config["ghost_backup_keep_days"]
        )
        total_prune_size += prune_size
    except:
        logger.warning("Error while pruning ghost backups!")
        logger.warning(traceback.format_exc())

    # If docker client is available, backup tracked instances (that are not marked as deleted)
    if docker_client:
        # Find the instances that require backup, prune extra backups for each instance if necessary, then create backups
        try:
            df, prune_size, total_backup_size = (
                workflows.prune_extra_and_create_backups(
                    df.copy(),
                    docker_client,
                    backup_dir_path,
                    config["min_backup_interval"],
                    config["backup_keep_num"],
                    config["warn_large_backup_mb"]
                )
            )
            total_prune_size += prune_size
        except:
            logger.error("Error either while pruning extra or making backups!")
            logger.error(traceback.format_exc())

    # Save instance information to disk
    df.to_csv(instance_info_path, index=False)

    # Compile alert messages
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

    # print(error_message)

    # Send completion message on telegram
    free_percent, free_str = helpers.check_available_space(backup_dir_path)
    final_str = (
        f"{error_message}\n\n"
        f"Pruned: {helpers.convert_bytes_to_readable(total_prune_size)}, Created: {helpers.convert_bytes_to_readable(total_backup_size)}\n"
        f"Remaining disk space: {free_percent}%, {free_str}"
    )
    if bot:
        await bot.send_message(
            text=final_str,
            chat_id=config["telegram_chat_id"],
            parse_mode=ParseMode.HTML,
        )


async def main_bot_wrapped(df, bot):
    async with bot:
        await main(df, bot)


if __name__ == "__main__":
    config, instance_info_path, df, backup_dir_path = loader.load_and_parse_args(
        default_config, curr_path
    )

    logger, logLevelCountHandler = custom_logging.get_logger(__name__)

    if config["telegram_chat_id"] and config["telegram_bot_token"]:
        bot = telegram.Bot(config["telegram_bot_token"])
        logger.info("Connected to Telegram!")
        asyncio.run(main_bot_wrapped(df, bot))

    else:
        asyncio.run(main(df, None))

    logger.info("Gracefully exiting...")
