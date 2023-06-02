"""
Incremental File-Based Backup Script

This script performs incremental file-based backups by comparing the
modification time of files with the last backup time. It creates backups
only for files that have been modified since the last backup.

Usage:
    python backup.py

Configuration:
    The script requires a configuration file named 'backup.conf' in the
    same directory. The configuration file should contain the following
    settings:
    - [backup] section:
        - source_path: The directory path of the files to be backed up.
        - backup_path: The directory path where the backups will be
          stored.
    - [timeframes] section:
        - backup_retention_time: The amount of time to retain each backup.
        - second_stage_backup_interval: The interval to keep backups after
          the retention time has passed.

Backup Process:
    1. The script reads the configuration file.
    2. It checks if the backup path exists and creates it if necessary.
    3. The last backup time is obtained from the configuration file.
    4. The script compares the modification time of each file in the
       source path with the last backup time.
    5. If a file has been modified since the last backup or it is the first
       backup run, a backup is created. The backup file is named with the
       original file name appended with the current date and time.
    6. The last backup time is updated in the configuration file.
    7. The script completes the backup process.

Logging:
    The script logs its activities to a file named 'backup.log' in the same
    directory. The log file provides information about the backup process,
    including files being backed up and any changes detected.

Note:
    - The script assumes that the source path and backup path are valid and
      accessible directories.
    - Review the log file for information and potential issues during the
      backup process.
"""


import os
import shutil
import hashlib
import configparser
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler

CONFIG_FILE = "backup.conf"


def parse_timeframe(timeframe):
    """Parse the time frame value into timedelta"""
    duration = int(timeframe[:-1])
    unit = timeframe[-1]

    if unit == "h":
        return timedelta(hours=duration)
    elif unit == "d":
        return timedelta(days=duration)
    else:
        raise ValueError("Invalid time frame unit. Only 'h' (hours) and 'd' (days) are supported.")


def generate_hash(file_path):
    """Generate a hash of the file contents"""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def create_backup(source_path, backup_path, last_backup_time, backup_retention_time, second_stage_backup_interval):
    """Create a backup of files that have changed since the last backup"""
    current_time = datetime.now()

    for root, _, files in os.walk(source_path):
        for file in files:
            file_path = os.path.join(root, file)
            rel_path = os.path.relpath(file_path, source_path)
            # backup_file_path = os.path.join(backup_path, rel_path)

            # Check if the file has been modified since the last backup
            modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if modified_time > last_backup_time or last_backup_time == datetime.min:

                # Generate the backup file name with the current date and time
                backup_time = current_time.strftime("%Y%m%d%H%M%S")
                backup_file_name, file_ext = os.path.splitext(file)
                new_file_name = f"{backup_file_name}_{backup_time}{file_ext}"
                backup_file_path = os.path.join(backup_path, rel_path, new_file_name)

                # Create necessary directories in the backup path
                os.makedirs(os.path.dirname(backup_file_path), exist_ok=True)

                # Copy the file to the backup path
                shutil.copy2(file_path, backup_file_path)

                logging.info(f"Backed up: {file_path} -> {backup_file_path}")
            else:
                logging.debug(f"No changes detected: {file_path}")


def remove_old_backups(backup_path, oldest_allowed_backup_time, second_stage_backup_interval):
    """Remove old backups based on the backup retention time and second stage backup interval"""
    backups = os.listdir(backup_path)

    for backup in backups:
        backup_time_str = backup.split("_")[1]  # Extract the backup time from the backup file name
        backup_time = datetime.strptime(backup_time_str, "%Y%m%d%H%M%S")

        if backup_time < oldest_allowed_backup_time:
            # Check if the backup is eligible for second stage backup
            if second_stage_backup_interval > 0:
                time_difference = (datetime.now() - backup_time).days
                if time_difference % second_stage_backup_interval == 0:
                    continue

            # Remove the backup
            backup_file_path = os.path.join(backup_path, backup)
            shutil.rmtree(backup_file_path)
            logging.info(f"Removed backup: {backup_file_path}")


def perform_backup():
    """Perform the backup based on the config file"""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            TimedRotatingFileHandler("backup.log", when='h', interval=1, backupCount=4),
            logging.StreamHandler()
        ]
    )

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    source_path = config.get("backup", "source_path")
    backup_path = config.get("backup", "backup_path")
    backup_retention_time = parse_timeframe(config.get("timeframes", "backup_retention_time"))
    second_stage_backup_interval = parse_timeframe(config.get("timeframes", "second_stage_backup_interval"))

    # Check if the backup path exists
    if not os.path.exists(backup_path):
        os.makedirs(backup_path)

    # Calculate the last backup time based on the backup retention time
    try:
        last_backup_time_str = config.get("backup", "last_backup_time")    
        last_backup_time = datetime.strptime(last_backup_time_str, "%Y-%m-%d %H:%M:%S")
    except(configparser.NoOptionError):
        last_backup_time = datetime.min

    logging.info("Starting backup process...")
    logging.debug(f"Source Path: {source_path}")
    logging.debug(f"Backup Path: {backup_path}")
    logging.debug(f"Backup Retention Time: {backup_retention_time}")
    logging.debug(f"Second Stage Backup Interval: {second_stage_backup_interval}")

    create_backup(source_path, backup_path, last_backup_time, backup_retention_time, second_stage_backup_interval)

    # Update the config file with the current backup time
    config.set("backup", "last_backup_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    with open(CONFIG_FILE, "w") as f:
        config.write(f)

    logging.info("Backup process completed.")


if __name__ == "__main__":
    perform_backup()

