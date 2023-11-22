import json
import logging

from typing import List

from medusa.storage import Storage


def calculate_size(config, backup_names: List[str], exclude_two_backups: bool):
    storage = Storage(config=config.storage)

    valid_backups = []
    for backup_name in backup_names:
        node_backup = storage.get_node_backup(fqdn=storage.config.fqdn, name=backup_name)

        if not node_backup.exists():
            logging.warning('No such backup {}'.format(backup_name))
            continue

        manifest = node_backup.manifest
        if manifest is None:
            logging.warning('No manifest found for backup {}'.format(backup_name))
            continue

        valid_backups.append(backup_name)

    include_backups = valid_backups
    exclude_backups = []
    if exclude_two_backups:
        include_backups = valid_backups[:-2]
        exclude_backups = valid_backups[-2:]

    logging.info("Including backups: {}".format(include_backups))
    logging.info("Excluding backups: {}".format(exclude_backups))

    _calculate_size(config, include_backups, exclude_backups)


def _calculate_size(config, include_backups: List[str], exclude_backups: List[str]):
    storage = Storage(config=config.storage)
    size_map = {}

    for backup_name in include_backups:
        node_backup = storage.get_node_backup(fqdn=storage.config.fqdn, name=backup_name)
        manifest = node_backup.manifest
        for section in json.loads(manifest):
            for obj in section['objects']:
                size_map[obj['path']] = obj['size']

    for backup_name in exclude_backups:
        node_backup = storage.get_node_backup(fqdn=storage.config.fqdn, name=backup_name)
        manifest = node_backup.manifest
        for section in json.loads(manifest):
            for obj in section['objects']:
                if obj['path'] in size_map:
                    del size_map[obj['path']]

    logging.info("Total size of backups: {} bytes".format(sum(size_map.values())))
