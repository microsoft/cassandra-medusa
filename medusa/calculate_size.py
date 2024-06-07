import json
import logging

from medusa.storage import Storage


def calculate_size(config, backup_names, exclude_backups):
    with Storage(config=config.storage) as storage:

        size_map = {}

        for backup_name in backup_names:
            node_backup = storage.get_node_backup(fqdn=storage.config.fqdn, name=backup_name)

            if not node_backup.exists():
                logging.warning('No such backup {}'.format(backup_name))
                continue

            manifest = node_backup.manifest
            if manifest is None:
                logging.warning('No manifest found for backup {}'.format(backup_name))
                continue

            for section in json.loads(manifest):
                for obj in section['objects']:
                    size_map[obj['path']] = obj['size']

        for backup_name in exclude_backups:
            node_backup = storage.get_node_backup(fqdn=storage.config.fqdn, name=backup_name)
            if not node_backup.exists():
                logging.warning('No such backup {}'.format(backup_name))
                continue

            manifest = node_backup.manifest
            if manifest is None:
                logging.warning('No manifest found for backup {}'.format(backup_name))
                continue

            for section in json.loads(manifest):
                for obj in section['objects']:
                    if obj['path'] in size_map:
                        del size_map[obj['path']]

        print("Total size of backups: {} bytes".format(sum(size_map.values())))
