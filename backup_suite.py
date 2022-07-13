from backup_service import BackupService



class BackupSuite:
    configs: tuple[BackupService]

    def __init__(self, *configs: BackupService) -> None:
        self.configs = configs
        pass

    def backup(self):
        for config in self.configs:
            config.full_backup()