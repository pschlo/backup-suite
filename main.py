from backup_suite import BackupSuite


def main():
    suite = BackupSuite(config='config.yml')
    suite.backup()


if __name__ == '__main__':
    main()