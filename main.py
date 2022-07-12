from backup_suite import BackupSuite
from webdav_config import WebDavConfig


def main():
    config1 = WebDavConfig(
        root_url=r"https://cloud.rotex1880-cloud.org:443/remote.php/dav/files/backup/?test=3#frag",
        login='backup',
        password=r";B-F$\4EeeQtVMjrZ.]r",
        local_root_path=r'D:/nextcloud-backup-test/',
        do_async=True
        )
    
    suite = BackupSuite(config1)
    suite.backup()


if __name__ == '__main__':
    main()