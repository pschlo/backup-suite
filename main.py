from backup_suite import BackupSuite
from webdav_service import WebDavService
from configparser import ConfigParser, SectionProxy


def main():

    # parse config file
    config: ConfigParser = ConfigParser(interpolation=None)
    config.read('config.ini')
    webdav_cfg: SectionProxy = config['WebDAV Config']
    


    config1 = WebDavService(
        root_url = r"https://cloud.rotex1880-cloud.org/remote.php/dav/files/backup",
        username = webdav_cfg['username'],
        password = webdav_cfg['password'],
        local_root_path = r'D:/nextcloud-backup-test/',
        do_async = True
        )
    
    suite = BackupSuite(config1)
    suite.backup()


if __name__ == '__main__':
    main()