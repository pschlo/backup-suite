from backup_suite import BackupSuite
from webdav_service import WebDavService
from typing import Any
from configobj import ConfigObj, SimpleVal, ConfigObjError  # type: ignore



def main():

    # parse config file
    config: ConfigObj = ConfigObj('config.ini', list_values=False, interpolation=False, configspec='configspec.ini')

    # validator will check if config.ini matches configspec.ini
    # check_res is True or False or bool dict
    validator = SimpleVal()
    check_res: bool | dict[Any, Any] = config.validate(validator)  # type: ignore

    if check_res is not True:
        raise ConfigObjError('Invalid config file')
    else:
        print('Successfully loaded config file')


    webdav_cfg = config['WebDAV Config']  # type: ignore

    config1 = WebDavService(
        root_url = webdav_cfg['root url'],  # type: ignore
        username = webdav_cfg['username'],  # type: ignore
        password = webdav_cfg['password'],  # type: ignore
        local_root_path = webdav_cfg['local root path'],  # type: ignore
        do_async = True
        )
    
    suite = BackupSuite(config1)
    suite.backup()


if __name__ == '__main__':
    main()