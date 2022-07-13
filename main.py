from backup_suite import BackupSuite
from webdav_service import WebDavService
import yaml
from cerberus.validator import Validator  # type: ignore
import pprint


# TODO: put loading of config file in separate method
# TODO: in BackupService class: split one big for-loop into two (i.e. restrict ThreadPoolExecutor to only a few lines)

def main():

    # read config schema and create validator
    with open('config_schema.py', 'r') as f:
        config_schema = eval(f.read())
    v = Validator(config_schema)  # type: ignore

    # read config
    with open('config.yml', 'r') as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise e

    # check if config file fits schema
    if not v.validate(config):  # type: ignore
        printer = pprint.PrettyPrinter()
        printer.pprint(v.errors)  # type: ignore
        raise ValueError('Invalid config file')

    print('Successfully loaded config file')


    webdav_cfg = config['WebDAV Config']

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