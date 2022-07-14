from backup_suite import BackupSuite
from webdav_service import WebDavService
import yamale  # type: ignore
from yamale.schema import Schema  # type: ignore
from typing import Any


# type definitions
YamlData = dict[str, Any]
YamlDoc = tuple[YamlData, str]

# TODO: in BackupService class: split one big for-loop into two (i.e. restrict ThreadPoolExecutor to only a few lines)


def load_config() -> YamlData:
    # load validation schema
    config_schema: Schema = yamale.make_schema('config_schema.yml')  # type: ignore

    # load config data:
    #   - data can contain multiple YAML documents in one file, separated by ---
    #   - a tuple (data, path) corresponds to a single YAML document
    #   - make_data returns a list of such tuples
    config: list[YamlDoc] = yamale.make_data('config.yml')  # type: ignore

    # validate
    try:
        yamale.validate(config_schema, config)  # type: ignore
    except yamale.YamaleError as e:
        print('Invalid config file!\n%s' % str(e))
        exit(1)

    print('Successfully loaded config file')
    
    # extract config data
    first_doc: YamlDoc = config[0]
    first_doc_data: YamlData = first_doc[0]
    return first_doc_data


def main():
    config = load_config()
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