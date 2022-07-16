from backup_service import BackupService
from webdav_service import WebDavService
from typing import Type
import yamale  # type: ignore
from yamale.schema import Schema  # type: ignore
from typing import Any, Optional
import logging


# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create handlers
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler('log.txt')
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# create formatters
console_fmt = '[%(asctime)s] %(levelname)s (%(name)s): %(message)s'
console_datefmt = '%H:%M:%S'
console_formatter = logging.Formatter(console_fmt, console_datefmt)
console_handler.setFormatter(console_formatter)

file_fmt = '[%(asctime)s] %(levelname)s (%(name)s): %(message)s'
file_datefmt = '%Y-%m-%d %H:%M:%S'
file_formatter = logging.Formatter(file_fmt, file_datefmt)
file_handler.setFormatter(file_formatter)

#logger.error('Example message')


# type definitions
YamlData = dict[str, Any]
YamlDoc = tuple[YamlData, str]


class BackupSuite:

    services: tuple[BackupService]
    CFG_TO_SERVICE: dict[str, Type[BackupService]] = {
        'WebDAV Config': WebDavService
    }


    def __init__(self, *services: BackupService, config: Optional[str]) -> None:
        if config is not None:
            # path to config file given
            # create service objects from file
            config_yaml: YamlData = BackupSuite.load_config(config)
            services_tmp: list[BackupService] = []

            # loop through every config section
            section: str
            for section in config_yaml:
                service_class = BackupSuite.CFG_TO_SERVICE[section]
                # create service object
                service: BackupService = service_class(**config_yaml[section])
                services_tmp.append(service)

            # store service objects as tuple
            self.services = tuple(services_tmp)
        else:
            # list of BackupService objects given
            self.services = services


    @staticmethod
    def load_config(config_path: str) -> YamlData:
        SCHEMA_PATH: str = 'config_schema.yml'

        # load validation schema
        schema: Schema = yamale.make_schema(SCHEMA_PATH)  # type: ignore

        # load config data:
        #   - data can contain multiple YAML documents in one file, separated by ---
        #   - a tuple (data, path) corresponds to a single YAML document
        #   - make_data returns a list of such tuples
        config: list[YamlDoc] = yamale.make_data(config_path)  # type: ignore

        # validate
        try:
            yamale.validate(schema, config)  # type: ignore
        except yamale.YamaleError as e:
            print('Invalid config file!\n%s' % str(e))
            exit(1)

        print('Successfully loaded config file')
        
        # extract config data
        first_doc: YamlDoc = config[0]
        first_doc_data: YamlData = first_doc[0]
        return first_doc_data


    def backup(self):
        for service in self.services:
            service.full_backup()
