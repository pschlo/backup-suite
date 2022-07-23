# put this import first because it sets the default logger class
from modified_logging import ConsoleFormatter, FileFormatter, MultiLineLogger, thread_to_jobname

import logging
from logging import StreamHandler, FileHandler, getLogger

from backup_service import BackupService
from webdav_service import WebDavService

from typing import Type, Any, Optional
import yamale  # type: ignore
from yamale.schema import Schema  # type: ignore

from apscheduler.events import *  # type: ignore
from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
from apscheduler.schedulers.blocking import BlockingScheduler  # type: ignore
from apscheduler.schedulers.base import BaseScheduler  # type: ignore
from apscheduler.job import Job  # type: ignore
from apscheduler.triggers.base import BaseTrigger # type: ignore
from apscheduler.triggers.cron import CronTrigger # type: ignore
from apscheduler.executors.pool import ThreadPoolExecutor  # type: ignore

import warnings
from pytz_deprecation_shim._exceptions import PytzUsageWarning  # type: ignore

from datetime import datetime
import time
import sched_callbacks
import threading
from pathlib import Path



'''
TODO
- IMPORTANT: computation of local resource paths with regex does not work on Unix!!
- IMPORTANT: Fix handling of failed PROPFIND request
- IMPORTANT: Fix how raised exceptions are logged, e.g. in conn_info.py or scheduler callbacks
- check out predefined logging handlers, e.g. Rotating File Handler
- add config file option to control backup frequency
- rethink path terminology
- rethink entire project structure
- better recording of failed/incomplete backups
- improve log output
- use WebDAV lock mechanism
- disable server-side caching, e.g. by sending the resp. header
- allow different authentication mechanisms
- increase requests connection pool limits; see root logger debug output
- communication with this program from the console
- fix multi line indent for file handler
- replace conn_info prints with logs
- set appropriate max_workers value
- check what the 'daemon' parameter for threads does
'''


logger: MultiLineLogger = getLogger('suite')  # type: ignore
# LogRecords created by other modules are passed to root logger
logging.getLogger().setLevel(logging.ERROR)

# type definitions
YamlData = dict[str, Any]
YamlDoc = tuple[YamlData, str]


# singleton
class BackupSuite:

    services: tuple[BackupService]
    
    # maps backup service names to respective classes
    CFG_TO_SERVICE: dict[str, Type[BackupService]] = {
        'WebDAV Config': WebDavService
    }


    def __init__(self, *services: BackupService, config: Optional[str]) -> None:
        # initialize logger
        self.init_logger(logging.INFO, logging.INFO)

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
    def init_logger(console_level: int, file_level: int) -> None:
        logger.setLevel(logging.DEBUG)

        # create console handler
        ch = StreamHandler()
        c_fmt = '[%(asctime)s %(slevelname)5s %(jobname)s]: %(message)s'
        c_datefmt = '%H:%M:%S'
        ch.setFormatter(ConsoleFormatter(c_fmt, c_datefmt))
        ch.setLevel(console_level)
        logger.addHandler(ch)

        # create file handler
        if not Path('logs').exists():
            Path('logs').mkdir()
        fh = FileHandler('./logs/general.log', encoding='utf-8')
        f_fmt = '[%(asctime)s %(slevelname)5s %(jobname)s]: %(message)s'
        f_datefmt = '%Y-%m-%d %H:%M:%S'
        fh.setFormatter(FileFormatter(f_fmt, f_datefmt))
        fh.setLevel(file_level)
        logger.addHandler(fh)


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
            logger.error('Error loading config file:', lines=str(e))
            exit(1)

        logger.info('Loaded config file')
        
        # extract config data
        first_doc: YamlDoc = config[0]
        first_doc_data: YamlData = first_doc[0]
        return first_doc_data


    # perform a backup
    def backup(self, jobname: str):
        # small delay for scheduler callback to report 'Executing job'
        time.sleep(0.2)

        # create new file handler
        timestamp: str = datetime.now().astimezone().strftime('%Y-%m-%d_%H-%M-%S_UTC%z')
        fh = FileHandler(f'./logs/{timestamp}.log', encoding='utf-8')
        f_fmt = '[%(asctime)s %(slevelname)5s %(jobname)s]: %(message)s'
        f_datefmt = '%H:%M:%S'
        fh.setFormatter(FileFormatter(f_fmt, f_datefmt))
        fh.setLevel(logging.INFO)
        logger.addHandler(fh)

        # update thread to job mapping
        thread_id = threading.get_ident()
        thread_to_jobname[thread_id] = jobname

        # run backups
        for service in self.services:
            service.backup()

        # done
        # reset thread to job mapping
        del thread_to_jobname[thread_id]
        # remove file handler
        fh.close()
        logger.removeHandler(fh)


    # keep running and perform a backup as specified in schedule, e.g. every 2 hours
    def scheduled_backup(self):
        logger.info('Initializing scheduled backup')

        # create scheduler
        scheduler: BaseScheduler = BlockingScheduler(executors={'default': ThreadPoolExecutor(pool_kwargs={'thread_name_prefix': 'JobThread'})})

        # create job
        trigger: BaseTrigger = ModCronTrigger(year='*', month='*', day='*', week='*', day_of_week='*', hour='*', minute='*', second='*/15')
        jobname = 'BackupJob'
        scheduler.add_job(self.backup, trigger, name=jobname, coalesce=True, kwargs={'jobname': jobname})  # type: ignore
        # trigger: BaseTrigger = ModCronTrigger(year='*', month='*', day='*', week='*', day_of_week='*', hour='*', minute='*', second='*/17')
        # scheduler.add_job(self.backup, trigger, name='BackupJob2', coalesce=True)  # type: ignore

        # add event callbacks
        scheduler.add_listener(lambda x: sched_callbacks.job_executed(scheduler, x), EVENT_JOB_EXECUTED)  # type: ignore
        scheduler.add_listener(lambda x: sched_callbacks.job_error(scheduler, x), EVENT_JOB_ERROR)  # type: ignore
        scheduler.add_listener(lambda x: sched_callbacks.job_missed(scheduler, x), EVENT_JOB_MISSED)  # type: ignore
        scheduler.add_listener(lambda x: sched_callbacks.job_max_instances(scheduler, x), EVENT_JOB_MAX_INSTANCES)  # type: ignore
        scheduler.add_listener(lambda x: sched_callbacks.job_submitted(scheduler, x), EVENT_JOB_SUBMITTED)  # type: ignore
        scheduler.add_listener(lambda x: sched_callbacks.sched_started(scheduler, x), EVENT_SCHEDULER_STARTED)  # type: ignore

        # start scheduler
        scheduler.start()  # type: ignore

        



# CronTrigger.get_next_fire_time leads to a PytzUsageWarning, which can be ignored
# using this with-statement disables these warnings
class ModCronTrigger(CronTrigger):
    def get_next_fire_time(self, previous_fire_time: datetime | None, now: datetime) -> datetime | None:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=PytzUsageWarning)
            next_fire_time: datetime | None = super().get_next_fire_time(previous_fire_time, now)  # type: ignore
        return next_fire_time


