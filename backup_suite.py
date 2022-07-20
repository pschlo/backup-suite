# put this import first because it sets the default logger class
from modified_logging import ConsoleFormatter, FileFormatter, MultiLineLogger

from backup_service import BackupService
from webdav_service import WebDavService
from time_conversion import TimeUnit, convert_duration, format_duration

from typing import Type
import yamale  # type: ignore
from yamale.schema import Schema  # type: ignore
from typing import Any, Optional
import logging
from logging import StreamHandler, FileHandler, getLogger
from apscheduler import events  # type: ignore
from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
from apscheduler.schedulers.blocking import BlockingScheduler  # type: ignore
from apscheduler.events import JobEvent  # type: ignore
from apscheduler.schedulers.base import BaseScheduler  # type: ignore
from apscheduler.job import Job  # type: ignore
import time
from datetime import datetime, timezone, timedelta



'''
TODO
- use WebDAV lock mechanism
- disable server-side caching, e.g. by sending the resp. header
- allow different authentication mechanisms
- increase requests connection pool limits; see root logger debug output
- communication with this program from the console
- disable logging output from apscheduler
'''

# logging.basicConfig(level=logging.DEBUG)

logger: MultiLineLogger = getLogger('suite')  # type: ignore

# type definitions
YamlData = dict[str, Any]
YamlDoc = tuple[YamlData, str]


# singleton
class BackupSuite:

    services: tuple[BackupService]
    scheduler: BaseScheduler

    CFG_TO_SERVICE: dict[str, Type[BackupService]] = {
        'WebDAV Config': WebDavService
    }


    def __init__(self, *services: BackupService, config: Optional[str]) -> None:
        # initialize logger
        self.init_logger(logging.INFO, logging.INFO)

        # create scheduler
        self.scheduler = BackgroundScheduler()

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
        c_fmt = '[%(asctime)s %(slevelname)5s]: %(message)s'
        c_datefmt = '%H:%M:%S'
        ch.setFormatter(ConsoleFormatter(c_fmt, c_datefmt))
        ch.setLevel(console_level)
        logger.addHandler(ch)

        # create file handler
        fh = FileHandler('log.txt', encoding='utf-8')
        f_fmt = '[%(asctime)s %(slevelname)5s]: %(message)s'
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
            logger.error('Invalid config file!', lines=str(e))
            exit(1)

        logger.info('Successfully loaded config file')
        
        # extract config data
        first_doc: YamlDoc = config[0]
        first_doc_data: YamlData = first_doc[0]
        return first_doc_data



    # perform a backup once
    def single_backup(self):
        logger.info('Starting single backup')
        for service in self.services:
            service.full_backup()


    # keep running and perform a backup as specified in schedule, i.e. every 2 hours
    def interval_backup(self):
        logger.info('Starting interval backup')

        # execute daily at 03:00 AM
        job: Job = self.scheduler.add_job(self.single_backup, 'cron', year='*', month='*', day='*', week='*', day_of_week='*', hour='*', minute='*', second='*/5')  # type: ignore
        self.scheduler.add_listener(lambda x: BackupSuite.callback_job_executed(self.scheduler, x), events.EVENT_JOB_EXECUTED)  # type: ignore
        self.scheduler.add_listener(lambda x: BackupSuite.callback_job_error(self.scheduler, x), events.EVENT_JOB_ERROR)  # type: ignore
        self.scheduler.add_listener(lambda x: BackupSuite.callback_job_missed(self.scheduler, x), events.EVENT_JOB_MISSED)  # type: ignore
        self.scheduler.add_listener(lambda x: BackupSuite.callback_job_max_instances(self.scheduler, x), events.EVENT_JOB_MAX_INSTANCES)  # type: ignore
        self.scheduler.start()  # type: ignore
        logger.info('Scheduler started')

        log_next_run_time(job)

        while True:
            # sleep so that python does not use 100% CPU the entire time
            time.sleep(1)


    # job was executed successfully
    @staticmethod
    def callback_job_executed(scheduler: BaseScheduler, event: JobEvent):
        job: Job = scheduler.get_job(event.job_id)  # type: ignore
        log_next_run_time(job)
    

    # job raised an exception during execution
    @staticmethod
    def callback_job_error(scheduler: BaseScheduler, event: JobEvent):
        job: Job = scheduler.get_job(event.job_id)  # type: ignore
        logger.error('An error occurred while executing the backup job')


    # job’s execution was missed
    @staticmethod
    def callback_job_missed(scheduler: BaseScheduler, event: JobEvent):
        job: Job = scheduler.get_job(event.job_id)  # type: ignore
        logger.warning('Execution of backup job was missed')


    # job being submitted to its executor was not accepted by the executor because the job has already reached its maximum concurrently executing instances
    @staticmethod
    def callback_job_max_instances(scheduler: BaseScheduler, event: JobEvent):
        job: Job = scheduler.get_job(event.job_id)  # type: ignore
        logger.warning("Executor did not execute backup job because maximum concurrently running instance limit is reached")




### FUNCTIONS


def log_next_run_time(job: Job):
    # create string representing time left until next run
    # datetime.now() returns a naive datetime equal to the current system time
    # to get aware current system time: first get as UTC, then cast to local time zone
    curr_time = datetime.now(timezone.utc).astimezone()
    next_run_time: datetime = job.next_run_time  # type: ignore
    seconds_till_next: int = int((next_run_time - curr_time).total_seconds())
    if seconds_till_next > 0:
        till_next_str = 'in ' + format_duration(seconds_till_next)
    else:
        till_next_str = 'now'

    # retrieve UTC offset
    utc_offset: Optional[timedelta] = next_run_time.utcoffset()
    if utc_offset is None:
        raise ValueError('Invalid UTC offset')

    # create UTC offset string
    sign = '+' if utc_offset >= timedelta(0) else '-'
    _, minutes, hours = convert_duration(abs(utc_offset), TimeUnit.HOURS)
    utc_offset_str = '%s%02d:%02d' % (sign, hours, minutes)

    logger.info('Next run is at %s (%s)', next_run_time.strftime(f'%Y-%m-%d %H:%M:%S UTC{utc_offset_str}'), till_next_str)
    
