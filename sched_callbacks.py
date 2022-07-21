from apscheduler.events import *  # type: ignore
from apscheduler.schedulers.base import BaseScheduler  # type: ignore
from apscheduler.job import Job  # type: ignore

from logging import getLogger
from modified_logging import MultiLineLogger

from time_conversion import format_timedelta, format_datetime, get_delta_to_now
from datetime import datetime, timedelta


logger: MultiLineLogger = getLogger('suite.sched_callbacks')  # type: ignore

# how many digits of the hexadecimal job ID should be logged
LEN_JOB_ID: int = 8


## CALLBACKS
# TODO: what happens if error occurs in callback?


# job was executed successfully
def job_executed(scheduler: BaseScheduler, event: JobExecutionEvent):
    job: Job = scheduler.get_job(event.job_id)  # type: ignore
    logger.info(f'Finished executing {get_job_name(job)}')
    log_next_runs(scheduler.get_jobs())  # type: ignore


# job raised an exception during execution
def job_error(scheduler: BaseScheduler, event: JobExecutionEvent):
    job: Job = scheduler.get_job(event.job_id)  # type: ignore
    logger.error(f'An error occurred while executing {get_job_name(job)}')


# job’s execution was missed
def job_missed(scheduler: BaseScheduler, event: JobExecutionEvent):
    job: Job = scheduler.get_job(event.job_id)  # type: ignore
    delta: timedelta = abs(get_delta_to_now(event.scheduled_run_time))  # type: ignore
    logger.warning(f'Execution of {get_job_name(job)} was missed by {format_timedelta(delta)}')  # type: ignore


# job being submitted to its executor was not accepted by the executor because the job has already reached its maximum concurrently executing instances
def job_max_instances(scheduler: BaseScheduler, event: JobSubmissionEvent):
    job: Job = scheduler.get_job(event.job_id)  # type: ignore
    logger.warning(f"Skipped execution of {get_job_name(job)}: Maximum number of running instances reached ({job.max_instances})")  # type: ignore


# job was started, i.e. submitted to executor
def job_submitted(scheduler: BaseScheduler, event: JobSubmissionEvent):
    job: Job = scheduler.get_job(event.job_id)  # type: ignore
    logger.info(f'Executing {get_job_name(job)}')  # type: ignore


def sched_started(scheduler: BaseScheduler, event: SchedulerEvent):
    logger.info('Scheduler started')
    log_next_runs(scheduler.get_jobs())  # type: ignore


## OTHER FUNCTIONS


def get_job_name(job: Job) -> str:
    return f'{job.name} {job.id[:LEN_JOB_ID]}'  # type: ignore


# generate log message that shows the next run times of jobs
def log_next_runs(jobs: list[Job]):
    lines: list[str] = []
    for job in jobs:
        line: str = f'[{get_job_name(job)}]: '
        line += format_next_run_time(job.next_run_time)  # type: ignore
        lines.append(line)
    logger.info('Next runs:', lines=lines)


# formats the next_run_time of a job to 'YYYY-MM-DD HH:MM:SS (in <time from now>)'
def format_next_run_time(next_run_time: datetime) -> str:
    fmt_datetime = format_datetime(next_run_time)
    fmt_timedelta = format_timedelta(get_delta_to_now(next_run_time), prefix='in', now='now')
    return  f'{fmt_datetime} ({fmt_timedelta})'
