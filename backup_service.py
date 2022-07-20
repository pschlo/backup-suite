from typing import Optional, Any
import requests as req
import shutil
from pathlib import PurePath, Path
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
import re
import logging

from modified_logging import MultiLineLogger
from logging import getLogger
from conn_info import ConnInfo


'''
TERMINOLOGY

https://en.wikipedia.org/wiki/URL

URL structure:  <scheme>://<netloc>/<remote_root_path>/<resource_path>

-- ROOT PATH --
[remote]    remote root path:       path to the root directory of the network location, i.e. the part between netloc and resource path
[local]     local root path:        absolute path to the local destination directory of the backup, e.g. 'D:/backups/webdav-backup-13'
[remote]    root url:               URL without a resource path: <scheme>://<netloc>/<remote_root_path>

-- RESOURCE PATH --
[remote]    remote resource path    path to a resource on the network location, excluding the root path prefix, i.e. the part after root path
[local]     local resource path     path to a resource on local file system, excluding the root path prefix. In most cases, this is equvalent to its remote counterpart.
                                    However, the remote path sometimes contains illegal chars, which are removed in the local path
[remote]    resource url:           URL to a resource: <scheme>://<netloc>/<remote_root_path>/<resource_path>

-- FULL PATH --
[remote]    remote full path:       absolute path on network location; looks like POSIX-style full local path. <scheme>://<netloc>/<full_remote_path> is a valid URL
[local]     local full path:        absolute path on local file system, e.g. 'D:/data/test/file1.txt' or '/mnt/drive1/file1.txt'

'''


'''
ERRORS

RequestException: any error with the request; base exception
    HTTPError: raised if request returned unsuccessful status code
        ResponseNotOkError: raised if status code is not "200 OK" for GET or "207 Multi-Status" for PROPFIND
            ServiceUnavailableError: raised if status code is "503 Service Unavailable"
'''


logger: MultiLineLogger = getLogger('suite.service')  # type: ignore


class BackupService:

    # how many times we should try to download a resource before giving up
    # e.g. setting to 1 will not retry any failed downloads
    MAX_TRIES: Optional[int] = None

    # HTTP status codes for which we should re-try the request
    RETRY_CODES: tuple[int,...] = (
        req.codes.too_many_requests,
        req.codes.bad_gateway,
        req.codes.service_unavailable,
        req.codes.gateway_timeout
    )

    # maps remote resource paths to local resource paths
    remote_res_to_local_res: dict[PurePath, PurePath] = dict()

    conn_info: ConnInfo
    local_root_path: PurePath
    do_async: bool
    delay_in_hours: int
    starting_from: str


    def __init__(
        self,
        root_url: str,
        local_root_path: str,
        do_async: bool,
        interval: dict[str, Any]
        ) -> None:

        self.conn_info = ConnInfo(root_url)
        self.local_root_path = PurePath(local_root_path)
        self.do_async = do_async
        self.delay_in_hours = interval['delay_in_hours']
        self.starting_from = interval['starting_from']

        logger.info("Initialized %s", self.__class__.__name__, self.conn_info.hostname, self.local_root_path,
            lines=['[remote] %s', '[local] %s'])


    def _delete_local_root(self) -> None:
        if Path(self.local_root_path).exists():
            shutil.rmtree(self.local_root_path)
            logger.debug('Deleted %s', self.local_root_path)


    def _compute_local_res_paths(self, remote_res_paths: list[PurePath]):
        INVALID_WIN_CHARS: str = r'[\<\>\:\"\/\|\?\*]+'
        for remote_res_path in remote_res_paths:
            local_res_path: PurePath = PurePath(re.sub(INVALID_WIN_CHARS, '', str(remote_res_path)))
            self.remote_res_to_local_res[remote_res_path] = local_res_path


    def _create_directory_tree(self, remote_res_paths: list[PurePath]) -> None:
        created_paths: set[PurePath] = set()

        remote_res_path: PurePath
        for remote_res_path in remote_res_paths:
            local_res_path: PurePath = self.remote_res_to_local_res[remote_res_path]

            # remove filename from path to obtain a path only consisting of the direcories to the resource
            local_dir_path: PurePath = local_res_path.parent
            local_full_path: Path = Path(self.local_root_path / local_dir_path)

            # create dirs
            if local_full_path not in created_paths:
                logger.debug('Creating dir %s', local_full_path)
                local_full_path.mkdir(parents=True, exist_ok=True)
                created_paths.add(local_full_path)


    def full_backup(self) -> None:
        # fetch resource list
        logger.info('Fetching resource list')
        remote_res_paths: list[PurePath] = self.get_resources()
        logger.debug('Resource list fetched')

        # compute local resource paths
        self._compute_local_res_paths(remote_res_paths)
        
        # delete dest dir
        logger.info('Deleting local root')
        self._delete_local_root()
        logger.debug('Local root deleted')

        # create directory tree
        logger.info('Creating directory tree')
        self._create_directory_tree(remote_res_paths)
        logger.debug('Direcory tree created')

        # download resources
        logger.info('Downloading resources')
        logger.info('%4s  %4s %-35s  %s' % ('TRY', 'CODE', 'REASON', 'PATH'))
        self.download_resources(remote_res_paths)
        logger.debug('Resources downloaded')

        logger.info('Backup finished')


    @staticmethod
    def shutdown_executor(executor: ThreadPoolExecutor) -> None:
        # program will not terminate until all running threads are terminated anyway
        # therefore wait=True is okay
        logger.info('Shutting down executor')
        executor.shutdown(wait=True, cancel_futures=True)
        logger.debug('Executor shut down')


    '''
    IMPORTANT

    if trying to exit main thread while other threads running:
        - main thread will be idle, but executor will keep deploying threads and threads will keep running
        - therefore: shutdown executor with cancel_futures=True before exiting

    - when using 'with' statement, executor shutdown is called with cancel_futures=False upon program exit
    - therefore, avoid 'with' statement and manually call shutdown with cancel_futures=True instead
    '''

    def download_resources(self, remote_resources: list[PurePath]) -> None:
        # types
        ResourceFuture = Future[None]

        # store mapping from futures to resources
        future_to_resource: dict[ResourceFuture, PurePath] = dict()

        # store resources that we should try downloading
        # initially, we want to try downloading every resource
        try_resources: list[PurePath] = remote_resources.copy()

        # store failed resources that cannot be downloaded, not even after retrying
        failed_resources: list[PurePath] = []

        # choose maximum number of threads
        # setting to None will let ThreadPoolExecutor choose automatically
        max_workers: Optional[int] = 20 if self.do_async else 1

        # create executor
        executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers)

        # at beginning of each iteration, 'try_resources' contains list of resources that still need to be downloaded
        # count iterations in 'try_num'
        try_num: int = 0
        while len(try_resources) > 0 and (self.MAX_TRIES is None or try_num < self.MAX_TRIES):
            try_num += 1

            # submit every resource in try list
            resource: PurePath
            for resource in try_resources:
                future = executor.submit(self.download_resource, resource)
                future_to_resource[future] = resource

            # check results
            retry_resources, new_failed_resources = self.check_futures(try_num, future_to_resource)
            try_resources = retry_resources
            failed_resources.extend(new_failed_resources)

            # every thread is now done
            # reset future to resource mapping
            future_to_resource.clear()

        # every resource has been downloaded without error or number of tries exceeded limit
        # resources left to try are now failed resources too
        failed_resources.extend(try_resources)
        # shut down executor
        self.shutdown_executor(executor)

        if len(failed_resources) == 0:
            logger.info('No failed resources')
        else:
            logger.error('Failed resources:', lines=failed_resources)


    # check resource download results
    # return resources to retry and failed resources
    def check_futures(self, try_num: int, future_to_resource: dict[Future[None], PurePath]) -> tuple[list[PurePath], list[PurePath]]:
        retry_resources: list[PurePath] = []
        failed_resources: list[PurePath] = []

        future: Future[Any]
        for future in as_completed(future_to_resource):
            resource: PurePath = future_to_resource[future]
            status_code: int | str
            reason: str
            log_level: int

            try:
                future.result()
            except req.HTTPError as e:
                r: req.Response = e.response
                status_code = r.status_code
                reason = r.reason
                if status_code in self.RETRY_CODES:
                    # retry
                    log_level = logging.WARNING
                    retry_resources.append(resource)
                else:
                    # no retry
                    log_level = logging.ERROR
                    failed_resources.append(resource)
            except req.RequestException as e:
                status_code = '---'
                reason = type(e).__name__
                if isinstance(e, req.Timeout):
                    # retry
                    log_level = logging.WARNING
                    retry_resources.append(resource)
                else:
                    log_level = logging.ERROR
                    failed_resources.append(resource)
            else:
                log_level = logging.INFO
                # status code is 200 OK and no exceptions raised
                status_code = '200'
                reason = 'OK'

            logger.log(log_level, '(%2s)  %3s  %-35s  %-100s', try_num, status_code, reason, resource)
        
        return retry_resources, failed_resources

## abstract methods

    # returns list of resources that should be downloaded
    def get_resources(self) -> list[PurePath]:
        raise NotImplementedError


    def download_resource(self, remote_res_path: PurePath) -> None:
        raise NotImplementedError
