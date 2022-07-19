from typing import Optional
import requests as req
import shutil
from pathlib import PurePath, Path
from concurrent.futures import ThreadPoolExecutor, Future, as_completed

from modified_logging import MultiLineLogger, getLogger
from clean_path import clean_path
from conn_info import ConnInfo


'''
TERMINOLOGY

https://en.wikipedia.org/wiki/URL

URL structure:  <scheme>://<netloc>/<remote_root_path>/<resource_path>

-- ROOT PATH --
[remote]    remote root path:   path to the root directory of the WebDAV server, i.e. the part between netloc and resource path
[local]     local root path:    absolute path to the local destination directory of the backup, e.g. 'D:/backups/webdav-backup-13'
[remote]    root url:           URL without a resource path: <scheme>://<netloc>/<remote_root_path>

-- RESOURCE PATH --
[both]      resource path:      path to a resource, excluding the root path prefix, i.e. the part after root path
[remote]    resource url:       URL to a resource: <scheme>://<netloc>/<remote_root_path>/<resource_path>

-- FULL PATH --
[local]     full local path:    absolute path on local file system, e.g. 'D:/data/test/file1.txt' or '/mnt/drive1/file1.txt'
[remote]    full remote path:   absolute path on network location; looks like POSIX-style full local path. <scheme>://<netloc>/<full_remote_path> is a valid URL

'''


'''
ERRORS

RequestException: any error with the request; base exception
    HTTPError: raised if request returned unsuccessful status code
        ResponseNotOkError: raised if status code is not "200 OK" for GET or "207 Multi-Status" for PROPFIND
            ServiceUnavailableError: raised if status code is "503 Service Unavailable"
'''



logger: MultiLineLogger = getLogger('suite.service')


class BackupService:

    conn_info: ConnInfo
    local_root_path: PurePath
    do_async: bool

    def __init__(
        self,
        root_url: str,
        local_root_path: str,
        do_async: bool
        ) -> None:

        self.conn_info = ConnInfo(root_url)
        self.local_root_path = PurePath(local_root_path)
        self.do_async = do_async

        logger.debug("Initialized %s", self.__class__.__name__, self.conn_info.hostname, self.local_root_path,
            lines=['[remote] %s', '[local] %s'])


    # returns list of resources that should be downloaded
    def get_resources(self) -> list[PurePath]:
        raise NotImplementedError


    def delete_local_root(self) -> None:
        if Path(self.local_root_path).exists():
            shutil.rmtree(self.local_root_path)
            logger.info('deleted %s', self.local_root_path)


    def create_directory_tree(self, resource_paths: list[PurePath]) -> None:
        created_paths: set[PurePath] = set()

        resource_path: PurePath
        for resource_path in resource_paths:
            # remove illegal chars
            resource_path = clean_path(resource_path)

            # remove filename from path
            resource_dir_path: PurePath = resource_path.parent
            local_dir_path: Path = Path(self.local_root_path / resource_dir_path)

            # create dirs
            if local_dir_path not in created_paths:
                logger.info('creating dir %s', local_dir_path)
                local_dir_path.mkdir(parents=True, exist_ok=True)
                created_paths.add(local_dir_path)


    def full_backup(self) -> None:
        logger.info('starting full backup')
        resource_paths: list[PurePath] = self.get_resources()
        
        # delete dest dir
        self.delete_local_root()

        # create directory tree
        self.create_directory_tree(resource_paths)

        # download resources
        self.download_resources(resource_paths)


    @staticmethod
    def shutdown_executor(executor: ThreadPoolExecutor) -> None:
        # program will not terminate until all running threads are terminated anyway
        # therefore wait=True is okay
        logger.debug('shutting down executor...')
        executor.shutdown(wait=True, cancel_futures=True)
        logger.debug('executor is shut down')


    '''
    IMPORTANT

    if trying to exit main thread while other threads running:
        - main thread will be idle, but executor will keep deploying threads and threads will keep running
        - therefore: shutdown executor with cancel_futures=True before exiting

    - when using 'with' statement, executor shutdown is called with cancel_futures=False upon program exit
    - therefore, avoid 'with' statement and manually call shutdown with cancel_futures=True instead
    '''

    def download_resources(self, resources: list[PurePath]) -> None:
        # types
        ResourceFuture = Future[None]

        # store mapping from futures to resources
        future_to_resource: dict[ResourceFuture, PurePath] = dict()

        # choose maximum number of threads
        # setting to None will let ThreadPoolExecutor choose automatically
        max_workers: Optional[int] = None if self.do_async else 1

        # create executor
        executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers)

        # at beginning of each iteration, 'resources' contains list of resources that still need to be downloaded
        # exit loop if no resources left
        # count iterations in 'try_num'
        try_num: int = 0
        while len(resources) > 0:
            try_num += 1

            # submit everything in resource list
            resource: PurePath
            for resource in resources:
                future = executor.submit(self.download_resource, resource)
                future_to_resource[future] = resource

            # reset resource list
            resources.clear()

            # check results and re-add to resource list if failed
            future: ResourceFuture
            for future in as_completed(future_to_resource):
                resource: PurePath = future_to_resource[future]
                status_code: str
                reason: str

                try:
                    future.result()
                except req.exceptions.HTTPError as e:
                    r: req.Response = e.response
                    status_code = str(r.status_code)
                    reason = str(r.reason)
                    resources.append(resource)
                except req.exceptions.RequestException as e:
                    status_code = '---'
                    reason = 'RequestError'
                    resources.append(resource)
                else:
                    # status code is 200 OK and no exceptions raised
                    status_code = '200'
                    reason = 'OK'

                logger.info('[%2s]  %3s  %-20s  %-100s', try_num, status_code, reason, resource)

            # every thread is now done
            # reset future to resource mapping
            future_to_resource.clear()

        # every resource has been downloaded without error
        # shut down executor
        self.shutdown_executor(executor)


    def download_resource(self, resource_path: PurePath) -> None:
        raise NotImplementedError
