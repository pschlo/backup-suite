from typing import Callable, Optional
import requests as req
import shutil
from pathlib import PurePath, Path
from concurrent.futures import ThreadPoolExecutor, Future, as_completed

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
[remote]    full remote path:   absolute path on network location; looks like POSIX-style full local path. scheme>://<netloc>/<full_remote_path> is a valid URL

'''


'''
ERRORS

RequestException: any error with the request; base exception
    HTTPError: raised if request returned unsuccessful status code
        ResponseNotOkError: raised if status code is not "200 OK" for GET or "207 Multi-Status" for PROPFIND
            ServiceUnavailableError: raised if status code is "503 Service Unavailable"
'''


class BackupService:

    conn_info: ConnInfo
    get_sources: Callable[[], list[PurePath]]
    local_root_path: PurePath
    do_async: bool

    def __init__(
        self,
        root_url: str,
        get_sources: Callable[[], list[PurePath]],
        local_root_path: str,
        do_async: bool
        ) -> None:

        self.conn_info = ConnInfo(root_url)
        self.get_sources = get_sources
        self.local_root_path = PurePath(local_root_path)
        self.do_async = do_async


    def delete_local_root(self) -> None:
        if Path(self.local_root_path).exists():
            shutil.rmtree(self.local_root_path)
            print('deleted', self.local_root_path)


    def create_directory_tree(self, resource_paths: list[PurePath]) -> None:
        created_paths: set[PurePath] = set()

        resource_path: PurePath
        for resource_path in resource_paths:
            # remove filename from path
            resource_dir_path: PurePath = resource_path.parent
            local_dir_path: Path = Path(self.local_root_path / resource_dir_path)

            # create dirs
            if local_dir_path not in created_paths:
                print(f'creating dir {local_dir_path}')
                local_dir_path.mkdir(parents=True, exist_ok=True)
                created_paths.add(local_dir_path)


    def full_backup(self) -> None:
        resource_paths: list[PurePath] = self.get_sources()
        
        # delete dest dir
        self.delete_local_root()

        # create directory tree
        self.create_directory_tree(resource_paths)


        # download resources

        ResourceFuture = Future[None]
        future_to_path: dict[ResourceFuture, PurePath] = dict()
        # ThreadPoolExecutor will choose max_workers automatically when set to None
        max_workers: Optional[int] = None if self.do_async else 1
        

        '''
        IMPORTANT

        if trying to exit main thread while other threads running:
            - main thread will be idle, but executor will keep deploying threads and threads will keep running
            - therefore: shutdown executor with cancel_futures=True before exiting

        - when using 'with' statement, executor shutdown is called with cancel_futures=False upon program exit
        - therefore, avoid 'with' statement and manually call shutdown with cancel_futures=True instead
        '''

        executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers)

        # launch download threads
        resource_path: PurePath
        for resource_path in resource_paths:
            future: ResourceFuture = executor.submit(self.download_resource, resource_path)
            future_to_path[future] = resource_path

        # wait for threads to finish
        future: ResourceFuture
        for future in as_completed(future_to_path):
            try:
                future.result()
            # catch HTTPerror and RequestException
            except req.exceptions.HTTPError as e:
                BackupService.shutdown_executor(executor)
                raise e
            except req.exceptions.RequestException as e:
                BackupService.shutdown_executor(executor)
                raise e
            else:
                # status code is 200 OK and no exceptions raised
                print(f'200 OK: {future_to_path[future].as_posix()}')
            
        # every thread is now done
        # shut down executor
        BackupService.shutdown_executor(executor)
        

    @staticmethod
    def shutdown_executor(executor: ThreadPoolExecutor):
        # program will not terminate until all running threads are terminated anyway
        # therefore wait=True is okay
        executor.shutdown(wait=True, cancel_futures=True)

    def download_resource(self, resource_path: PurePath) -> None:
        raise NotImplementedError
