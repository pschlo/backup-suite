from typing import Callable, Optional
import requests as req
import shutil
from pathlib import PurePath, Path
from concurrent.futures import ThreadPoolExecutor, Future, as_completed

from conn_info import ConnInfo
from exceptions import ResponseNotOkError, ServiceUnavailableError


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

    def full_backup(self) -> None:
        resource_paths: list[PurePath] = self.get_sources()
        
        ## delete dest dir

        if Path(self.local_root_path).exists():
            shutil.rmtree(self.local_root_path)
            print('deleted', self.local_root_path)

        ## create directory tree + download resource
        ResourceFuture = Future[tuple[int, str]]
        future_to_path: dict[ResourceFuture, PurePath] = dict()
        # ThreadPoolExecutor will choose max_workers automatically when set to None
        max_workers: Optional[int] = None if self.do_async else 1
        
        executor: ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers) as executor:
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

                # download resource
                future: ResourceFuture = executor.submit(self.resource_backup, resource_path)
                future_to_path[future] = resource_path

            future: ResourceFuture
            for future in as_completed(future_to_path):
                try:
                    status_code, reason = future.result()
                    if status_code != 200:
                        if status_code == 503:
                            raise ServiceUnavailableError(f'ServiceUnavailableError: {status_code} {reason}: {future_to_path[future]}')
                        raise ResponseNotOkError(f'ResponseNotOkError: {status_code} {reason}: {future_to_path[future]}')
                except ServiceUnavailableError as e:
                    print(e)
                except ResponseNotOkError as e:
                    print(e)
                except req.exceptions.RequestException as e:
                    print("Request exception:", e)
                else:
                    # status code is 200 OK and no exceptions raised
                    print(f'{status_code} {reason}: {future_to_path[future]}')
            
            # every request is now completed; ThreadPoolExecutor is automatically shut down
                    


    def resource_backup(self, resource_path: PurePath) -> tuple[int, str]:
        raise NotImplementedError
