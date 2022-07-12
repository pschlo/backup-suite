from typing import Any, Callable, Optional
import requests as req
import lxml.etree as etree  # type: ignore
import shutil
from urllib.parse import ParseResult, unquote, urlparse, urlunparse
from pathlib import PurePath, Path
import re



# define datatypes
Url = str

def main():
    '''
    config1 = HttpConfig([
        'https://freetestdata.com/wp-content/uploads/2021/09/Free_Test_Data_100KB_MP3.mp3',
        #'https://freetestdata.com/wp-content/uploads/2021/09/Free_Test_Data_500KB_MP3.mp3',
        #'https://freetestdata.com/wp-content/uploads/2021/09/Free_Test_Data_1OMB_MP3.mp3'
        ], '')
    '''
    config1 = WebDavConfig(
        root_url=r"https://cloud.rotex1880-cloud.org:443/remote.php/dav/files/backup/?test=3#frag",
        login='backup',
        password=r";B-F$\4EeeQtVMjrZ.]r",
        local_root_path=r'D:/nextcloud-backup-test/'
        )
    
    suite = BackupSuite(config1)
    suite.backup()


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



# removes double slashes
@staticmethod
def has_double_slash(string: str) -> bool:
    return re.match(r'//+', string) != None


class ConnInfo:
    scheme: str
    hostname: str
    port: Optional[int]
    netloc: str
    root_path: PurePath
    root_url: str


    def __init__(self, root_url: str) -> None:
        # parse URL
        parse_res: ParseResult = urlparse(root_url)
        self.scheme = parse_res.scheme
        self.hostname = parse_res.hostname or ''
        self.port = parse_res.port
        self.netloc = f'{self.hostname}:{self.port}' if self.port else self.hostname
        self.root_path = PurePath(parse_res.path)
        # assert root_path is relative path
        if self.root_path.is_relative_to('/'):
            self.root_path = self.root_path.relative_to('/')

        # rebuild root url; remove params, query and fragments
        self.root_url = self.full_path_to_url(self.root_path)

        # check for double slashes
        if has_double_slash(parse_res.path):
            raise ValueError('URL contains double slash')

        # detect wrong ports
        if self.scheme != '' and self.port != None:
            if self.scheme == 'http' and self.port != 80:
                print("WARN: HTTP port is not 80")
            if self.scheme == 'https' and self.port != 443:
                print("WARN: HTTPS port is not 443")


    # converts resource path to URL
    def resource_path_to_url(self, resource_path: PurePath) -> str:
        return self.full_path_to_url(self.root_path / resource_path)

    # converts full remote path to URL
    def full_path_to_url(self, full_path: PurePath) -> str:
        return urlunparse((self.scheme, self.netloc, full_path.as_posix(), '', '', ''))



class BackupConfig:

    conn_info: ConnInfo
    get_sources: Callable[[], list[PurePath]]
    local_root_path: PurePath

    def __init__(
        self,
        root_url: str,
        get_sources: Callable[[], list[PurePath]],
        local_root_path: str
        ) -> None:

        self.conn_info = ConnInfo(root_url)
        self.get_sources = get_sources
        self.local_root_path = PurePath(local_root_path)

    def full_backup(self) -> None:
        resource_paths: list[PurePath] = self.get_sources()
        
        ## delete dest dir

        if Path(self.local_root_path).exists():
            shutil.rmtree(self.local_root_path)
            print('deleted', self.local_root_path)

        ## create directory tree + download resource

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
            self.resource_backup(resource_path)
            


    def resource_backup(self, resource_path: PurePath) -> None:
        raise NotImplementedError



class WebDavConfig(BackupConfig):

    session: req.Session
    login: str
    password: str

    def __init__(
        self,
        root_url: str,
        local_root_path: str,
        login: str,
        password: str
        ) -> None:

        super().__init__(root_url, self.get_resources, local_root_path)
        self.session = req.Session()
        self.login = login
        self.password = password


    # returns list of resources that should be backed up
    def get_resources(self) -> list[PurePath]:
        response: req.Response = self.send_request('PROPFIND', self.conn_info.root_url, header={'Depth': '99'})
        return self.parse_resource_list(response.content, root_prefix=self.conn_info.root_path)

    # TODO: request error handling
    def send_request(
        self,
        method: str,
        url: str,
        header: Optional[dict[str, str]] = None
        ) -> req.Response:

        # IMPORTANT
        # accessing response.content will call response.iter_content and read in 10240 chunks (very small!)
        # using stream=False will access response.content
        # therefore: use stream=True and iterate yourself
        # https://stackoverflow.com/questions/37135880/python-3-urllib-vs-requests-performance

        return self.session.request(
            method = method,
            url = url,
            headers = header,
            auth = (self.login, self.password),
            timeout = 30,
            verify = True
        )

    @staticmethod
    def parse_resource_list(content: str | bytes, root_prefix: PurePath = PurePath('')) -> list[PurePath]:
        """Parses of response content XML from WebDAV server and extract file and directory names.

        :param content: the XML content of HTTP response from WebDAV server for getting list of files by remote path.
        :return: list of extracted file or directory names.
        """

        # root prefix must be relative path
        assert not root_prefix.is_relative_to('/')

        try:
            root_elem: etree.ElementBase = etree.fromstring(content)  # type: ignore
            resources: list[PurePath] = []

            # search for 'response' tag in entire tree in namespace 'DAV:'
            response_elem: etree.ElementBase
            for response_elem in root_elem.findall(".//{DAV:}response"):  # type: ignore
                is_dir: bool = len(response_elem.findall(".//{DAV:}collection")) > 0  # type: ignore
                # only store files; dirs are given implicitly in file paths
                if is_dir:
                    continue

                # find first <href> element
                href_elem: etree.ElementBase = response_elem.find(".//{DAV:}href")  # type: ignore
                # only store 'path' part of href
                resource_path_str: str = unquote(urlparse(href_elem.text).path)

                # create Path object as relative path, i.e. remove '/' at beginning if there is one
                resource_path: PurePath = PurePath(resource_path_str)
                if resource_path.is_relative_to('/'):
                    resource_path = resource_path.relative_to('/')

                # remove root prefix
                resource_path = resource_path.relative_to(root_prefix)

                resources.append(resource_path)
            return resources
        except etree.XMLSyntaxError:
            return list()

    def resource_backup(
        self,
        resource_path: PurePath
        ) -> None:

        full_local_path: PurePath = self.local_root_path / resource_path
        print('downloading', full_local_path)
        
        url: str = self.conn_info.resource_path_to_url(resource_path)
        response: req.Response = self.send_request('GET', url)

        KiB: int = 2**10
        MiB: int = 2**20
        # write file
        with open(full_local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=10*MiB):
                f.write(chunk)







'''
class HttpConfig(BackupConfig):
    session: req.Session
    login: str = ''
    password: str = ''
    verify: bool = True
    timeout: int = 30

    root_url: Url
    # whether we should mirror the source dir structure
    mirror_dirs: bool = False

    def __init__(self, root: Url, resources: list[Url], dest: Path) -> None:
        super().__init__(sources, dest)
        self.session = req.Session()

    def full_backup(self) -> None:
        for url in self.sources:
            # run HTTP GET request
            self.download_file(url)
            # store at dest
            pass
    
    def download_file(self, url: Url) -> None:
        KiB: int = 2**10
        MiB: int = 2**20
        local_filename: str = url.split('/')[-1]

        auth: Optional[tuple[str, str]]
        if self.login and self.password:
            auth = (self.login, self.password)
        else:
            auth = None

        req_args: dict[str, Any] = {
            'url': url,
            'auth': auth,
            'stream': True,
            'verify': self.verify,
            'timeout': self.timeout
        }
        
        with self.session.get(**req_args) as r:
            # raise any HTTP errors
            r.raise_for_status()
            # write file
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=10*MiB):
                    f.write(chunk)





class SshConfig(BackupConfig):
    def __init__(self, sources: list[Url], dest: Path) -> None:
        super().__init__(sources, dest)

'''

class BackupSuite:
    configs: tuple[BackupConfig]

    def __init__(self, *configs: BackupConfig) -> None:
        self.configs = configs
        pass

    def backup(self):
        for config in self.configs:
            config.full_backup()




if __name__ == '__main__':
    main()