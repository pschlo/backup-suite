from typing import Any, Callable, Optional
import requests as req
import lxml.etree as etree  # type: ignore
import shutil
from urllib.parse import ParseResult, unquote, urlparse, urlunparse
from pathlib import PurePath, Path

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
        root_url=r"https://cloud.rotex1880-cloud.org/remote.php/dav/files/backup/",
        login='backup',
        password=r";B-F$\4EeeQtVMjrZ.]r",
        dest=PurePath('D:/nextcloud-backup-test/')
        )
    
    suite = BackupSuite(config1)
    suite.backup()


    '''
    URL DEFINITIONS
    https://en.wikipedia.org/wiki/URL
    '''


class BackupConfig:
    root: Url
    get_sources: Callable[[], list[PurePath]]
    dest: PurePath
    conn_info: ParseResult

    def __init__(
        self,
        root: Url,
        get_sources: Callable[[], list[PurePath]],
        dest: PurePath
        ) -> None:

        self.root = root
        self.get_sources = get_sources
        self.dest = dest

    def full_backup(self) -> None:
        resources: list[PurePath] = self.get_sources()
        
        ## delete dest dir

        if Path(self.dest).exists():
            shutil.rmtree(self.dest)
            print('deleted', self.dest)

        ## create directory tree

        created_paths: set[PurePath] = set()
        resource: PurePath
        for resource in resources:
            # remove root folder prefix
            dirname: PurePath = resource.relative_to(PurePath(self.conn_info.path).relative_to('/'))
            # remove filename
            dirname = dirname.parent

            full_local_path: Path = Path(self.dest / dirname)

            # create dirs
            if full_local_path not in created_paths:
                print(f'creating dir {full_local_path}')
                full_local_path.mkdir(parents=True, exist_ok=True)
                created_paths.add(full_local_path)

        ## download files

        for resource in resources:
            self.resource_backup(resource, self.dest)


    
    def resource_backup(self, resource_path: PurePath, dest: PurePath) -> None:
        raise NotImplementedError



class WebDavConfig(BackupConfig):

    conn_info: ParseResult
    session: req.Session
    login: str
    password: str

    def __init__(
        self,
        root_url: Url,
        login: str,
        password: str,
        dest: PurePath,
        ) -> None:

        super().__init__(root_url, self.get_resources, dest)
        self.login = login
        self.password = password
        self.session = req.Session()

        # parse URL
        self.conn_info = urlparse(root_url)
    

    # returns list of resources that should be backed up
    def get_resources(self) -> list[PurePath]:
        url: str = urlunparse((self.conn_info.scheme, self.conn_info.netloc, '', '', '', ''))
        response: req.Response = self.send_request('PROPFIND', url, header={'Depth': '99'})
        return self.parse_resource_list(response.content)

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
            url = self.root,
            headers = header,
            auth = (self.login, self.password),
            timeout = 60,
            verify = True
        )

    @staticmethod
    def parse_resource_list(content: str | bytes) -> list[PurePath]:
        """Parses of response content XML from WebDAV server and extract file and directory names.

        :param content: the XML content of HTTP response from WebDAV server for getting list of files by remote path.
        :return: list of extracted file or directory names.
        """

        try:
            root: etree.ElementBase = etree.fromstring(content)  # type: ignore
            resources: list[PurePath] = []

            # search for 'response' tag in entire tree in namespace 'DAV:'
            response: etree.ElementBase
            for response in root.findall(".//{DAV:}response"):  # type: ignore
                # find first <href> element
                href_elem: etree.ElementBase = response.find(".//{DAV:}href")  # type: ignore
                # only store 'path' part of href
                path_str: str = unquote(urlparse(href_elem.text).path)
                # urlparse path is always absolute path, i.e. starts with '/'
                # create Path object as relative path, i.e. remove '/'
                path: PurePath = PurePath(path_str).relative_to('/')
                is_dir: bool = len(response.findall(".//{DAV:}collection")) > 0  # type: ignore
                # only store files; dirs are given implicitly in file paths
                if not is_dir:
                    resources.append(path)
            return resources
        except etree.XMLSyntaxError:
            return list()

    def resource_backup(
        self,
        resource_path: PurePath,
        dest: PurePath
        ) -> None:

        return
        #response = self.send_request('GET', self.root + '')







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