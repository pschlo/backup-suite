from typing import Any, Callable, Optional
import time
import requests as req
import lxml.etree as etree
import os
import shutil
import re
from urllib.parse import unquote, urlsplit, urlparse

# define datatypes
Url = str
Path = str

def main():
    '''
    config1 = HttpConfig([
        'https://freetestdata.com/wp-content/uploads/2021/09/Free_Test_Data_100KB_MP3.mp3',
        #'https://freetestdata.com/wp-content/uploads/2021/09/Free_Test_Data_500KB_MP3.mp3',
        #'https://freetestdata.com/wp-content/uploads/2021/09/Free_Test_Data_1OMB_MP3.mp3'
        ], '')
    '''
    config1 = WebDavConfig(
        root=r"https://cloud.rotex1880-cloud.org//remote.php/dav/files/backup/",
        login='backup',
        password=r";B-F$\4EeeQtVMjrZ.]r",
        dest='D:/nextcloud-backup-test/')
    
    suite = BackupSuite(config1)
    suite.backup()


    '''
    URL DEFINITIONS
    https://en.wikipedia.org/wiki/URL
    '''


    def get_url(self, path):
        """Generates url by uri path.

        :param path: uri path.
        :return: the url string.
        """
        url = {'hostname': self.webdav.hostname, 'root': self.webdav.root, 'path': path}
        return "{hostname}{root}{path}".format(**url)

    def get_full_path(self, urn):
        """Generates full path to remote resource exclude hostname.

        :param urn: the URN to resource.
        :return: full path to resource with root path.
        """
        return "{root}{path}".format(root=unquote(self.webdav.root), path=urn.path())

    def get_rel_path(self, urn):
        """Removes root path from URN.

        :param urn: the URN to resource.
        :return: relative path to resource without root path.
        """
        return re.sub(f'^{self.webdav.root}', '', urn.path())


class BackupConfig:
    root: Url
    get_sources: Callable[[], list[str]]
    dest: Path

    def __init__(
        self,
        root: Url,
        get_sources: Callable[[], list[str]],
        dest: Path
        ) -> None:

        self.root = root
        self.get_sources = get_sources
        self.dest = dest

    def full_backup(self) -> None:
        sources: list[str] = self.get_sources()
        
        source: str
        for source in sources:
            self.resource_backup(source, self.dest)
    
    def resource_backup(self, resource: str, dest: Path) -> None:
        raise NotImplementedError







class WebDavConfig(BackupConfig):

    session: req.Session
    login: str
    password: str

    def __init__(
        self,
        root: Url,
        login: str,
        password: str,
        dest: Path
        ) -> None:

        super().__init__(root, self.get_resources, dest)
        self.login = login
        self.password = password
        self.session = req.Session()
    

    def get_resources(self) -> list[str]:
        response = self.send_request('PROPFIND', self.root, header={'Depth': '99'})
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
    def parse_resource_list(content: str | bytes) -> list[str]:
        """Parses of response content XML from WebDAV server and extract file and directory names.

        :param content: the XML content of HTTP response from WebDAV server for getting list of files by remote path.
        :return: list of extracted file or directory names.
        """
        try:
            root: etree.ElementBase = etree.fromstring(content, None)
            urns: list[str] = []

            # search for 'response' tag in entire tree in namespace 'DAV:'
            response: etree.ElementBase
            for response in root.findall(".//{DAV:}response", None):
                href_elem: etree.ElementBase = response.find(".//{DAV:}href", None)
                urns.append(href_elem.text)
                a = urlsplit(href_elem.text)
                #is_dir = len(response.findall(".//{DAV:}collection")) > 0
                #urns.append(Urn(href, is_dir))
            return urns
        except etree.XMLSyntaxError:
            return list()

    def resource_backup(
        self,
        resource: str,
        dest: Path
        ) -> None:

        if os.path.exists(dest + resource):
            #shutil.rmtree(dest + resource)
            pass
        print(f'creating dir {dest+resource}')
        #os.makedirs(dest + resource)

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