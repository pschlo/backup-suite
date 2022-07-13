from typing import Optional
import requests as req
import lxml.etree as etree  # type: ignore
from urllib.parse import unquote, urlparse
from pathlib import PurePath

from backup_service import BackupService
from exceptions import ResponseNotOkError


class WebDavService(BackupService):

    session: req.Session
    username: str
    password: str

    def __init__(
        self,
        root_url: str,
        local_root_path: str,
        username: str,
        password: str,
        do_async: bool = False
        ) -> None:

        super().__init__(root_url, self.get_resources, local_root_path, do_async)
        self.session = req.Session()
        self.username = username
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

        response: req.Response = self.session.request(
            method = method,
            url = url,
            headers = header,
            auth = (self.username, self.password),
            timeout = 30,
            verify = True
            )
        # HTTP status codes:
        # 200 OK
        # 207 Multi-Status
        if response.status_code not in (200, 207):
            raise ResponseNotOkError(f'{response.status_code} {response.reason}')

        return response

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
        ) -> tuple[int, str]:

        full_local_path: PurePath = self.local_root_path / resource_path
        # print('downloading', full_local_path)

        url: str = self.conn_info.resource_path_to_url(resource_path)
        response: req.Response = self.send_request('GET', url)

        # KiB: int = 2**10
        MiB: int = 2**20
        # write file
        with open(full_local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=10*MiB):
                f.write(chunk)

        return response.status_code, response.reason
