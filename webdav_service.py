from typing import Optional
import requests as req
import lxml.etree as etree  # type: ignore
from urllib.parse import unquote, urlparse
from pathlib import PurePath

from backup_service import BackupService
from exceptions import ResponseNotOkError, ServiceUnavailableError


class WebDavService(BackupService):

    # how many levels of the directory tree the WebDAV server should scan for resources
    # setting to 1 will only scan one level deep, i.e. get resources with path <root_path>/<resource>
    # setting to high value will likely get every nested resource in root folder
    PROPFIND_DEPTH: int = 99

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
        response: req.Response = self.send_request('PROPFIND', self.conn_info.root_url, header={'Depth': str(self.PROPFIND_DEPTH)})
        return self.parse_resource_list(response.content, root_prefix=self.conn_info.root_path)

    
    # every request must come from this method
    # response status codes are checked, raises req.exceptions.RequestException subclass if check failed
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

        r: req.Response
        try:
            r = self.session.request(
                method = method,
                url = url,
                headers = header,
                auth = (self.username, self.password),
                timeout = 30,
                verify = True
                )
        # this does NOT check the HTTP status code
        except req.exceptions.Timeout as e:
            # TODO: retry (increase timeout?)
            raise e
        except req.exceptions.RequestException as e:
            raise e


        '''
        HTTP status codes:
        200 OK
        207 Multi-Status

        - WebDAV PROPFIND returns a '207 Multi-Status' response
        - multi status response code tells if a property of a resource is available or not
        - do not fetch multi status response code since only existence of resource is relevant
        '''


        # check for bad HTTP status code
        # raise subclasses of req.HTTPError

        # explicitly catch some status codes
        if r.status_code == 503:
            raise ServiceUnavailableError(f'ServiceUnavailableError: {r.status_code} {r.reason}: {url}')

        # catch all other status codes that are not OK
        if method == 'PROPFIND':
            if r.status_code != 207:
                raise ResponseNotOkError(f'{r.status_code} {r.reason}: {url}')
            else:
                # success
                pass
        else:
            if r.status_code != 200:
                raise ResponseNotOkError(f'ResponseNotOkError: {r.status_code} {r.reason}: {url}')
            else:
                # success
                pass

        return r


    # parse PROPFIND response
    # return list of resource paths, excluding dirs
    @staticmethod
    def parse_resource_list(content: str | bytes, root_prefix: PurePath = PurePath('')) -> list[PurePath]:

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


    def download_resource(
        self,
        resource_path: PurePath
        ):

        full_local_path: PurePath = self.local_root_path / resource_path
        # print('downloading', full_local_path)

        url: str = self.conn_info.resource_path_to_url(resource_path)

        r: req.Response
        try:
            r = self.send_request('GET', url)
        except req.exceptions.HTTPError as e:
            raise e
        except req.exceptions.RequestException as e:
            raise e

        # KiB: int = 2**10
        MiB: int = 2**20
        # write file
        with open(full_local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=10*MiB):
                f.write(chunk)


