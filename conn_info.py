from typing import Optional
from urllib.parse import ParseResult, urlparse, urlunparse
from pathlib import PurePath
import re
from logging import Logger, getLogger


logger: Logger = getLogger('suite.conn_info')

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
        if ConnInfo.has_double_slash(parse_res.path):
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

    # check for double slashes
    @staticmethod
    def has_double_slash(string: str) -> bool:
        return re.match(r'//+', string) != None
