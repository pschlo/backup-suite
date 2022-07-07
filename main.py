from typing import Any, Optional
import requests as req

def main():
    config1 = HttpConfig([
        'https://freetestdata.com/wp-content/uploads/2021/09/Free_Test_Data_100KB_MP3.mp3',
        'https://freetestdata.com/wp-content/uploads/2021/09/Free_Test_Data_500KB_MP3.mp3',
        'https://freetestdata.com/wp-content/uploads/2021/09/Free_Test_Data_1OMB_MP3.mp3'
        ], '')
    
    suite = BackupSuite(config1)
    suite.backup()




class BackupConfig:
    sources: list[str]
    dest: str

    def __init__(self, sources: list[str], dest: str) -> None:
        self.sources = sources
        self.dest = dest

    def backup(self) -> None:
        raise NotImplementedError



class WebDavConfig(BackupConfig):
    def __init__(self, sources: list[str], dest: str) -> None:
        super().__init__(sources, dest)



class HttpConfig(BackupConfig):
    session: req.Session
    login: str = ''
    password: str = ''
    verify: bool = True
    timeout: int = 30

    def __init__(self, sources: list[str], dest: str) -> None:
        super().__init__(sources, dest)
        self.session = req.Session()

    def backup(self) -> None:
        for url in self.sources:
            # run HTTP GET request
            self.download_file(url)
            # store at dest
            pass
    
    def download_file(self, url: str) -> None:
        GiB: int = 2**10
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
            # print(r.headers)
            r.raise_for_status()
            # write file
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8*GiB):
                    f.write(chunk)





class SshConfig(BackupConfig):
    def __init__(self, sources: list[str], dest: str) -> None:
        super().__init__(sources, dest)



class BackupSuite:
    configs: tuple[BackupConfig]

    def __init__(self, *configs: BackupConfig) -> None:
        self.configs = configs
        pass

    def backup(self):
        for config in self.configs:
            config.backup()




if __name__ == '__main__':
    main()