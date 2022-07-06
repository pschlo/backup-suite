from typing import Any

def main():
    source1 = WebDavConfig(["url1", "url2"], "dest1")
    source2 = HttpConfig(["test"], "dest2");

    suite1 = BackupSuite(source1, source1)
    suite2 = BackupSuite(source2, source2)

    suite1.backup()
    # print(suite2.sources)

    # source1.backup()




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
    def __init__(self, sources: list[str], dest: str) -> None:
        super().__init__(sources, dest)

    def backup(self) -> None:
        for url in self.sources:
            # run HTTP GET request
            # store at dest
            pass


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