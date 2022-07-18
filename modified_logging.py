from logging import LogRecord, StreamHandler, FileHandler
from typing import Any
from pathlib import PurePath


# upon calling e.g. logger.error, the logger creates a LogRecord object and passes it to every one of its handlers
# a handler can change the LogRecord object by using filters
# however, this change is permament
# this module introduces handlers that can change a LogRecord, emit it, and then undo their changes

# WARNING: Not compatible with multi-threaded handling!


# abstract class that contains mod/unmod methods suitable for changing the args attribute of a LogRecord
class ArgsModifyingHandler():
    old_args: tuple[Any]

    def mod(self, record: LogRecord):
        assert isinstance(record.args, tuple)
        self.old_args = record.args
        new_args = list(record.args)
        self.mod_args(new_args)
        record.args = tuple(new_args)

    def unmod(self, record: LogRecord):
        record.args = self.old_args
    
    def mod_args(self, args: list[Any]) -> None:
        raise NotImplementedError


# put LogRecord message together by using full path for any Path object
class VerboseHandler(ArgsModifyingHandler):
    def mod_args(self, args: list[Any]) -> None:
        for i, value in enumerate(args):
            if isinstance(value, PurePath):
                args[i] = value.as_posix()


# put LogRecord message together by only using the filename for any Path object
class NonverboseHandler(ArgsModifyingHandler):
    def mod_args(self, args: list[Any]) -> None:
        for i, value in enumerate(args):
            if isinstance(value, PurePath):
                args[i] = value.name


class ModStreamHandler(StreamHandler, NonverboseHandler):  # type: ignore
    # override
    def emit(self, record: LogRecord):
        self.mod(record)
        super().emit(record)
        self.unmod(record)


class ModFileHandler(FileHandler, VerboseHandler):
    # override
    def emit(self, record: LogRecord):
        self.mod(record)
        super().emit(record)
        self.unmod(record)
