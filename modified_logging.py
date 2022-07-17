import logging
from typing import Any
from pathlib import PurePath


# itermediate abstract class for LogRecord subclasses that modify the format args
class ModifiedLogRecord(logging.LogRecord):
    def modify(self, args: list[Any]) -> None:
        raise NotImplementedError

    def getMessage(self) -> str:
        assert isinstance(self.args, tuple)
        old_args: tuple[Any] = self.args
        # copy old args, modify it in-place and update args attribute
        new_args = list(old_args)
        self.modify(new_args)
        self.args = tuple(new_args)
        # call parent
        msg = super().getMessage()
        # restore old args
        self.args = old_args
        return msg


# put message together by only using the filename for any Path object
class NonverboseRecord(ModifiedLogRecord):
    def modify(self, args: list[Any]) -> None:
        for i, value in enumerate(args):
            if isinstance(value, PurePath):
                args[i] = value.name
        

# put message together by using full path for any Path object
class VerboseRecord(ModifiedLogRecord):
    def modify(self, args: list[Any]) -> None:
        for i, value in enumerate(args):
            if isinstance(value, PurePath):
                args[i] = value.as_posix()


# a handler formats the message of a LogResult py passing it to a Formatter and then emits the result to a destination
# the Formatter extracts some details from the LogResult (e.g. time, thread number, ...), which it can use for formatting
# these details also include the log message, which was formatted by LogResult from the string and format arguments passed by the application,
# e.g. with 'logger.error('File %s is invalid', filename)
# By default, handlers can therefore not change the message but only the formatting around it (e.g. add a timestamp to the beginning)
# modified stream handlers circumvent this problem by temporarily casting the LogResult to a subclass that overrides the getMessage() method
# when the Formatter then calls record.getMessage(), the message is put together by the LogResult subclass

class ModStreamHandler(logging.StreamHandler):  # type: ignore
    def format(self, record: logging.LogRecord) -> str:
        # cast record to subclass
        record.__class__ = NonverboseRecord
        msg: str = super().format(record)
        # cast back
        record.__class__ = logging.LogRecord
        return msg


class ModFileHandler(logging.FileHandler):
    def format(self, record: logging.LogRecord) -> str:
        # cast record to subclass
        record.__class__ = VerboseRecord
        msg: str = super().format(record)
        # cast back
        record.__class__ = logging.LogRecord
        return msg