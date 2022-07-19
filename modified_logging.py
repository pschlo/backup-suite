import logging
from logging import LogRecord, Formatter, LoggerAdapter, _acquireLock, _releaseLock  # type: ignore
from typing import Any
from pathlib import PurePath


# when calling e.g. logger.info, the logger creates a new LogRecord
# the logger then passes the record to its Handlers, which in turn pass it to their Formatter
# the Formatter then asks the LogRecord for its message by calling 'record.getMessage'
# the LogRecord then creates the log message from the msg and the format args passed by the user, and returns it
# e.g. when calling logger.info('Created file %s', '/mnt/file.txt'), the LogRecord returns 'Created file /mnt/file.txt' when self.getMessage is called
# the Formatter stores this message in the LogRecord since it is the same for each Formatter anyway
# it then formats the message as specified, e.g. by adding a time stamp to the front, and returns the formatted message to the Handler

# we change this behaviour by moving the creation of the LogRecord message to the Formatters and away from the LogRecord
# this way, each Formatter can not only format around the message (e.g. add time stamp to front), but also decide themselves how they want to create the message
# to achieve this, we override the getMessage method to return the message already set in the record (this way we can keep using the superclass format method)
# in the formatter, we then set the message attribute of the record ourselves


class ModRecord(LogRecord):
    def getMessage(self) -> str:
        return self.message

# set LogRecord subclass used by Loggers upon log record creation
logging.setLogRecordFactory(ModRecord)


# formatter that modifies the args and appends extra lines
# because the message attribute of the LogRecord might change from Formatter to Formatter, we need locks
# this can only be avoided by not calling Formatter.format and doing the formatting yourself, but this is too much effort
class ModFormatter(Formatter):
    def format(self, record: LogRecord) -> str:
        message = str(record.msg)

        # modify args
        assert isinstance(record.args, tuple)
        new_args_tmp = list(record.args)
        self.modify_args(new_args_tmp)
        new_args = tuple(new_args_tmp)

        # append additional lines
        # can either be a list of strings or a single string containing line breaks
        if hasattr(record, 'lines'):
            lines: str | list[str] = record.lines  # type: ignore
            if isinstance(lines, str):
                # split at newline
                lines = lines.splitlines()
            message = self.append_lines(message, lines)

        # merge message with user-specified format args
        if new_args:
            message = message % new_args

        _acquireLock()
        record.message = message
        # let superclass handle further formatting
        formatted_msg: str =  super().format(record)
        _releaseLock()

        return formatted_msg

    def modify_args(self, args: list[Any]) -> None:
        raise NotImplementedError
    
    def append_lines(self, message: str, lines: list[str]) -> str:
        raise NotImplementedError


# append lines with line break
class MultiLineFormatter(ModFormatter):
    def append_lines(self, message: str, lines: list[str]) -> str:
            for line in lines:
                message += '\n\t\t' + line
            return message


# append lines as a single line
class SingleLineFormatter(ModFormatter):
    def append_lines(self, message: str, lines: list[str]) -> str:
        for line in lines:
            message += '  ' + line
        return message


# put message together using full paths
class VerboseFormatter(ModFormatter):
    def modify_args(self, args: list[Any]) -> None:
        for i, value in enumerate(args):
            if isinstance(value, PurePath):
                args[i] = value.as_posix()


# put message together using only file/folder names as paths
class NonverboseFormatter(ModFormatter):
    def modify_args(self, args: list[Any]) -> None:
        for i, value in enumerate(args):
            if isinstance(value, PurePath):
                args[i] = value.name


class ConsoleFormatter(NonverboseFormatter, MultiLineFormatter):
    pass

class FileFormatter(VerboseFormatter, MultiLineFormatter):
    pass


# adapter that allows creation of multi-line log records
class MultiLineLogger(LoggerAdapter):  # type: ignore
    logger: logging.Logger

    def process(self, msg: Any, kwargs: Any) -> tuple[Any, Any]:
        if 'lines' in kwargs:
            if 'extra' not in kwargs:
                kwargs['extra'] = dict()
            kwargs['extra']['lines'] = kwargs['lines']
            del kwargs['lines']
        return msg, kwargs
    
    def addHandler(self, *args: Any, **kwargs: Any):
        self.logger.addHandler(*args, **kwargs)

    def addFilter(self, *args: Any, **kwargs: Any):
        self.logger.addFilter(*args, **kwargs)


def getLogger(name: str):
    return MultiLineLogger(logging.getLogger(name))
