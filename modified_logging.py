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


# use these for shorter log output
# max 5 chars per name
level_to_name = {
    logging.CRITICAL: 'CRIT',
    logging.ERROR: 'ERROR',
    logging.WARNING: 'WARN',
    logging.INFO: 'INFO',
    logging.DEBUG: 'DEBUG',
    logging.NOTSET: 'NTSET',
}

# mapping of thread IDs to job names
thread_to_jobname: dict[int, str] = dict()

# how long the prefix of a record message is, excluding the jobname
LEN_RECORD_PREFIX: int = 19


# Note: This class might also make adding filters to change attributes redundant, because changes to a LogRecord after it has been created can just be done in the __init__ method here
class ModRecord(LogRecord):
    # short level name
    slevelname: str
    # name of the job running in the thread that produced this record
    jobname: str
    # total length of the record prefix, including the jobname
    prefix_length: int

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        # set short level name
        self.slevelname = level_to_name[self.levelno]

        if self.thread in thread_to_jobname:
            self.jobname = thread_to_jobname[self.thread]
        else:
            self.jobname = 'Main'

        self.prefix_length = LEN_RECORD_PREFIX + len(self.jobname)

    def getMessage(self) -> str:
        # if formatter is ModFormatter, message attribute is already set
        if hasattr(self, 'message') and self.message is not None:
            return self.message
        # otherwise default behaviour
        return super().getMessage()


# set LogRecord subclass used by Loggers upon log record creation
logging.setLogRecordFactory(ModRecord)


# formatter that modifies the args and appends extra lines
# because the message attribute of the LogRecord might change from Formatter to Formatter, we need locks
# this could only be avoided by not calling Formatter.format and doing the formatting yourself, but this is too much effort
# works with any LogRecord class, but needs ModRecord if args or lines should be modified
class ModFormatter(Formatter):
    def format(self, record: LogRecord) -> str:
        if not isinstance(record, ModRecord):
            return super().format(record)

        message = str(record.msg)

        # modify args
        assert isinstance(record.args, tuple)
        new_args_tmp = list(record.args)
        self.modify_args(new_args_tmp)
        new_args = tuple(new_args_tmp)

        # append additional lines
        # can either be a list of strings or a single string containing line breaks
        if hasattr(record, 'lines'):
            lines: str | list[Any] = record.lines  # type: ignore
            if isinstance(lines, str):
                # split at newline
                lines = lines.splitlines()
            # lines is now list of objects
            # apply args modifier
            mod_lines = lines.copy()
            self.modify_args(mod_lines)
            message = self.append_lines(message, mod_lines, len_prefix=record.prefix_length)

        # merge message with user-specified format args
        if new_args:
            message = message % new_args

        _acquireLock()
        record.message = message
        # let superclass handle further formatting
        formatted_msg: str = super().format(record)
        _releaseLock()

        return formatted_msg

    def modify_args(self, args: list[Any]) -> None:
        # do nothing if not overridden
        pass
    
    def append_lines(self, msg: str, lines: list[str], len_prefix: int) -> str:
        # only called if 'lines' keyword argument was given
        # need to handle extra lines somehow; raise if not overridden
        raise NotImplementedError("Extra lines given, but handling of extra lines is not defined")


# append lines with line break
class MultiLineFormatter(ModFormatter):
    def append_lines(self, msg: str, lines: list[str], len_prefix: int) -> str:
            for line in lines:
                msg += '\n' + ' ' * (len_prefix+2) + line
            return msg


# append lines as a single line
class SingleLineFormatter(ModFormatter):
    def append_lines(self, msg: str, lines: list[str], len_prefix: int) -> str:
        for line in lines:
            msg += '  ' + line
        return msg


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


# Logger that can accept a 'lines' keyword argument
# created LogRecords will have a 'lines' attribute
# Otherwise, it is equivalent to logging.Logger
class MultiLineLogger(logging.Logger):
    def debug(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        msg, kwargs = self._process(msg, kwargs)
        return super().debug(msg, *args, **kwargs)

    def info(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        msg, kwargs = self._process(msg, kwargs)
        return super().info(msg, *args, **kwargs)

    def warning(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        msg, kwargs = self._process(msg, kwargs)
        return super().warning(msg, *args, **kwargs)

    def warn(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        msg, kwargs = self._process(msg, kwargs)
        return super().warn(msg, *args, **kwargs)

    def error(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        msg, kwargs = self._process(msg, kwargs)
        return super().error(msg, *args, **kwargs)

    def exception(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        msg, kwargs = self._process(msg, kwargs)
        return super().exception(msg, *args, **kwargs)

    def critical(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        msg, kwargs = self._process(msg, kwargs)
        return super().critical(msg, *args, **kwargs)

    def fatal(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        msg, kwargs = self._process(msg, kwargs)
        return super().fatal(msg, *args, **kwargs)

    def log(self, level: Any, msg: Any, *args: Any, **kwargs: Any) -> None:
        msg, kwargs = self._process(msg, kwargs)
        return super().log(level, msg, *args, **kwargs)

    def _process(self, msg: Any, kwargs: Any) -> tuple[Any, Any]:
        # create 'extra' argument
        if 'extra' not in kwargs:
            kwargs['extra'] = dict()

        # pass additional lines
        if 'lines' in kwargs:
            kwargs['extra']['lines'] = kwargs['lines']
            del kwargs['lines']

        return msg, kwargs


logging.setLoggerClass(MultiLineLogger)

