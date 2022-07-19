from pathlib import PurePath
import re


INVALID_WIN_CHARS: str = r'[\<\>\:\"\/\|\?\*]+'


# remove invalid chars from path
def clean_path(path: PurePath) -> PurePath:
    return PurePath(re.sub(INVALID_WIN_CHARS, '', str(path)))