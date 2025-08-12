import importlib
import logging
import re
from typing import Optional

from qtpy.QtGui import QIcon, QPixmap

logger = logging.getLogger(__name__)


def load_resource(path: str) -> Optional[bytes]:
    path = path.removeprefix('raw.githubusercontent.com/mxmlnkn/ratarmount/master/')
    try:
        split = re.sub('/+', '/', path.strip('/')).rsplit('/', maxsplit=1)
        module = "ratarmount.widgets.resources"
        if len(split) > 1:
            module += '.' + split[0].replace('/', '.')
        return importlib.resources.read_binary(module, split[-1])
    except ModuleNotFoundError as exception:
        logger.error(
            "Unable to load resource '%s' because of: %s",
            path,
            exception,
            exc_info=logger.isEnabledFor(logging.DEBUG),
        )
    return None
