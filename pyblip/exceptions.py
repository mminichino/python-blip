##
##

import sys
import os
import inspect
import logging

logger = logging.getLogger('pyblip.exception')
logger.addHandler(logging.NullHandler())


class FatalError(Exception):

    def __init__(self, message):
        import traceback
        logging.debug(traceback.print_exc())
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        logger.debug("Error: {} in {} {} at line {}: {}".format(type(self).__name__, filename, function, line, message))
        logger.error(f"{message} [{filename}:{line}]")
        sys.exit(1)


class NonFatalError(Exception):

    def __init__(self, message):
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        self.message = "Error: {} in {} {} at line {}: {}".format(
            type(self).__name__, filename, function, line, message)
        logger.debug(f"Caught exception: {self.message}")
        super().__init__(self.message)


class BLIPException(Exception):

    def __init__(self, number, properties, body):
        prefix = ""
        if 'Error-Domain' in properties:
            prefix = f" {properties['Error-Domain']}"
        if 'Error-Code' in properties:
            prefix = f"{prefix} {properties['Error-Code']}"
        self.message = f"BLIP Error: MSG#{number}{prefix} {body}"
        logger.debug(f"BLIP exception: {self.message}")
        super().__init__(self.message)


class CRCMismatch(NonFatalError):
    pass


class BLIPError(BLIPException):
    pass
