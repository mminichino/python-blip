##
##

import sys
import os
import inspect
import logging
import traceback

logger = logging.getLogger('pythonblip.exception')
logger.addHandler(logging.NullHandler())


class FatalError(Exception):

    def __init__(self, message):
        logging.debug(traceback.print_exc())
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        logger.debug("Error: {} in {} {} at line {}: {}".format(type(self).__name__, filename, function, line, message))
        logger.error(f"{message} [{filename}:{line}]")
        sys.exit(1)


class NonFatalError(Exception):

    def __init__(self, message):
        tb = traceback.format_exc()
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        self.message = "Error: {} in {} {} at line {}: {}".format(
            type(self).__name__, filename, function, line, message)
        logger.debug(tb)
        super().__init__(self.message)


class BLIPException(Exception):

    def __init__(self, number, properties, body):
        self.error_domain = None
        self.error_code = None
        prefix = ""
        if 'Error-Domain' in properties:
            self.error_domain = properties['Error-Domain']
            prefix = f" {self.error_domain}"
        if 'Error-Code' in properties:
            self.error_code = int(properties['Error-Code'])
            prefix = f"{prefix} {self.error_code}"
        self.message = f"BLIP Error: MSG#{number}{prefix} {body}"
        logger.debug(f"BLIP exception: {self.message}")
        super().__init__(self.message)


class ClientException(Exception):

    def __init__(self, code, message):
        self.error_code = code
        self.message = f"Client Error: {code} {message}"
        logger.debug(f"client exception: {self.message}")
        super().__init__(self.message)


class CRCMismatch(NonFatalError):
    pass


class WebSocketError(NonFatalError):
    pass


class BLIPError(BLIPException):
    pass


class NotAuthorized(NonFatalError):
    pass


class HTTPNotImplemented(NonFatalError):
    pass


class InternalServerError(NonFatalError):
    pass


class ClientError(ClientException):
    pass


class ReplicationError(NonFatalError):
    pass


class OutputError(NonFatalError):
    pass
