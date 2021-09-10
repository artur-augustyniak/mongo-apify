#!/usr/bin/env python3
import logging
import os

logger = logging.getLogger(__name__)


NOT_FOUND_RESP = {
    "detail": "object not found",
    "status": 404,
}


def error_handler(api_method):
    def wrapper(**kwargs):
        try:
            return api_method(**kwargs)
        except Exception as e:
            exc = str(e)
            logger.error("api call %s failed with error %s(%s)" %
                         (api_method, type(e), exc))
            if os.environ.get('LOGLEVEL', "DEBUG") == "DEBUG":
                msg = exc
            else:
                msg = "Something went wrong, call 911"
            return {
                "detail": msg,
                "status": 500
            }, 500
    return wrapper


if __name__ == '__main__':
    pass
