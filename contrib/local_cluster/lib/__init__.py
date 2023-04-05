import logging
import sys


def _setup_logs():
    logger = logging.getLogger(__name__)

    logger.handlers.clear()
    
    log_format = logging.Formatter(
        "%(asctime)s | %(name)20s :: %(levelname)-8s :: %(message)s"
    )

    stdout_handler = logging.StreamHandler(stream=sys.stderr)
    stdout_handler.setFormatter(log_format)

    logger.addHandler(stdout_handler)


_setup_logs()
