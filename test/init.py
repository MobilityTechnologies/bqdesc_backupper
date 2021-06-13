import logging
import os
import sys

import warnings
from urllib3.exceptions import InsecureRequestWarning
def ignore_warnings(test_func):
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            warnings.simplefilter("ignore", InsecureRequestWarning)
            warnings.simplefilter("ignore", UserWarning)
            warnings.simplefilter("ignore", DeprecationWarning)
            test_func(self, *args, **kwargs)

    return do_test

app_home = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
sys.path.append(os.path.join(app_home))
from conf.config_for_test import Config
config = Config

log_format = logging.Formatter("%(asctime)s [%(levelname)8s] %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.WARNING)
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(log_format)
logger.addHandler(stdout_handler)
