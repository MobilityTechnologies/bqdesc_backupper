import logging
import os
import sys

app_home = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bqdesc_backuper")
sys.path.append(app_home)

import warnings
from urllib3.exceptions import InsecureRequestWarning


# ResourceWarning: unclosed <ssl.SSLSocketを回避するためのデコレータ
def ignore_warnings(test_func):
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            warnings.simplefilter("ignore", InsecureRequestWarning)
            warnings.simplefilter("ignore", UserWarning)
            warnings.simplefilter("ignore", DeprecationWarning)
            test_func(self, *args, **kwargs)

    return do_test


from config_for_test import Config
config = Config

# ログの設定
log_format = logging.Formatter("%(asctime)s [%(levelname)8s] %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# 標準出力へのハンドラ
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(log_format)
logger.addHandler(stdout_handler)

TEST_DS = "test_bqdesc_buckuper"
GCP_PROJECT_ID = "jtx-dwh-dev"
