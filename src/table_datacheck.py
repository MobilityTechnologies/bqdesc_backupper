import logging
import os
import sys

app_home = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(app_home, "lib"))
sys.path.append(os.path.join(app_home, "conf"))

# load config
from conf.config import Config

config = Config()

# load library
from lib.firestore import Firestore

# Logger setting
logger = logging.getLogger()
firestore = Firestore(config, logger)

if __name__ == '__main__':
    firestore.firestore_data_check()
    firestore.print_table_description_stats()
