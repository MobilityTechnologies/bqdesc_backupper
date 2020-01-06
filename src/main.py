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
from lib.controller import Controller
from lib.firestore import Firestore
from lib.bigquery import BqUpdateResult
from lib.slack import Slack

RESPONSE_HEADERS = {'Access-Control-Allow-Origin': '*'}

# Logger setting
from google.cloud import logging as cloud_logging
from google.cloud.logging.handlers import CloudLoggingHandler

client = cloud_logging.Client()
handler = CloudLoggingHandler(client)
logger = logging.getLogger('cloudLogger')
logger.setLevel(logging.INFO)
logger.addHandler(handler)

controller = Controller(config, logger)
firestore = Firestore(config, logger)
slack = Slack(config, logger)


def cloud_functions_main(request):
    """
    this function is invoked by Cloud Functions
    """

    param = request.get_json()
    try:
        msg = "ok"
        if param["action"] == "backup_table":
            table = param["table"]
            dataset = param["dataset"]
            controller.backup_table(table_id=table, dataset_id=dataset)
        elif param["action"] == "backup_dataset":
            dataset = param["dataset"]
            controller.backup_dataset(dataset_id=dataset)
        elif param["action"] == "backup_all":
            msg = controller.backup_all()
        elif param["action"] == "restore_table":
            table = param["table"]
            dataset = param["dataset"]
            controller.restore_table(table_id=table, dataset_id=dataset)
        elif param["action"] == "restore_dataset":
            dataset = param["dataset"]
            controller.restore_dataset(dataset_id=dataset)
        elif param["action"] == "restore_all":
            msg = controller.restore_all()
        elif param["action"] == "snapshot_make":
            firestore.make_db_snapshot()
        elif param["action"] == "snapshot_recover_table":
            table = param["table"]
            dataset = param["dataset"]
            snapshot_id = param["snapshot"]
            firestore.recover_table_from_snapshot(dataset, table, snapshot_id)
        elif param["action"] == "snapshot_recover_dataset":
            dataset = param["dataset"]
            snapshot_id = param["snapshot"]
            firestore.recover_dataset_from_snapshot(dataset, snapshot_id)
        else:
            raise Exception("unknown action: " + param["action"])
        return (msg, 200, RESPONSE_HEADERS)
    except Exception as e:
        logger.exception(e)
        if config.enable_slack_notify:
            import traceback
            except_str = traceback.format_exc()
            slack.post_error("bqdesc_backupper Error\n" + except_str)
        return (f"Exception : {e}", 500, RESPONSE_HEADERS)
