import logging
import os
import sys

import click

app_home = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(app_home, "lib"))
sys.path.append(os.path.join(app_home, "conf"))

# load config
from conf.config import Config

config = Config()

# load library
from lib.controller import Controller
from lib.firestore import Firestore
from lib.slack import Slack

# Logger setting
log_format = logging.Formatter("%(asctime)s [%(levelname)8s] %(message)s")
logger = logging.getLogger()
log_level_mapper = {"info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR, "debug": logging.DEBUG}
logger.setLevel(log_level_mapper[config.loglevel])

# Output log to STDOUT
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(log_format)
logger.addHandler(stdout_handler)

controller = Controller(config, logger)
firestore = Firestore(config, logger)
slack = Slack(config, logger)


@click.group(help='BigQuery Description Backuper')
@click.pass_context
def cli_main(ctx):
    pass


@cli_main.group(help='Backup BigQuery Description to FireStore')
@click.pass_context
def backup(ctx):
    pass


@cli_main.group(help='Restore from FireStore to BigQuery')
@click.pass_context
def restore(ctx):
    pass


@cli_main.group(help='FireStore Data Snapshot')
@click.pass_context
def snapshot(ctx):
    pass


@backup.command(help="Backup specified table and fields description")
@click.option('--dataset', '-d', required=True)
@click.option('--table', '-t', required=True)
def table(table, dataset):
    controller.backup_table(table_id=table, dataset_id=dataset)


@backup.command(help="Backup specified dataset description")
@click.option('--dataset', '-d', required=True)
def dataset(dataset):
    controller.backup_dataset(dataset_id=dataset)


@backup.command(help="Backup all dataset and table(fields) descriptions in project")
def all():
    controller.backup_all()


@restore.command(help="Restore specified table and fields description")
@click.option('--dataset', '-d', required=True)
@click.option('--table', '-t', required=True)
def table(table, dataset):
    controller.restore_table(table_id=table, dataset_id=dataset)


@restore.command(help="Restore specified dataset description")
@click.option('--dataset', '-d', required=True)
def dataset(dataset):
    controller.restore_dataset(dataset_id=dataset)


@restore.command(help="Restore all dataset and table(fields) description")
def all():
    controller.restore_all()


@snapshot.command(help="Make FireStore collection snapshot")
def make():
    firestore.make_db_snapshot()


@snapshot.command(help="List FireStore collection snapshots")
def list():
    for id in firestore.list_db_snapshot():
        print(id)


@snapshot.command(help="Recover table data on FireStore from specified snapshot")
@click.option('--dataset', '-d', required=True)
@click.option('--table', '-t', required=True)
@click.option('--snapshot_id', '-s', required=True, help="format is YYYYMMDD")
def recover_table(dataset, table, snapshot_id):
    firestore.recover_table_from_snapshot(dataset, table, snapshot_id)


@snapshot.command(help="Recover dataset data on FireStore from specified snapshot")
@click.option('--dataset', '-d', required=True)
@click.option('--snapshot_id', '-s', required=True, help="format is YYYYMMDD")
def recover_dataset(dataset, snapshot_id):
    firestore.recover_dataset_from_snapshot(dataset, snapshot_id)


if __name__ == "__main__":
    try:
        cli_main()
    except Exception as e:
        logger.exception(e)
        if config.enable_slack_notify:
            import traceback

            except_str = traceback.format_exc()
            slack.post_error("bqdesc_backupper Error\n" + except_str)
        sys.exit(1)
