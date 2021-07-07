import datetime
import unittest

import pytest

from test.init import config, logger, ignore_warnings
from src.lib.bigquery import Bigquery
from src.lib.controller import Controller
from src.lib.dataset_desc import DatasetDesc
from src.lib.firestore import Firestore
from src.lib.table_desc import TableDesc

TEST_DS = "test_bqdesc_backuper"
TEST_TABLE = "update_test"
TEST_COL1 = "col1"
TEST_COL2 = "col2"


@pytest.mark.gcp_project
class TestController(unittest.TestCase):
    def setUp(self):
        self.controller = Controller(config=config, logger=logger)
        self.bq = Bigquery(config=config, logger=logger)
        self.firestore = Firestore(config=config, logger=logger)
        self.project_id = config.gcp_project
        self.table_reference = {"projectId": self.project_id, "datasetId": TEST_DS, "tableId": TEST_TABLE}
        self.dataset_reference = {"projectId": self.project_id, "datasetId": TEST_DS}

    @ignore_warnings
    def test_backup_table(self):
        # Update BigQuery
        ymd_desc = "{0}".format(datetime.datetime.now())
        new_table_dict = {
            'description': ymd_desc,
            "schema": {
                "fields": [{
                    "name": TEST_COL1,
                    "description": "a",
                    "type": "STRING"
                }, {
                    "name": TEST_COL2,
                    "description": "a",
                    "type": "STRING"
                }]
            },
            'tableReference': self.table_reference
        }
        table_desc = TableDesc(in_dict=new_table_dict)
        self.bq.update_table_desc(table_desc)
        # backup
        self.controller.backup_table(TEST_DS, TEST_TABLE)
        # check db
        table_desc = self.firestore.get_table_desc(TEST_DS, TEST_TABLE)
        self.assertEqual(ymd_desc, table_desc.description)

    @ignore_warnings
    def test_restore_table(self):
        ymd_desc = "{0}".format(datetime.datetime.now())
        new_table_dict = {
            'description': ymd_desc,
            "schema": {
                "fields": [{
                    "name": TEST_COL1,
                    "description": "a",
                    "type": "STRING"
                }, {
                    "name": TEST_COL2,
                    "description": "a",
                    "type": "STRING"
                }]
            },
            'tableReference': self.table_reference
        }
        table_desc = TableDesc(in_dict=new_table_dict)
        # Update DB
        self.firestore.put_table_desc(dataset_id=TEST_DS, table_id=TEST_TABLE, table_desc=table_desc)
        # restore
        self.controller._restore_table(dataset_id=TEST_DS, table_id=TEST_TABLE)
        # check BD
        table_desc = self.bq.get_table_desc(dataset_id=TEST_DS, table_id=TEST_TABLE)
        self.assertEqual(ymd_desc, table_desc.description)

    @ignore_warnings
    def test_backup_dataset(self):
        # Update BQ
        ymd_desc = "{0}".format(datetime.datetime.now())
        new_dataset_dict = {"description": ymd_desc, "datasetReference": self.dataset_reference}
        dataset_desc = DatasetDesc(in_dict=new_dataset_dict)
        self.bq.update_dataset_desc(dataset_desc=dataset_desc)
        # backup
        self.controller.backup_dataset(TEST_DS)
        # check DB
        dataset_desc = self.firestore.get_dataset_desc(TEST_DS)
        self.assertEqual(ymd_desc, dataset_desc.description)

    @ignore_warnings
    def test_restore_desc(self):
        ymd_desc = "{0}".format(datetime.datetime.now())
        new_dataset_dict = {"description": ymd_desc, "datasetReference": self.dataset_reference}
        dataset_desc = DatasetDesc(in_dict=new_dataset_dict)
        # update DB
        self.firestore.put_dataset_desc(dataset_id=TEST_DS, dataset_desc=dataset_desc)
        # restore
        self.controller._restore_dataset(dataset_id=TEST_DS)
        # check BQ
        dataset_desc = self.bq.get_dataset_desc(dataset_id=TEST_DS)
        self.assertEqual(ymd_desc, dataset_desc.description)

    # @ignore_warnings
    # def test_backup_all(self):
    #    self.controller.backup_all()

    # @ignore_warnings
    # def test_restore_all(self):
    #    self.controller.restore_all()


if __name__ == '__main__':
    unittest.main(warnings='ignore')
