import datetime
import unittest

from init import config, logger, ignore_warnings

from lib.bigquery import Bigquery
from lib.controller import Controller
from lib.dataset_desc import DatasetDesc
from lib.firestore import Firestore
from lib.localfile import LocalFile
from lib.table_desc import TableDesc

TEST_DS = "test_bqdesc_backuper"
TEST_TABLE = "update_test"
TEST_COL1 = "col1"
TEST_COL2 = "col2"


class TestControllerBase(unittest.TestCase):
    """Base class for controller test case. `self.storage` will be parameterized.
    """
    __test__ = False  # HACK: skip base class tests

    def setUp(self):
        self.controller = Controller(config=config, logger=logger)
        self.bq = Bigquery(config=config, logger=logger)
        self.project_id = config.gcp_project
        self.storage = None  # specified in parameterized test class
        self.table_reference = {
            "projectId": self.project_id,
            "datasetId": TEST_DS,
            "tableId": TEST_TABLE
        }
        self.dataset_reference = {
            "projectId": self.project_id,
            "datasetId": TEST_DS
        }

    @ignore_warnings
    def test_backup_table(self):
        # Update BigQuery
        ymd_desc = "{0}".format(datetime.datetime.now())
        new_table_dict = {
            'description': ymd_desc,
            "schema": {"fields": [{"name": TEST_COL1, "description": "a", "type": "STRING"},
                                  {"name": TEST_COL2, "description": "a", "type": "STRING"}]},
            'tableReference': self.table_reference
        }
        table_desc = TableDesc(in_dict=new_table_dict)
        self.bq.update_table_desc(table_desc)
        # backup
        self.controller.backup_table(TEST_DS, TEST_TABLE)
        # check db
        table_desc = self.storage.get_table_desc(TEST_DS, TEST_TABLE)
        self.assertEqual(ymd_desc, table_desc.description)

    @ignore_warnings
    def test_restore_table(self):
        ymd_desc = "{0}".format(datetime.datetime.now())
        new_table_dict = {
            'description': ymd_desc,
            "schema": {"fields": [{"name": TEST_COL1, "description": "a", "type": "STRING"},
                                  {"name": TEST_COL2, "description": "a", "type": "STRING"}]},
            'tableReference': self.table_reference
        }
        table_desc = TableDesc(in_dict=new_table_dict)
        # Update storage
        self.storage.put_table_desc(dataset_id=TEST_DS, table_id=TEST_TABLE, table_desc=table_desc)
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
        # check storage
        dataset_desc = self.storage.get_dataset_desc(TEST_DS)
        self.assertEqual(ymd_desc, dataset_desc.description)

    @ignore_warnings
    def test_restore_desc(self):
        ymd_desc = "{0}".format(datetime.datetime.now())
        new_dataset_dict = {"description": ymd_desc, "datasetReference": self.dataset_reference}
        dataset_desc = DatasetDesc(in_dict=new_dataset_dict)
        # update storage
        self.storage.put_dataset_desc(dataset_id=TEST_DS, dataset_desc=dataset_desc)
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


# parameterize self.storage
class TestFireStore(TestControllerBase):
    __test__ = True

    def setUp(self):
        super().setUp()
        self.storage = Firestore(config=config, logger=logger)


class TestLocalFile(TestControllerBase):
    __test__ = True

    def setUp(self):
        super().setUp()
        self.storage = LocalFile(config=config, logger=logger)


if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestFireStore))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestLocalFile))
    unittest.TextTestRunner(verbosity=2).run(suite)
