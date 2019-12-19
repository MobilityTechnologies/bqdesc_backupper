import datetime
import unittest

from init import config, logger, ignore_warnings

from dataset_desc import DatasetDesc
from firestore import Firestore
from table_desc import TableDesc

TEST_DS = "test_bqdesc_buckuper"
TEST_TABLE = "update_test"

class TestFireStore(unittest.TestCase):

    def setUp(self):
        self.db = Firestore(config, logger)

    @ignore_warnings
    def test_table_put_get(self):
        rand_desc = "{0}".format(datetime.datetime.now())
        table_dict = {
            'description': rand_desc,
            'schema': {'fields': [{'name': 'col1', 'type': 'STRING', 'description': rand_desc}]},
            'tableReference': {'projectId': 'a', 'datasetId': TEST_DS, 'tableId': TEST_TABLE}
        }
        table_desc = TableDesc(in_dict=table_dict)
        self.db.put_table_desc(TEST_DS, TEST_TABLE, table_desc)
        ret = self.db.get_table_desc(TEST_DS, TEST_TABLE)
        self.assertIsInstance(ret, TableDesc)
        self.assertEqual(rand_desc, ret.field_list[0].description)

    @ignore_warnings
    def test_dataset_put_get(self):
        rand_desc = "{0}".format(datetime.datetime.now())
        dataset_desc = DatasetDesc(
            in_dict={'description': rand_desc, 'datasetReference': {'projectId': 'a', 'datasetId': TEST_DS}})
        self.db.put_dataset_desc(TEST_DS, dataset_desc)
        ret = self.db.get_dataset_desc(TEST_DS)
        self.assertIsInstance(ret, DatasetDesc)
        self.assertEqual(rand_desc, ret.description)

    @ignore_warnings
    def test_snapshot_for_table_desc(self):
        TR = {'projectId': 'a', 'datasetId': TEST_DS, 'tableId': TEST_TABLE}
        # insert data
        rand_desc1 = "{0}".format(datetime.datetime.now())
        table_dict1 = {'description': rand_desc1, 'schema': {'fields': []}, 'tableReference': TR}
        table_desc1 = TableDesc(in_dict=table_dict1)
        self.db.put_table_desc(TEST_DS, TEST_TABLE, table_desc1)
        # check
        self.assertEqual(rand_desc1, self.db.get_table_desc(TEST_DS, TEST_TABLE).description)
        # take snapshot
        ymd = self.db.make_db_snapshot()
        # insert again
        rand_desc2 = "{0}".format(datetime.datetime.now())
        table_dict2 = {'description': rand_desc2, 'schema': {'fields': []}, 'tableReference': TR}
        table_desc2 = TableDesc(in_dict=table_dict2)
        self.db.put_table_desc(TEST_DS, TEST_TABLE, table_desc2)
        # check
        self.assertEqual(rand_desc2, self.db.get_table_desc(TEST_DS, TEST_TABLE).description)
        # restore from snapshot
        self.db.recover_table_from_snapshot(TEST_DS, TEST_TABLE, ymd)
        # check
        self.assertEqual(rand_desc1, self.db.get_table_desc(TEST_DS, TEST_TABLE).description)

    @ignore_warnings
    def test_snapshot_for_dataset_desc(self):
        DR = {'projectId': 'a', 'datasetId': TEST_DS}
        # insert data
        rand_desc1 = "{0}".format(datetime.datetime.now())
        dataset_dict1 = {'description': rand_desc1, 'datasetReference': DR}
        dataset_desc1 = DatasetDesc(in_dict=dataset_dict1)
        self.db.put_dataset_desc(TEST_DS, dataset_desc1)
        # check
        self.assertEqual(rand_desc1, self.db.get_dataset_desc(TEST_DS).description)
        # take snapshot
        ymd = self.db.make_db_snapshot()
        # insert again
        rand_desc2 = "{0}".format(datetime.datetime.now())
        dataset_dict2 = {'description': rand_desc2, 'datasetReference': DR}
        dataset_desc2 = DatasetDesc(in_dict=dataset_dict2)
        self.db.put_dataset_desc(TEST_DS, dataset_desc2)
        # check
        self.assertEqual(rand_desc2, self.db.get_dataset_desc(TEST_DS).description)
        # restore from snapshot
        self.db.recover_dataset_from_snapshot(TEST_DS, ymd)
        # check
        self.assertEqual(rand_desc1, self.db.get_dataset_desc(TEST_DS).description)

    @ignore_warnings
    def test_list_snapshot(self):
        self.db.list_db_snapshot()


if __name__ == '__main__':
    unittest.main(warnings='ignore')
