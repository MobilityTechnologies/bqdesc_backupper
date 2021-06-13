import datetime
import unittest

from init import config, logger

from lib.dataset_desc import DatasetDesc
from lib.localfile import LocalFile
from lib.table_desc import TableDesc

TEST_DS = "test_bqdesc_buckuper"
TEST_TABLE = "update_test"


class TestLocalFile(unittest.TestCase):
    def setUp(self):
        self.localfile = LocalFile(config, logger)

    def test_table_put_get(self):
        rand_desc = f"{datetime.datetime.now()}"
        table_dict = {
            'description': rand_desc,
            'schema': {'fields': [{'name': 'col1', 'type': 'STRING', 'description': rand_desc}]},
            'tableReference': {'projectId': 'a', 'datasetId': TEST_DS, 'tableId': TEST_TABLE}
        }
        table_desc = TableDesc(in_dict=table_dict)
        self.localfile.put_table_desc(TEST_DS, TEST_TABLE, table_desc)
        ret = self.localfile.get_table_desc(TEST_DS, TEST_TABLE)
        self.assertEqual(rand_desc, ret.field_list[0].description)

    def test_dataset_put_get(self):
        rand_desc = f"{datetime.datetime.now()}"
        dataset_desc = DatasetDesc(
            in_dict={'description': rand_desc, 'datasetReference': {'projectId': 'a', 'datasetId': TEST_DS}})
        self.localfile.put_dataset_desc(TEST_DS, dataset_desc)
        ret = self.localfile.get_dataset_desc(TEST_DS)
        self.assertEqual(rand_desc, ret.description)


if __name__ == '__main__':
    unittest.main(warnings='ignore')
