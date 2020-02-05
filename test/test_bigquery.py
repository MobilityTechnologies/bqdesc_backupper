import datetime
import unittest

from init import config, logger, ignore_warnings

from lib.bigquery import Bigquery, ResultType
from lib.dataset_desc import DatasetDesc
from lib.table_desc import TableDesc

GCP_PROJECT_ID = config.gcp_project
TEST_DS = "test_bqdesc_buckuper"
TEST_TABLE = "update_test"
TEST_COL1 = "col1"
TEST_COL2 = "col2"

class TestBigquery(unittest.TestCase):
    def setUp(self):
        self.bq = Bigquery(config, logger)
        self.table_reference = {
            "projectId": GCP_PROJECT_ID,
            "datasetId": TEST_DS,
            "tableId": TEST_TABLE
        }
        self.dataset_reference = {
            "projectId": GCP_PROJECT_ID,
            "datasetId": TEST_DS
        }

    # -----------------------
    # Table
    # -----------------------
    @ignore_warnings
    def test_get_table__both_table_and_filed_have_desc(self):
        table_name = "both_table_and_filed_have_desc"
        table_desc: TableDesc = self.bq.get_table_desc(TEST_DS, table_name)
        self.assertEqual("this is table desc", table_desc.description)
        self.assertEqual(1, len(table_desc.field_list))
        self.assertEqual("this is col1 desc", table_desc.field_list[0].description)
        self.assertEqual("col1", table_desc.field_list[0].name)
        self.assertEqual(table_name, table_desc.table_id)
        self.assertEqual(TEST_DS, table_desc.dataset_id)
        self.assertEqual(GCP_PROJECT_ID, table_desc.project_id)

    @ignore_warnings
    def test_get_table__col_only_have_desc(self):
        table_desc: TableDesc = self.bq.get_table_desc(TEST_DS, "col_only_have_desc")
        self.assertEqual("", table_desc.description)
        self.assertEqual(1, len(table_desc.field_list))
        self.assertEqual("this is col1 desc", table_desc.field_list[0].description)

    @ignore_warnings
    def test_get_table__table_only_have_desc(self):
        table_desc: TableDesc = self.bq.get_table_desc(TEST_DS, "table_only_have_desc")
        self.assertEqual("this is table desc", table_desc.description)
        self.assertEqual(1, len(table_desc.field_list))
        self.assertEqual("", table_desc.field_list[0].description)

    @ignore_warnings
    def test_get_table__neither_table_nor_filed_have_desc(self):
        table_desc: TableDesc = self.bq.get_table_desc(TEST_DS, "neither_table_nor_filed_have_desc")
        self.assertEqual("", table_desc.description)
        self.assertEqual(1, len(table_desc.field_list))
        self.assertEqual("", table_desc.field_list[0].description)

    @ignore_warnings
    def test_get_table__part_of_cols_have_desc(self):
        table_desc: TableDesc = self.bq.get_table_desc(TEST_DS, "part_of_cols_have_desc")
        self.assertEqual(2, len(table_desc.field_list))
        self.assertEqual("this is col1 desc", table_desc.field_list[0].description)
        self.assertEqual("", table_desc.field_list[1].description)

    @ignore_warnings
    def test_update_table_desc(self):
        """
             in     BQ     BQ
        c1: "d"    "a"    "d"
        c2: "e"    "b" -> "e"
        c3: "f"    "c"    "f"
        """
        rand = "{0}".format(datetime.datetime.now())
        new_table_dict = {
            'description': 'new table description' + rand,
            'schema': {
                'fields': [
                    {'name': 'col1', 'type': 'STRING', 'description': 'new col1 description' + rand},
                    {'name': 'col2', 'type': 'STRING', 'description': 'new col2 description' + rand}]},
            'tableReference': self.table_reference,
        }
        table_desc = TableDesc(in_dict=new_table_dict)
        bq_update_result = self.bq.update_table_desc(table_desc)
        self.assertEqual(ResultType.UPDATE, bq_update_result.type)
        ret_table_desc = self.bq.get_table_desc(TEST_DS, TEST_TABLE)
        self.assertEqual('new table description' + rand, ret_table_desc.description)
        self.assertEqual('new col1 description' + rand, ret_table_desc.field_list[0].description)
        self.assertEqual('new col2 description' + rand, ret_table_desc.field_list[1].description)

    @ignore_warnings
    def test_update_table_desc__one_description_is_empty_string(self):
        """
             in     BQ     BQ
        c1: "d"    "a"    "d"
        c2: "e"    "b" -> "e"
        c3: ""     "c"    ""
        """
        rand = "{0}".format(datetime.datetime.now())
        new_table_dict = {
            'description': 'new table description' + rand,
            'schema': {
                'fields': [
                    {'name': 'col1', 'type': 'STRING', 'description': 'new col1 description' + rand},
                    {'name': 'col2', 'type': 'STRING', 'description': ''}
                ]},
            'tableReference': self.table_reference,
        }
        table_desc = TableDesc(in_dict=new_table_dict)
        bq_update_result = self.bq.update_table_desc(table_desc)
        self.assertEqual(ResultType.UPDATE, bq_update_result.type)
        ret_table_desc = self.bq.get_table_desc(TEST_DS, TEST_TABLE)
        self.assertEqual('new table description' + rand, ret_table_desc.description)
        self.assertEqual('new col1 description' + rand, ret_table_desc.field_list[0].description)
        self.assertEqual('', ret_table_desc.field_list[1].description)

    @ignore_warnings
    def test_update_table_desc__filed_was_added(self):
        """
             in     BQ     BQ
        c1: "d"    "a"    "d"
        c2: "e"    "b" -> "e"
        c3: "f"    "c"    "f"
        c4         "g"    "g"
        """
        rand = "{0}".format(datetime.datetime.now())
        new_table_dict = {
            'description': 'new table description' + rand,
            'schema': {
                'fields': [
                    {'name': 'col1', 'type': 'STRING', 'description': 'new col1 description' + rand}
                ]},
            'tableReference': self.table_reference,
        }
        table_desc = TableDesc(in_dict=new_table_dict)
        bq_update_result = self.bq.update_table_desc(table_desc)
        self.assertEqual(ResultType.UPDATE, bq_update_result.type)
        ret_table_desc = self.bq.get_table_desc(TEST_DS, TEST_TABLE)
        self.assertEqual('new table description' + rand, ret_table_desc.description)
        self.assertEqual('new col1 description' + rand, ret_table_desc.field_list[0].description)
        # update only existing fields.
        self.assertNotEqual('new col2 description' + rand, ret_table_desc.field_list[1].description)

    # field was removed
    @ignore_warnings
    def test_update_table_desc__field_was_removed(self):
        """
             in     BQ     BQ
        c1: "d"    "a"    "d"
        c2: "e"    "b" -> "e"
        c3: "f"    "c"    "f"
        c4  "g"
        """
        self.bq.get_table_desc(TEST_DS, TEST_TABLE)
        rand = "{0}".format(datetime.datetime.now())
        new_table_dict = {
            'description': 'new table description' + rand,
            'schema': {
                'fields': [
                    {'name': 'col1', 'type': 'STRING', 'description': 'new col1 description' + rand},
                    {'name': 'col2', 'type': 'STRING', 'description': 'new col2 description' + rand},
                    {'name': 'col3', 'type': 'STRING', 'description': 'new col3 description' + rand},
                ]},
            'tableReference': self.table_reference,
        }
        table_desc = TableDesc(in_dict=new_table_dict)
        bq_update_result = self.bq.update_table_desc(table_desc)
        self.assertEqual(ResultType.UPDATE, bq_update_result.type)
        ret_table_desc = self.bq.get_table_desc(TEST_DS, TEST_TABLE)
        self.assertEqual('new table description' + rand, ret_table_desc.description)
        self.assertEqual('new col1 description' + rand, ret_table_desc.field_list[0].description)
        self.assertEqual('new col2 description' + rand, ret_table_desc.field_list[1].description)

    @ignore_warnings
    def test_update_table_desc__same(self):
        """
             in     BQ     BQ
        c1: "a"    "a"    "a"
        c2: "b"    "b" -> "b"
        c3: "c"    "c"    "c"
        """
        ret1 = self.bq.get_table_desc(TEST_DS, TEST_TABLE)
        bq_update_result = self.bq.update_table_desc(ret1)
        self.assertEqual(ResultType.SAME, bq_update_result.type)

    @ignore_warnings
    def test_update_table_desc__too_many_deletion_error(self):
        """
             in     BQ     BQ
        c1: ""     "a"
        c2: ""     "b" -> ERROR
        c3: ""     "c"
        """
        new_table_dict = {
            'schema': {
                'fields': [
                    {'name': 'col1', 'type': 'STRING'},
                    {'name': 'col2', 'type': 'STRING'},
                    {'name': 'col3', 'type': 'STRING'}
                ]},
            'tableReference': {"projectId": GCP_PROJECT_ID, "datasetId": TEST_DS,
                               "tableId": "three_filed_descriptions"},
        }
        table_desc = TableDesc(in_dict=new_table_dict)
        bq_update_result = self.bq.update_table_desc(table_desc)
        self.assertEqual(ResultType.TOO_MANY_DELETION, bq_update_result.type)

    @ignore_warnings
    def test_update_table_desc__too_many_deletion_error_2(self):
        """
             in     BQ     BQ
        c1: ""     "a"
        c2: ""     "b" -> ERROR
        c3:        "c"
        """
        new_table_dict = {
            'schema': {
                'fields': [
                    {'name': 'col1', 'type': 'STRING'},
                    {'name': 'col2', 'type': 'STRING'}
                ]},
            'tableReference': {"projectId": GCP_PROJECT_ID, "datasetId": TEST_DS, "tableId": "three_filed_descriptions"}
        }
        table_desc = TableDesc(in_dict=new_table_dict)
        bq_update_result = self.bq.update_table_desc(table_desc)
        self.assertEqual(ResultType.TOO_MANY_DELETION, bq_update_result.type)

    @ignore_warnings
    def test_update_table_desc__with_no_field_list(self):
        """
             in     BQ     BQ
        c1:        "a"     "a"
        c2:        "b" ->  "b"
        c3:        "c"     "c"
        """
        rand = "{0}".format(datetime.datetime.now())
        new_table_dict = {
            'tableReference': {"projectId": GCP_PROJECT_ID, "datasetId": TEST_DS, "tableId": "three_filed_descriptions"}
        }
        table_desc = TableDesc(in_dict=new_table_dict)
        self.bq.update_table_desc(table_desc)

    # 存在しない
    @ignore_warnings
    def test_update_table_not_exist_dataset(self):
        new_table_dict = {'schema': {'fields': []},
                          'tableReference': {"projectId": GCP_PROJECT_ID, "datasetId": "does_not_exist_dataset",
                                             "tableId": TEST_TABLE}
                          }
        table_desc = TableDesc(in_dict=new_table_dict)
        bq_update_result = self.bq.update_table_desc(table_desc)
        self.assertEqual(ResultType.DATASET_NOT_FOUND, bq_update_result.type)

    @ignore_warnings
    def test_update_table_not_exist_dataset(self):
        new_table_dict = {'schema': {'fields': []},
                          'tableReference': {"projectId": GCP_PROJECT_ID, "datasetId": TEST_DS,
                                             "tableId": "does not exist table"}
                          }
        table_desc = TableDesc(in_dict=new_table_dict)
        bq_update_result = self.bq.update_table_desc(table_desc)
        self.assertEqual(ResultType.TABLE_NOT_FOUND, bq_update_result.type)
        self.assertEqual(False,bq_update_result.is_success)

    @ignore_warnings
    def test_update_table_not_exist_dataset_with_ignore_table_not_found_error_when_restore(self):
        org_value = config.ignore_table_not_found_error_when_restore
        try:
            config.ignore_table_not_found_error_when_restore = True
            tmp_bq = Bigquery(config, logger)

            new_table_dict = {'schema': {'fields': []},
                          'tableReference': {"projectId": GCP_PROJECT_ID, "datasetId": TEST_DS,
                                             "tableId": "does not exist table"}
                          }
            table_desc = TableDesc(in_dict=new_table_dict)
            bq_update_result = tmp_bq.update_table_desc(table_desc)
            self.assertEqual(ResultType.TABLE_NOT_FOUND, bq_update_result.type)
            self.assertEqual(True,bq_update_result.is_success)
        finally:
            config.ignore_table_not_found_error_when_restore = org_value

    @ignore_warnings
    def test_list_table_id(self):
        ret = self.bq.list_table_id(TEST_DS)
        self.assertTrue(len(ret) > 0)

    # ----------------------------
    # Dataset
    # ----------------------------

    @ignore_warnings
    def test_list_dataset(self):
        ret = self.bq.list_dataset_id()
        self.assertTrue(len(ret) > 0)

    @ignore_warnings
    def test_get_dataset_desc(self):
        dataset_desc: DatasetDesc = self.bq.get_dataset_desc(TEST_DS)
        self.assertEqual(TEST_DS, dataset_desc.dataset_id)

    @ignore_warnings
    def test_update_dataset_desc(self):
        ymd_str = "{0}".format(datetime.datetime.now())
        dataset_desc = DatasetDesc(in_dict={"description": ymd_str, "datasetReference": self.dataset_reference})
        update_result = self.bq.update_dataset_desc(dataset_desc)
        self.assertTrue(ResultType.UPDATE, update_result.type)
        self.assertEqual(ymd_str, self.bq.get_dataset_desc(TEST_DS).description)

    @ignore_warnings
    def test_update_dataset_desc_twice(self):
        ymd_str = "{0}".format(datetime.datetime.now())
        dataset_desc = DatasetDesc(in_dict={"description": ymd_str, "datasetReference": self.dataset_reference})
        tmp_update_result = self.bq.update_dataset_desc(dataset_desc)
        update_result = self.bq.update_dataset_desc(dataset_desc)
        self.assertEqual(ResultType.SAME, update_result.type)


    @ignore_warnings
    def test_update_dataset_not_exist_dataset(self):
        ymd_str = "{0}".format(datetime.datetime.now())
        dataset_reference = {
            "projectId": GCP_PROJECT_ID,
            "datasetId": "does not exist dataset"
        }
        dataset_desc = DatasetDesc(in_dict={"description": ymd_str, "datasetReference": dataset_reference})
        bq_update_result = self.bq.update_dataset_desc(dataset_desc)
        self.assertEqual(ResultType.DATASET_NOT_FOUND, bq_update_result.type)
        self.assertEqual(False,bq_update_result.is_success)

    @ignore_warnings
    def test_update_dataset_not_exist_dataset_with_ignore_dataset_not_found_error_when_restore(self):
        org_value = config.ignore_dataset_not_found_error_when_restore
        try:
            config.ignore_dataset_not_found_error_when_restore = True
            ymd_str = "{0}".format(datetime.datetime.now())
            dataset_reference = {
                "projectId": GCP_PROJECT_ID,
                "datasetId": "does not exist dataset"
            }
            dataset_desc = DatasetDesc(in_dict={"description": ymd_str, "datasetReference": dataset_reference})
            bq_update_result = self.bq.update_dataset_desc(dataset_desc)
            self.assertEqual(ResultType.DATASET_NOT_FOUND, bq_update_result.type)
            self.assertEqual(True,bq_update_result.is_success)
        finally:
            config.ignore_dataset_not_found_error_when_restore = org_value


    # ----------------------------
    # Project
    # ----------------------------
    @ignore_warnings
    def test_list_project(self):
        ret = self.bq.list_project_id()
        self.assertTrue(len(ret) > 0)


if __name__ == '__main__':
    unittest.main(warnings='ignore')
