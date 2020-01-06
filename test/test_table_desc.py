import unittest

from lib.table_desc import TableDesc


class TestTableDesc(unittest.TestCase):

    def test_init(self):
        table_dict = {
            'description': 'table desc',
            'schema': {'fields': [
                {'name': 'col1', 'type': 'STRING', 'description': 'col1 desc'},
                {'name': 'col2', 'type': 'STRING', 'description': 'col2 desc'}
            ]},
            'tableReference': {"projectId": "a", "datasetId": "b", "tableId": "c"}
        }
        table_desc = TableDesc(table_dict)
        self.assertFalse(table_desc.is_no_description())
        self.assertEqual('table desc', table_desc.description)
        self.assertEqual('col1 desc', table_desc.field_list[0].description)
        self.assertEqual('a', table_desc.project_id)
        self.assertEqual('b', table_desc.dataset_id)
        self.assertEqual('c', table_desc.table_id)
        self.assertEqual(table_dict, table_desc.to_dict())

    def test_init_with_no_description1(self):
        table_dict = {
            'description': '',
            'schema': {'fields': []},
            'tableReference': {"projectId": "a", "datasetId": "b", "tableId": "c"}
        }
        table_desc = TableDesc(table_dict)
        self.assertTrue(table_desc.is_no_description())
        self.assertEqual('', table_desc.description)
        self.assertEqual(0, len(table_desc.field_list))

    def test_init_with_no_description2(self):
        table_dict = {
            'tableReference': {"projectId": "a", "datasetId": "b", "tableId": "c"}
        }
        table_desc = TableDesc(table_dict)
        self.assertTrue(table_desc.is_no_description())
        self.assertEqual('', table_desc.description)
        self.assertEqual(0, len(table_desc.field_list))

    def test_field_name_list(self):
        table_dict = {
            'description': 'table desc',
            'schema': {'fields': [
                {'name': 'col1', 'type': 'STRING', 'description': 'col1 desc'},
                {'name': 'col2', 'type': 'STRING', 'description': 'col2 desc'}
            ]},
            'tableReference': {"projectId": "a", "datasetId": "b", "tableId": "c"}
        }
        table_desc = TableDesc(table_dict)
        filed_name_list = table_desc.field_name_list()
        self.assertEqual('col1', filed_name_list[0])
        self.assertEqual('col2', filed_name_list[1])

    def test_update_description(self):
        table_dict = {'description': 'table desc 1',
                      'schema': {'fields': [
                          {'name': 'col1', 'type': 'STRING', 'description': 'col1 desc 1'},
                          {'name': 'col2', 'type': 'STRING', 'description': 'col2 desc 1'},
                          {'name': 'col3', 'type': 'STRING', 'description': 'col3 desc 1'}
                      ]},
                      'tableReference': {"projectId": "a", "datasetId": "b", "tableId": "c"}
                      }
        table_desc = TableDesc(table_dict)
        table_dict_2 = {'description': 'table desc 2',
                        'schema': {'fields': [
                            {'name': 'col1', 'type': 'STRING', 'description': 'col1 desc 2'},
                            {'name': 'col2', 'type': 'STRING', 'description': 'col2 desc 2'},
                            {'name': 'col9', 'type': 'STRING', 'description': 'col9 desc 2'}
                        ]},
                        'tableReference': {"projectId": "a", "datasetId": "b", "tableId": "c"}
                        }
        table_desc_2 = TableDesc(table_dict_2)

        table_desc.update_description(table_desc_2)
        self.assertEqual('table desc 2', table_desc.description)
        self.assertEqual('col1 desc 2', table_desc.field_list[0].description)
        self.assertEqual('col2 desc 2', table_desc.field_list[1].description)
        self.assertEqual('col3 desc 1', table_desc.field_list[2].description)

    # table descriptipnが指定されていない場合は "" が書かれているのと同義
    def test_update_description_only_table(self):
        table_dict = {'description': 'table desc 1',
                      'schema': {'fields': [
                          {'name': 'col1', 'type': 'STRING', 'description': 'col1 desc 1'}
                      ]},
                      'tableReference': {"projectId": "a", "datasetId": "b", "tableId": "c"}
                      }
        table_desc = TableDesc(table_dict)
        table_dict_2 = {'schema': {'fields': [
            {'name': 'col1', 'type': 'STRING', 'description': 'col1 desc 1'}
        ]},
            'tableReference': {"projectId": "a", "datasetId": "b", "tableId": "c"}

        }
        table_desc_2 = TableDesc(table_dict_2)

        table_desc.update_description(table_desc_2)
        self.assertEqual('', table_desc.description)
        self.assertEqual('col1 desc 1', table_desc.field_list[0].description)


if __name__ == '__main__':
    unittest.main(warnings='ignore')
