import unittest

from lib.dataset_desc import DatasetDesc


class TestDatasetDesc(unittest.TestCase):

    def test_init(self):
        dataset_dict = {
            'description': 'dataset desc',
            'datasetReference': {"projectId": "a", "datasetId": "b"}
        }
        dataset_desc = DatasetDesc(dataset_dict)
        self.assertFalse(dataset_desc.is_no_description())
        self.assertEqual('dataset desc', dataset_desc.description)
        self.assertEqual('a', dataset_desc.project_id)
        self.assertEqual('b', dataset_desc.dataset_id)
        self.assertEqual(dataset_dict, dataset_desc.to_dict())

    def test_init_with_no_description1(self):
        dataset_dict = {
            'description': '',
            'datasetReference': {"projectId": "a", "datasetId": "b"}
        }
        dataset_desc = DatasetDesc(dataset_dict)
        self.assertTrue(dataset_desc.is_no_description())
        self.assertEqual('', dataset_desc.description)

    def test_init_with_no_description2(self):
        dataset_dict = {
            'datasetReference': {"projectId": "a", "datasetId": "b"}
        }
        dataset_dict = DatasetDesc(dataset_dict)
        self.assertTrue(dataset_dict.is_no_description())
        self.assertEqual('', dataset_dict.description)


if __name__ == '__main__':
    unittest.main(warnings='ignore')
