from abc import ABCMeta, abstractclassmethod

from lib.dataset_desc import DatasetDesc
from lib.table_desc import TableDesc


class Storage(metaclass=ABCMeta):
    def __init__(self) -> None:
        pass

    @abstractclassmethod
    def put_table_desc(self, dataset_id, table_id, table_desc: TableDesc):
        pass

    @abstractclassmethod
    def get_table_desc(self, dataset_id, table_id) -> TableDesc:
        pass

    @abstractclassmethod
    def get_all_table_desc_list(self) -> [TableDesc]:
        pass

    @abstractclassmethod
    def put_dataset_desc(self, dataset_id, dataset_desc: DatasetDesc):
        pass

    @abstractclassmethod
    def get_dataset_desc(self, dataset_id) -> DatasetDesc:
        pass

    @abstractclassmethod
    def get_all_dataset_desc_list(self) -> [DatasetDesc]:
        pass
