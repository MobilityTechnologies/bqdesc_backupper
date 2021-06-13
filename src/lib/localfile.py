from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Union

from lib.storage import Storage
from lib.dataset_desc import DatasetDesc
from lib.table_desc import TableDesc


class LocalFile(Storage):
    """
    LocalFile stores description of table and dataset as json file on `self.localfile_dir`.
    A directory structure is as follows:
    ```
    your-gcp-project-id
    ├── dataset-id
    │   └── table_id.json
    └── dataset-id.json
    ```
    """

    def __init__(self, config, logger):
        super().__init__()
        self.logger = logger
        self.project = config.gcp_project
        self.localfile_dir: Path = Path(config.localfile_dir)

    def put_table_desc(self, dataset_id, table_id, table_desc):
        dic = table_desc.to_dict()
        dic["created_at"] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        target_dir = self.localfile_dir / self.project / dataset_id
        _prepare_dir(target_dir)
        file_name = target_dir / f"{table_id}.json"
        with file_name.open(mode="w") as outfile:
            outfile.write(json.dumps(dic, indent=2, ensure_ascii=False))

    def get_table_desc(self, dataset_id, table_id) -> TableDesc:
        target_file = self.localfile_dir / self.project / dataset_id / f"{table_id}.json"
        if target_file.exists():
            return self._file_to_desc(target_file)
        raise Exception(f"Description file for {self.projct}:{dataset_id}.{table_id} does not exist.")

    def get_all_table_desc_list(self) -> [TableDesc]:
        return [self._file_to_desc(f) for f in self.localfile_dir.iterdir() if f.is_file() and _is_table_desc_file(f.name)]

    def put_dataset_desc(self, dataset_id, dataset_desc):
        dic = dataset_desc.to_dict()
        dic["created_at"] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        target_dir = self.localfile_dir / self.project
        _prepare_dir(target_dir)
        file_name = target_dir / f"{dataset_id}.json"
        with file_name.open(mode="w") as outfile:
            outfile.write(json.dumps(dic, indent=2, ensure_ascii=False))

    def get_dataset_desc(self, dataset_id) -> DatasetDesc:
        target_file = self.localfile_dir / self.project / f"{dataset_id}.json"
        if target_file.exists():
            return self._file_to_desc(target_file)
        raise Exception(f"Description file for {self.project}:{dataset_id} does not exist.")

    def get_all_dataset_desc_list(self) -> [DatasetDesc]:
        return [self._file_to_desc(f) for f in self.localfile_dir.iterdir() if f.is_file() and not _is_table_desc_file(f.name)]

    @staticmethod
    def _file_to_desc(f: Path) -> Union[TableDesc, DatasetDesc]:
        with f.open() as desc:
            dic = json.load(desc)
            if _is_table_desc_file(dic):
                return TableDesc(dic)
            else:
                return DatasetDesc(dic)


def _prepare_dir(target_dir) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)


def _is_table_desc_file(dic: dict) -> bool:
    return 'tableReference' in dic.keys()
