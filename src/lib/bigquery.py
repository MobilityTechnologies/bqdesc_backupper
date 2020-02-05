from google.api_core.exceptions import NotFound, BadRequest
from google.cloud import bigquery
from enum import Enum
import os
import re

from lib.table_desc import TableDesc
from lib.dataset_desc import DatasetDesc

MATCH_ALL = r'.*'
MATCH_NONE = r'^$'

class ResultType(Enum):
    SAME = "same"
    UPDATE = "update"
    DATASET_NOT_FOUND = "dataset not found"
    TABLE_NOT_FOUND = "table not found"
    TOO_MANY_DELETION = "too many deletion"

class BqUpdateResult(object):
    def __init__(self,is_success,msg:ResultType,detail=""):
        self.is_success = is_success
        self.type = msg
        self.detail = detail


class Bigquery:

    def __init__(self,config,logger):
        self.logger = logger
        self.config = config
        self.project = config.gcp_project
        os.environ["GOOGLE_CLOUD_PROJECT"] = self.project
        if config.gcp_use_key_json:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.gcp_key_json
        self.client = bigquery.Client(project = self.project)

    #-------------------------------
    # Project
    #-------------------------------

    def list_project_id(self, include_pattern=MATCH_ALL, exclude_pattern=MATCH_NONE):
        ret = []
        for proj in self.client.list_projects():
            if re.search(include_pattern, proj.project_id) and not re.search(exclude_pattern, proj.project_id):
                ret.append(proj.project_id)
        return ret

    #-------------------------------
    # Dataset
    #-------------------------------

    def get_dataset_desc(self, dataset_id) -> DatasetDesc:
        dataset_ref = self.client.dataset(dataset_id)
        dataset = self.client.get_dataset(dataset_ref)  # API Request Here
        dataset_dict = dataset.to_api_repr()
        return DatasetDesc(in_dict=dataset_dict)

    def update_dataset_desc(self, dataset_desc:DatasetDesc) -> BqUpdateResult:
        dataset_id = dataset_desc.dataset_id
        try:
            existing_dataset_desc : DatasetDesc = self.get_dataset_desc(dataset_id=dataset_id)
            if existing_dataset_desc.description == dataset_desc.description:
                return BqUpdateResult(True,ResultType.SAME,detail="do nothing")
            else:
                ds = bigquery.Dataset(self.client.dataset(dataset_id))
                ds.description = dataset_desc.description
                self.client.update_dataset(ds, ['description'])
                return BqUpdateResult(True,ResultType.UPDATE,f"{existing_dataset_desc.description} -> {dataset_desc.description}")
        except NotFound as e:
            if self.config.ignore_dataset_not_found_error_when_restore:
                is_success = True
            else:
                is_success = False
            return BqUpdateResult(is_success,ResultType.DATASET_NOT_FOUND)
        except BadRequest as e:
            if e.errors[0]["message"].find("Invalid dataset ID") > -1:
                if self.config.ignore_dataset_not_found_error_when_restore:
                    is_success = True
                else:
                    is_success = False
                return BqUpdateResult(is_success,ResultType.DATASET_NOT_FOUND,detail=str(e))
            raise e

    def list_dataset_id(self, include_pattern=MATCH_ALL, exclude_pattern=MATCH_NONE):
        ret = []
        for dataset in self.client.list_datasets():
            if re.search(include_pattern,dataset.dataset_id) and not re.search(exclude_pattern,dataset.dataset_id):
                ret.append(dataset.dataset_id)
        return ret

    #-------------------------------
    # Table
    #-------------------------------

    def get_table_desc(self, dataset_id, table_id) -> TableDesc:
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = self.client.get_table(table_ref)  # API Request Here
        table_dict = table.to_api_repr()
        return TableDesc(in_dict=table_dict)

    def update_table_desc(self, new_table_desc:TableDesc)->BqUpdateResult:
        try:
            dataset_id = new_table_desc.dataset_id
            table_id = new_table_desc.table_id
            now_table_desc = self.get_table_desc(dataset_id=dataset_id,table_id=table_id)
        except NotFound as e:
            if self.config.ignore_table_not_found_error_when_restore:
                is_success = True
            else:
                is_success = False
            return BqUpdateResult(is_success,ResultType.TABLE_NOT_FOUND,detail=str(e))
        except BadRequest as e:
            if e.errors[0]["message"].find("Invalid table ID") > -1:
                if self.config.ignore_table_not_found_error_when_restore:
                    is_success = True
                else:
                    is_success = False
                return BqUpdateResult(is_success,ResultType.TABLE_NOT_FOUND,detail=str(e))
            raise e
        is_same, diff_msg = new_table_desc.check_diff(now_table_desc)
        if is_same:
            return BqUpdateResult(True,ResultType.SAME,detail="do nothing")
        elif len(now_table_desc.field_list) >= 2 and len(new_table_desc.field_list) >= 2 and \
                now_table_desc.num_of_field_desc() - new_table_desc.num_of_field_desc() >= 2  :
            msg = f"filld description: " + \
                  f"existing={now_table_desc.num_of_field_desc()}/{len(now_table_desc.field_list)} " + \
                  f"new={new_table_desc.num_of_field_desc()}/{len(new_table_desc.field_list)}."
            return BqUpdateResult(False,ResultType.TOO_MANY_DELETION,msg)
        else:
            now_table_desc.update_description(other=new_table_desc)
            generated_dict = now_table_desc.to_dict()
            generated_dict["tableReference"] = {"projectId":self.project,"datasetId":dataset_id,"tableId":table_id}
            new_table = bigquery.table.Table.from_api_repr(generated_dict)
            self.client.update_table(new_table,["description","schema"])
            return BqUpdateResult(True,ResultType.UPDATE,diff_msg)


    def list_table_id(self, dataset_id, include_pattern=MATCH_ALL, exclude_pattern=MATCH_NONE):
        dataset_ref = self.client.dataset(dataset_id)
        tables = list(self.client.list_tables(dataset_ref))
        ret = []
        for table in tables:
            if re.search(include_pattern, table.table_id) and not re.search(exclude_pattern, table.table_id):
                ret.append(table.table_id)
        return ret


