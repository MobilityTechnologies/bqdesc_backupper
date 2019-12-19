from bigquery import Bigquery, BqUpdateResult, ResultType
from dataset_desc import DatasetDesc
from firestore import Firestore
from table_desc import TableDesc


class Controller:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.bigquery = Bigquery(config=config, logger=logger)
        self.firestore = Firestore(config=config, logger=logger)

    # -----------------------------------------
    # Backup
    # -----------------------------------------

    def backup_table(self, dataset_id, table_id) -> bool:
        self.logger.info(
            f'Backup BigQuery table "{dataset_id}.{table_id}" description to FireStore "{self.firestore.table_desc_col}:{dataset_id}.{table_id}"')
        table_desc: TableDesc = self.bigquery.get_table_desc(dataset_id=dataset_id, table_id=table_id)
        if (table_desc.is_no_description()):
            self.logger.info("table has no description. do nothing")
            return False
        else:
            self.firestore.put_table_desc(dataset_id=dataset_id, table_id=table_id, table_desc=table_desc)
            self.logger.info("ok")
            return True

    def backup_dataset(self, dataset_id) -> bool:
        self.logger.info(
            f'Backup BigQuery dataset "{dataset_id}" description to FireStore "{self.firestore.dataset_desc_col}:{dataset_id}"')
        dataset_desc: DatasetDesc = self.bigquery.get_dataset_desc(dataset_id=dataset_id)
        if (dataset_desc.is_no_description()):
            self.logger.info("dataset has no description. do nothing")
            return False
        else:
            self.firestore.put_dataset_desc(dataset_id=dataset_id, dataset_desc=dataset_desc)
            self.logger.info("ok")
            return True

    def backup_all(self) -> bool:
        self.logger.info(f'[BACKUP] Backup BigQuery all descriptions to FireStore "{self.firestore.dataset_desc_col}"')
        result_type_counter = {"ok": 0, "skip": 0, "exception": 0}
        for dataset_id in self.bigquery.list_dataset_id(
                include_pattern=self.config.dataset_include_pattern,
                exclude_pattern=self.config.dataset_exclude_pattern):
            # backup dataset description
            try:
                dataset_desc: DatasetDesc = self.bigquery.get_dataset_desc(dataset_id=dataset_id)
                if (dataset_desc.is_no_description()):
                    self.logger.info(f"[BACKUP] [D] [skip] [{dataset_id}] dataset has no description.")
                    result_type_counter["skip"] += 1
                else:
                    self.firestore.put_dataset_desc(dataset_id=dataset_id, dataset_desc=dataset_desc)
                    self.logger.info(f"[BACKUP] [D] [ok] [{dataset_id}]")
                    result_type_counter["ok"] += 1
            except Exception as e:
                self.logger.exception(e)
                result_type_counter["exception"] += 1
            # backup table description
            for table_id in self.bigquery.list_table_id(
                    dataset_id,
                    include_pattern=self.config.table_include_pattern,
                    exclude_pattern=self.config.table_exclude_pattern):
                try:
                    table_desc: TableDesc = self.bigquery.get_table_desc(dataset_id=dataset_id, table_id=table_id)
                    if (table_desc.is_no_description()):
                        self.logger.info(f"[BACKUP] [T] [skip] [{dataset_id}.{table_id}] table has no description.")
                        result_type_counter["skip"] += 1
                    else:
                        self.firestore.put_table_desc(dataset_id=dataset_id, table_id=table_id, table_desc=table_desc)
                        self.logger.info(f"[BACKUP] [T] [ok] [{dataset_id}.{table_id}]")
                        result_type_counter["ok"] += 1
                except Exception as e:
                    self.logger.exception(e)
                    result_type_counter["exception"] += 1
        if result_type_counter["exception"] > 0:
            self.logger.error(f"[BACKUP] Finish with some errors. result={result_type_counter}")
            return False
        else:
            self.logger.info(f"[BACKUP] Finish with no error.  result={result_type_counter}")
            return True

    # -----------------------------------------
    # RESTORE
    # -----------------------------------------

    def restore_table(self, dataset_id, table_id):
        self.logger.info(
            f'Restore FireStore "{self.firestore.table_desc_col}:{dataset_id}.{table_id}" to BigQuery table "{dataset_id}.{table_id}"')
        table_desc: TableDesc = self.firestore.get_table_desc(dataset_id, table_id)
        bq_update_result: BqUpdateResult = self.bigquery.update_table_desc(new_table_desc=table_desc)
        self.logger.info(f'{bq_update_result.type.value}. {bq_update_result.detail}')
        return bq_update_result

    def restore_dataset(self, dataset_id):
        self.logger.info(
            f'Restore FireStore "{self.firestore.table_desc_col}:{dataset_id}" to BigQuery dataset "{dataset_id}"')
        dataset_desc: DatasetDesc = self.firestore.get_dataset_desc(dataset_id)
        bq_update_result = self.bigquery.update_dataset_desc(dataset_desc=dataset_desc)
        self.logger.info(f"{bq_update_result.type.value}. {bq_update_result.detail}")
        return bq_update_result

    def restore_all(self) -> dict:
        self.logger.info(
            f"[RESTORE] Restore FireStore ({self.firestore.table_desc_col}) to BigQuery Table and FireStore ({self.firestore.dataset_desc_col}) to BigQuery Datasets")
        result_type_counter = {"exception": 0}
        for _, result_type in ResultType.__members__.items():
            result_type_counter[result_type.value] = 0

        error_count = 0
        for dataset_desc in self.firestore.get_all_dataset_desc_list():
            try:
                bq_update_result: BqUpdateResult = self.bigquery.update_dataset_desc(dataset_desc=dataset_desc)
                msg = f"[RESTORE] [D] [{bq_update_result.type.value}] [{dataset_desc.dataset_id}] {bq_update_result.detail}"
                result_type_counter[bq_update_result.type.value] += 1
                if bq_update_result.is_success:
                    self.logger.info(msg)
                else:
                    error_count += 1
                    self.logger.warning(msg)
            except Exception as e:
                self.logger.exception(e)
                result_type_counter["exception"] += 1
                error_count += 1

        for table_desc in self.firestore.get_all_table_desc_list():
            try:
                bq_update_result: BqUpdateResult = self.bigquery.update_table_desc(new_table_desc=table_desc)
                msg = f"[RESTORE] [D] [{bq_update_result.type.value}] [{table_desc.dataset_id}.{table_desc.table_id}] {bq_update_result.detail}"
                result_type_counter[bq_update_result.type.value] += 1
                if bq_update_result.is_success:
                    self.logger.info(msg)
                else:
                    self.logger.warning(msg)
                    error_count += 1
            except Exception as e:
                self.logger.exception(e)
                result_type_counter["exception"] += 1
                error_count += 1
        if error_count > 0:
            self.logger.error(f"[RESTORE] Finish with some errors. Result = {result_type_counter}")
        else:
            self.logger.info(f"[RESTORE] Finish with no error. Result = {result_type_counter}")
        return result_type_counter
