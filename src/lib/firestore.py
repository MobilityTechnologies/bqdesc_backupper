import os
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from src.lib.dataset_desc import DatasetDesc
from src.lib.table_desc import TableDesc


class Firestore(object):
    def __init__(self, config, logger):
        self.logger = logger
        self.project = config.gcp_project
        self.table_desc_col = config.firestore_table_desc_collection_name
        self.dataset_desc_col = config.firesotre_dataset_desc_collection_name
        os.environ["GOOGLE_CLOUD_PROJECT"] = self.project
        if config.gcp_use_key_json:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.gcp_key_json

        cred = credentials.ApplicationDefault()
        if firebase_admin._DEFAULT_APP_NAME not in firebase_admin._apps:
            # Avoid double initialize
            firebase_admin.initialize_app(cred, {
                'projectId': self.project,
            })
        self.firestore_client = firestore.client()

    # ------------------
    # Table
    # ------------------

    def put_table_desc(self, dataset_id, table_id, table_desc: TableDesc):
        doc_ref = self.firestore_client.collection(self.table_desc_col).document(f"{dataset_id}.{table_id}")
        dic = table_desc.to_dict()
        # to make timezone aware datetime instance, pass timzeone.utc
        dic["created_at"] = datetime.now(timezone.utc)
        doc_ref.set(dic)

    def get_table_desc(self, dataset_id, table_id) -> TableDesc:
        doc_ref = self.firestore_client.collection(self.table_desc_col).document(f"{dataset_id}.{table_id}")
        doc_snp = doc_ref.get()
        if doc_snp.exists:
            return TableDesc(in_dict=doc_snp.to_dict())
        else:
            raise Exception(f"Does not exist. collection = {self.dataset_desc_col} document_id = {dataset_id}.{table_id}")

    def get_all_table_desc_list(self) -> [TableDesc]:
        doc_ref = self.firestore_client.collection(self.table_desc_col)
        return [TableDesc(in_dict=u.to_dict()) for u in doc_ref.get()]

    # ------------------
    # Dataset
    # ------------------

    def put_dataset_desc(self, dataset_id, dataset_desc: DatasetDesc):
        doc_ref = self.firestore_client.collection(self.dataset_desc_col).document(f"{dataset_id}")
        dic = dataset_desc.to_dict()
        dic["created_at"] = datetime.now(timezone.utc)
        doc_ref.set(dic)

    def get_dataset_desc(self, dataset_id) -> DatasetDesc:
        doc_ref = self.firestore_client.collection(self.dataset_desc_col).document(f"{dataset_id}")
        doc_snp = doc_ref.get()
        if doc_snp.exists:
            return DatasetDesc(in_dict=doc_snp.to_dict())
        else:
            raise Exception(f"Does not exist. collection = {self.dataset_desc_col} document_id = {dataset_id}")

    def get_all_dataset_desc_list(self) -> [DatasetDesc]:
        doc_ref = self.firestore_client.collection(self.dataset_desc_col)
        return [DatasetDesc(in_dict=u.to_dict()) for u in doc_ref.get()]

    # ------------------
    # DB SnapShot
    # ------------------

    def _copy_collection(self, src_col, dst_col):
        doc_ref = self.firestore_client.collection(src_col)
        src_list = doc_ref.get()
        for src_doc_snapshot in src_list:
            doc_ref = self.firestore_client.collection(dst_col).document(src_doc_snapshot.id)
            doc_ref.set(src_doc_snapshot.to_dict())

    def _copy_doc(self, src_col, dst_col, document_id):
        src_doc_ref = self.firestore_client.collection(src_col).document(document_id)
        src_doc = src_doc_ref.get()._data
        if src_doc is None:
            raise Exception(f"FireStore collection={src_col} document_id={document_id} is not found")
        else:
            dst_doc_ref = self.firestore_client.collection(dst_col).document(document_id)
            dst_doc_ref.set(src_doc)

    def make_db_snapshot(self):
        self.logger.info("Make FileStore snapshot collection")
        ymd = datetime.now().strftime("%Y%m%d")
        for src_col, dst_col in [(self.table_desc_col, self.table_desc_col + "-" + ymd), (self.dataset_desc_col, self.dataset_desc_col + "-" + ymd)]:
            self.logger.info(f"copy {src_col} -> {dst_col}")
            self._copy_collection(src_col=src_col, dst_col=dst_col)
        return ymd

    def list_db_snapshot(self):
        snapshot_list = []
        snapshot_prefix = self.table_desc_col + "-"
        for col in self.firestore_client.collections():
            if col.id.find(snapshot_prefix) > -1:
                snapshot_list.append(col.id.replace(snapshot_prefix, ""))
        return snapshot_list

    def recover_table_from_snapshot(self, dataset_id, table_id, snap_shot_ymd):
        """
        Copy table description in snapshot collection to production collection
        """
        self.logger.info(f"Recover table data on FireStore from snapshot. table={dataset_id}.{table_id}, snapshot_id={snap_shot_ymd}")
        src_col = self.table_desc_col + "-" + snap_shot_ymd
        dst_col = self.table_desc_col
        self.logger.info(f"copy {src_col}:{dataset_id}.{table_id} -> {dst_col}:{dataset_id}.{table_id} ")
        self._copy_doc(src_col=src_col, dst_col=dst_col, document_id=f"{dataset_id}.{table_id}")

    def recover_dataset_from_snapshot(self, dataset_id, snap_shot_ymd):
        """
        Copy dataset description in snapshot collection to production collection
        """
        self.logger.info(f"Recover dataset data on FireStore from snapshot. dataset={dataset_id}, snapshot_id={snap_shot_ymd}")
        src_col = self.dataset_desc_col + "-" + snap_shot_ymd
        dst_col = self.dataset_desc_col
        self.logger.info(f"copy {src_col}:{dataset_id} -> {dst_col}:{dataset_id} ")
        self._copy_doc(src_col=src_col, dst_col=dst_col, document_id=f"{dataset_id}")
