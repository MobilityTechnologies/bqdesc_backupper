class DatasetDesc(object):
    def __init__(self, in_dict):
        self.project_id = in_dict["datasetReference"]["projectId"]
        self.dataset_id = in_dict["datasetReference"]["datasetId"]
        self.description = in_dict.get("description", "")

    def is_no_description(self):
        return self.description == ""

    def to_dict(self):
        return {
            "description": self.description,
            "datasetReference": {
                "projectId": self.project_id,
                "datasetId": self.dataset_id
            }
        }
