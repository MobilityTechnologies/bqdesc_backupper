class TableDesc(object):
    def __init__(self, in_dict):
        self.project_id = in_dict["tableReference"]["projectId"]
        self.dataset_id = in_dict["tableReference"]["datasetId"]
        self.table_id = in_dict["tableReference"]["tableId"]
        self.description = in_dict.get("description", "")
        if "schema" in in_dict and "fields" in in_dict["schema"]:
            self.field_list = [Field(f) for f in in_dict["schema"]["fields"]]
        else:
            self.field_list = []

    def is_no_description(self):
        return self.description == "" and self.num_of_field_desc() == 0

    def field_name_list(self):
        return [f.name for f in self.field_list]

    def to_dict(self):
        return {
            "description": self.description,
            "schema": {
                "fields": [f.to_dict() for f in self.field_list]
            },
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": self.dataset_id,
                "tableId": self.table_id,
            }
        }

    def check_diff(self, other) -> (bool, str):
        """
        check table description difference. And check filed description defference if both exist.
        :param other: TableDesc
        :return: (same or not, difference reason)
        """
        diff_reason_list = []
        other: TableDesc = other
        is_same = True
        if self.description != other.description:
            is_same = False
            diff_reason_list.append(f"table description is different {other.description} != {self.description}.")
        if len(self.field_list) != len(other.field_list):
            is_same = False
            diff_reason_list.append(f"number of fields is different {len(self.field_list)} != {len(other.field_list)}")
        for self_field in self.field_list:
            for other_field in other.field_list:
                if self_field.name == other_field.name:
                    if self_field.description != other_field.description:
                        is_same = False
                        diff_reason_list.append(
                            f"field({self_field.name}) description is defferent {other_field.description} != {self_field.description}.")
        return is_same, ",".join(diff_reason_list)

    def num_of_field_desc(self) -> int:
        sum = 0
        for field in self.field_list:
            if field.description != "":
                sum += 1
        return sum

    def has_fields_description(self, filed_name):
        for field in self.field_list:
            if field.name == filed_name:
                return True
        return False

    def _update_field_description(self, filed_name, description):
        for field in self.field_list:
            if field.name == filed_name:
                field.description = description

    def update_description(self, other):
        """
        引数のTableDescでfiled_descriptionを更新する。列が一致したもの飲みを更新する
        :param other:
        :return:
        """
        self.description = other.description
        for other_field in other.field_list:
            if self.has_fields_description(other_field.name):
                # どちらにも存在する列
                self._update_field_description(other_field.name, other_field.description)


class Field(object):
    def __init__(self, in_dict):
        self.name = in_dict["name"]
        self.description = in_dict.get("description", "")
        self.type = in_dict["type"]

    def to_dict(self):
        return {"name": self.name,
                "type": self.type,
                "description": self.description}
