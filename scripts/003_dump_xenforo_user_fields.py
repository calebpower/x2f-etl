from common import dump, serialize_blob


def mutate_user_field(row, cursor):
    field_id = row["field_id"].decode("utf-8")
    row["field_id"] = field_id
    row["field_choices"] = serialize_blob(row["field_choices"])
    return field_id


if __name__ == "__main__":
    dump("SELECT * FROM xf_user_field", "data/raw/user_fields", mutate_user_field)
