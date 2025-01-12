from common import dump


def mutate_category(row, cursor):
    node_id = row["node_id"]
    return node_id


if __name__ == "__main__":
    dump(
        "SELECT node_id, title FROM xf_node WHERE node_type_id = 'Category'",
        "data/raw/categories",
        mutate_category,
    )
