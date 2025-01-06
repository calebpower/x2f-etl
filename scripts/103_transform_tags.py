import dbm
import json
import os


def resolve_compound_name(node_id):
    node_type = (
        "forums" if os.path.exists(f"data/raw/forums/{node_id}.json") else "categories"
    )
    with open(
        f"data/raw/{node_type}/{node_id}.json", "r", encoding="utf-8"
    ) as node_file:
        node_json = json.load(node_file)
        return (
            node_json["node_id"],
            (
                (resolve_compound_name(node_json["parent_node_id"])[1] + ": ")
                if "parent_node_id" in node_json
                else ""
            )
            + node_json["title"],
        )


if __name__ == "__main__":
    os.makedirs("data/transform", exist_ok=True)
    with dbm.open("data/transform/tags.map", "n") as db:
        entries = os.listdir("data/raw/forums")
        for entry in entries:
            tag = resolve_compound_name(os.path.splitext(entry)[0])
            db[str(tag[0])] = tag[1]

    with dbm.open(f"data/transform/tags.map", "r") as db:
        for key in db.keys():
            key_decoded = key.decode("utf-8")
            value_decoded = db[key].decode("utf-8")
            print(f"{key_decoded} -> {value_decoded}")
        
