import dbm
import json
import os

from common import get_offset


def reindex(in_dir, out_file, offset_table=None, sub_entries=None):
    idx_offset = (
        0 if offset_table is None else get_offset(offset_table[0], offset_table[1])
    )
    entries = sorted([int(os.path.splitext(f)[0]) for f in os.listdir(in_dir)])
    os.makedirs(os.path.dirname(out_file), exist_ok=True)

    sub_indexes = {}

    if sub_entries is not None:
        for sub_entry in sub_entries:
            if sub_entry["id"] not in sub_indexes:
                sub_indexes[sub_entry["id"]] = {
                    "offset": (
                        0
                        if offset_table not in sub_entry
                        else get_offset(
                            sub_entry["offset_table"][0], sub_entry["offset_table"][1]
                        )
                    ),
                    "entries": [],
                }

    with dbm.open(out_file, "n") as db:
        for idx, val in enumerate(entries):
            key_encoded = str(val)
            val_encoded = str(idx + 1 + idx_offset)
            db[key_encoded] = val_encoded
            print(f"{out_file}: {key_encoded} -> {val_encoded}")
            if sub_entries is not None:
                with open(
                    f"{in_dir}/{key_encoded}.json", "r", encoding="utf-8"
                ) as parent_file:
                    parent_json = json.load(parent_file)
                    for sub_entry in sub_entries:
                        for sub_elem in parent_json[sub_entry["id"]]:
                            sub_indexes[sub_entry["id"]]["entries"].append(
                                sub_elem[sub_entry["key"]]
                            )

    if sub_entries is not None:
        for sub_entry in sub_entries:
            with dbm.open(sub_entry["out_file"], "n") as sub_entry_map:
                sub_index = sub_indexes[sub_entry["id"]]
                for idx, val in enumerate(sorted(sub_index["entries"])):
                    key_encoded = str(val)
                    val_encoded = str(idx + 1 + sub_index["offset"])
                    sub_entry_map[key_encoded] = val_encoded
                    print(f"{sub_entry['out_file']}: {key_encoded} -> {val_encoded}")


if __name__ == "__main__":
    folders = [
        ("attachments", None, None),
        (
            "conversations",
            None,
            [
                {
                    "id": "messages",
                    "key": "message_id",
                    "offset_table": None,
                    "out_file": "data/transform/messages.map",
                }
            ],
        ),
        ("forums", ("flarum_tags", "id"), None),
        (
            "profile_posts",
            ("flarum_user_comments", "id"),
            [
                {
                    "id": "comments",
                    "key": "comment_id",
                    "offset_table": ("flarum_user_comments", "id"),
                    "out_file": "data/transform/profile_comments.map",
                }
            ],
        ),
        (
            "threads",
            ("flarum_discussions", "id"),
            [
                {
                    "id": "posts",
                    "key": "post_id",
                    "offset_table": ("flarum_posts", "id"),
                    "out_file": "data/transform/posts.map",
                }
            ],
        ),
        ("user_groups", ("flarum_groups", "id"), None),
        ("users", ("flarum_users", "id"), None),
    ]
    # folder = folders[4]
    for folder in folders:
        reindex(
            f"data/raw/{folder[0]}",
            f"data/transform/{folder[0]}.map",
            folder[1],
            folder[2],
        )
