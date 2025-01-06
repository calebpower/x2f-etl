import dbm
import os

from common import get_offset


def reindex(in_dir, out_file, offset_table=None):
    idx_offset = (
        0 if offset_table is None else get_offset(offset_table[0], offset_table[1])
    )
    entries = sorted([int(os.path.splitext(f)[0]) for f in os.listdir(in_dir)])
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with dbm.open(out_file, "n") as db:
        for idx, val in enumerate(entries):
            key_encoded = str(val)
            val_encoded = str(idx + 1 + idx_offset)
            db[key_encoded] = val_encoded
            print(f"{out_file}: {key_encoded} -> {val_encoded}")


if __name__ == "__main__":
    folders = [
        ("attachments", None),
        ("conversations", None),
        ("forums", ("flarum_tags", "id")),
        ("profile_posts", ("flarum_user_comments", "id")),
        ("threads", ("flarum_discussions", "id")),
        ("user_groups", ("flarum_groups", "id")),
        ("users", ("flarum_users", "id")),
    ]
    for folder in folders:
        reindex(f"data/raw/{folder[0]}", f"data/transform/{folder[0]}.map", folder[1])
