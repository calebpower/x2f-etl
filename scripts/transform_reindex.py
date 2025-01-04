import dbm
import os


def reindex(in_dir, out_file):
    entries = sorted([int(os.path.splitext(f)[0]) for f in os.listdir(in_dir)])
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with dbm.open(out_file, "n") as db:
        for idx, val in enumerate(entries):
            key_encoded = str(val)
            val_encoded = str(idx + 1)
            db[key_encoded] = val_encoded
            print(f"{out_file}: {key_encoded} -> {val_encoded}")


if __name__ == "__main__":
    folders = [
        "attachments",
        "conversations",
        "forums",
        "profile_posts",
        "threads",
        "user_groups",
        "users",
    ]
    for folder in folders:
        reindex(f"data/raw/{folder}", f"data/transform/{folder}.map")
