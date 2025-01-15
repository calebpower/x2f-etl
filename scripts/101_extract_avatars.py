import dbm
import os
import random
import string

from PIL import Image


def generate_filename(directory, length=16):
    while True:
        filename = "".join(
            random.choices(string.ascii_letters + string.digits, k=length)
        )
        filepath = os.path.join(directory, filename)

        if not os.path.exists(filepath):
            return f"{filename}.png"


def copy_avatars(in_dir, out_dir, map_db):
    for original in [entry for entry in os.listdir(in_dir) if entry != "index.html"]:
        user_id, _ = os.path.splitext(original)
        new_filename = generate_filename(out_dir)
        with Image.open(f"{in_dir}/{original}") as img:
            out_path = f"{out_dir}/{new_filename}"
            img.save(out_path)
            map_db[user_id] = new_filename
            print(f"converted avatar {user_id} -> {out_path}")


if __name__ == "__main__":
    in_dir = "xenforo/data/avatars/l"
    out_dir = "data/raw/avatars"
    map_file = "data/transform/avatars.map"

    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(os.path.dirname(map_file), exist_ok=True)

    with dbm.open(map_file, "n") as map_db:
        for d in [entry for entry in os.listdir(in_dir) if entry != "index.html"]:
            copy_avatars(f"{in_dir}/{d}", out_dir, map_db)
