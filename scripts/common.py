import os
import json
import pymysql

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "user",
    "password": "password",
    "database": "database",
    "charset": "utf8mb4",
}


def load_db_config(file_path):
    if not os.path.exists(file_path):
        print(f"{file_path} not found--making it now")
        with open(file_path, "w") as f:
            json.dump(DB_CONFIG, f, indent=2)
        return DB_CONFIG

    with open(file_path, "r") as f:
        return json.load(f)


DB_CONFIG = load_db_config("db_config.json")


def serialize_blob(blob):
    if blob is None:
        return None
    return blob.decode("utf-8", errors="ignore")


def serialize_arr(data):
    if data is None:
        return []

    decoded = data.decode("utf-8")
    if "" == decoded.strip():
        return []

    return [int(x) if x.isdigit() else x for x in data.decode("utf-8").split(",")]


def dump(query, out_dir, row_ops):
    try:
        os.makedirs(out_dir, exist_ok=True)
        con = pymysql.connect(**DB_CONFIG)
        cursor = con.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            row_id = row_ops(row, cursor)
            out_file = os.path.join(out_dir, f"{row_id}.json")

            with open(out_file, "w", encoding="utf-8") as file:
                json.dump(row, file, indent=2, ensure_ascii=False)

            print(f"wrote {out_file}")

    except Exception as e:
        print(f"error: {e}")

    finally:
        if "cursor" in locals():
            cursor.close()
        if "con" in locals():
            con.close()
