import datetime
import dbm
import json
import os
import pymysql
import traceback


DEFAULT_DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "user",
    "password": "password",
    "database": "database",
    "charset": "utf8mb4",
}

db_configs = {"flarum": None, "xenforo": None}


def load_db_config(file_path):
    if not os.path.exists(file_path):
        print(f"{file_path} not found--making it now")
        with open(file_path, "w") as f:
            json.dump(DEFAULT_DB_CONFIG, f, indent=2)
        return DEFAULT_DB_CONFIG

    with open(file_path, "r") as f:
        return json.load(f)


for key in db_configs.keys():
    db_configs[key] = load_db_config(f"db_{key}.json")


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


def to_timestamp(unix_seconds):
    return (
        None
        if unix_seconds is None
        else datetime.datetime.fromtimestamp(unix_seconds).strftime("%Y-%m-%d %H:%M:%S")
    )


def db_op(config, op, params=None):
    try:
        con = pymysql.connect(**db_configs[config])
        cursor = con.cursor(pymysql.cursors.DictCursor)
        if op(cursor, params):
            con.commit()

    except Exception as e:
        print(f"error: {e}")
        traceback.print_exc()

    finally:
        if "cursor" in locals():
            cursor.close()
        if "con" in locals():
            con.close()


def get_mapped_id(file, orig_id):
    with dbm.open(f"data/transform/{file}") as dbm_file:
        return dbm_file[str(orig_id)].decode("utf-8")


def dump(query, out_dir, row_ops):
    try:
        os.makedirs(out_dir, exist_ok=True)
        con = pymysql.connect(**db_configs["xenforo"])
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


def get_offset(table, pri_key):
    try:
        con = pymysql.connect(**db_configs["flarum"])
        cursor = con.cursor(pymysql.cursors.DictCursor)
        query = f"SELECT {pri_key} FROM {table} ORDER BY {pri_key} DESC LIMIT 1"
        cursor.execute(query)
        res = cursor.fetchone()

        return 0 if res is None else res["id"]

    except Exception as e:
        print(f"error: {e}")

    finally:
        if "cursor" in locals():
            cursor.close()
        if "con" in locals():
            con.close()


def build_query(dict_obj, table):
    columns = ", ".join(dict_obj.keys())
    placeholders = ", ".join(["%s"] * len(dict_obj))
    return f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
