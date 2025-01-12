import dbm
import json
import os
import random
import string

from common import db_op, to_timestamp

# id - from user map
# username - from raw
# email - from raw (random@example.com) if null
# is_email_confirmed - set 1 if user_state is valid, 0 otherwise
# password - from raw
# avatar_url - from avatar map
# preferences - set NULL
# joined_at - from raw (register_date)
# last_seen_at - from raw (last_activity)
# marked_all_as_read_at - set NULL
# read_nodifications_at - set NULL
# discussion_count - from thread agg (threads)
# comment_count - from comment agg (posts)
# read_flags_at - set NULL
# suspended_until - set NULL if raw not is_banned or 2038-01-01 00:00:00 if is_banned
# suspend_reason - set NULL
# suspend message - set NULL
# clarkwinkelmann_status_emoji - set NULL
# clarkwinkelmann_status_text - set NULL


def insert_users(cursor, _):
    user_data = {
        "id": None,
        "username": None,
        "email": None,
        "is_email_confirmed": None,
        "password": None,
        "avatar_url": None,
        "joined_at": None,
        "last_seen_at": None,
        "discussion_count": None,
        "comment_count": None,
        "suspended_until": None,
    }

    columns = ", ".join(user_data.keys())
    placeholders = ", ".join(["%s"] * len(user_data))
    query = f"INSERT INTO flarum_users ({columns}) VALUES ({placeholders})"

    with dbm.open("data/transform/users.map") as user_db:
        for user_id in sorted(user_db.keys(), key=lambda x: int(x.decode("utf-8"))):
            user_id_decoded = user_id.decode("utf-8")
            user_data["id"] = int(user_db[user_id])
            with open(
                f"data/raw/users/{user_id_decoded}.json", "r", encoding="utf-8"
            ) as json_file:
                json_data = json.load(json_file)
                user_data["username"] = json_data["username"]
                user_data["email"] = (
                    "".join(random.choices(string.ascii_letters + string.digits, k=16))
                    if json_data["email"] is None or json_data["email"] == ""
                    else json_data["email"]
                )
                user_data["is_email_confirmed"] = (
                    1 if "valid" == json_data["user_state"] else 0
                )
                user_data["password"] = (
                    ""
                    if json_data["password_hash"] is None
                    else json_data["password_hash"]
                )
                user_data["joined_at"] = to_timestamp(json_data["register_date"])
                user_data["last_seen_at"] = to_timestamp(json_data["last_activity"])
                user_data["suspended_until"] = (
                    None if 0 == json_data["is_banned"] else "2038-01-01 00:00:00"
                )
            with dbm.open(f"data/transform/avatars.map", "r") as avatar_map:
                user_data["avatar_url"] = (
                    avatar_map[user_id] if user_id in avatar_map else None
                )
            with dbm.open(f"data/transform/threads.agg", "r") as thread_agg:
                if user_id in thread_agg:
                    content_agg = json.loads(thread_agg[user_id])
                    user_data["discussion_count"] = content_agg["threads"]
                    user_data["comment_count"] = content_agg["posts"]
                else:
                    user_data["discussion_count"] = 0
                    user_data["comment_count"] = 0
            print(user_data)

            cursor.execute(query, tuple(user_data.values()))

    return True


if __name__ == "__main__":
    db_op("flarum", insert_users)
