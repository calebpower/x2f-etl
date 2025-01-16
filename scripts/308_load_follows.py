import dbm
import json
import os

from common import build_query, db_op, get_mapped_id, to_timestamp

# user_id - pull from follow json (user_id)
# followed_user_id - pull from follow json (follow_user_id)
# created_at - pull from follow json (follow_date) - to_timestamp
# updated_at - same as created_at
# subscription - 'follow'
# id - just auto-increment this one


def insert_follow(cursor, _):
    follow_data = {
        "user_id": None,
        "followed_user_id": None,
        "created_at": None,
        "updated_at": None,
        "subscription": "follow",
    }

    query = build_query(follow_data, "flarum_user_followers")

    for filename in os.listdir("data/raw/users"):
        with open(f"data/raw/users/{filename}", "r", encoding="utf-8") as json_file:
            user_json = json.load(json_file)
            follow_data["user_id"] = get_mapped_id("users.map", user_json["user_id"])

            for follow_json in user_json["follows"]:
                follow_data["followed_user_id"] = get_mapped_id(
                    "users.map", follow_json["follow_user"]
                )
                follow_data["created_at"] = to_timestamp(follow_json["follow_date"])
                follow_data["updated_at"] = follow_data["created_at"]

                print(follow_data)
                cursor.execute(query, tuple(follow_data.values()))

    return True


if __name__ == "__main__":
    db_op("flarum", insert_follow)
