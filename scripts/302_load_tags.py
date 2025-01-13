import dbm
import json
import os
import random
import string

from common import db_op, to_timestamp, build_query

# id - from forum map
# name - from tags transform
# slug - from tags transform
# description - ???
# color - ""
# background_path - NULL
# background_mode - NULL
# position - from tags transform
# parent_id - NULL
# default_sort - NULL
# is_restricted - 0
# is_hidden - 0
# discussion_count - from tags transform
# last_posted_at - from tags transform
# last_posted_discussion_id - NULL, but update this later!
# last_posted_user_id - NULL, but update this later!
# icon - ""
# created_at - from forums
# updated_at - same as created_date
# post_count - from tags transform


def insert_tags(cursor, _):
    tag_data = {
        "id": None,
        "name": None,
        "slug": None,
        "description": None,
        "position": None,
        "is_restricted": 0,
        "is_hidden": 0,
        "discussion_count": None,
        "last_posted_at": None,
        "last_posted_discussion_id": None,
        "last_posted_user_id": None,
        "created_at": None,
        "updated_at": None,
        "post_count": None,
    }

    query = build_query(tag_data, "flarum_tags")

    with dbm.open("data/transform/tags.agg") as tag_db:
        priority = 1
        for tag_id in sorted(tag_db.keys(), key=lambda x: int(x.decode("utf-8"))):
            tag_id_decoded = tag_id.decode("utf-8")

            with dbm.open("data/transform/forums.map") as forum_db:
                tag_data["id"] = int(forum_db[tag_id])

            tag_json = json.loads(tag_db[tag_id])
            tag_data["name"] = tag_json["title"]
            tag_data["slug"] = tag_json["stub"]
            tag_data["position"] = priority
            tag_data["discussion_count"] = tag_json["discussion_count"]
            tag_data["post_count"] = tag_json["post_count"]
            tag_data["created_at"] = to_timestamp(tag_json["created_at"])
            tag_data["last_posted_at"] = to_timestamp(tag_json["last_posted_at"])
            tag_data["updated_at"] = tag_data["last_posted_at"]

            with open(
                f"data/raw/forums/{tag_id_decoded}.json", "r", encoding="utf-8"
            ) as json_file:
                json_data = json.load(json_file)
                tag_data["description"] = json_data["description"]

            priority += 1
            print(tag_data)

            cursor.execute(query, tuple(tag_data.values()))

    return True


if __name__ == "__main__":
    db_op("flarum", insert_tags)
