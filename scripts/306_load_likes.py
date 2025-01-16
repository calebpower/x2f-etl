import json
import os

from common import db_op, to_timestamp, get_mapped_id, build_query

# -- flarum_post_likes --
# post_id - pull from likes raw (post_id), then post map
# user_id - pull from likes raw (user_id), the user map
# created_at - pull from likes raw (like_date), to_timestamp


def insert_likes(cursor, _):
    like_data = {"post_id": None, "user_id": None, "created_at": None}

    query = build_query(like_data, "flarum_post_likes")
    likes = {}

    for filename in os.listdir("data/raw/likes"):
        with open(f"data/raw/likes/{filename}", "r", encoding="utf-8") as json_file:
            like_json = json.load(json_file)

            try:
                like_data["post_id"] = get_mapped_id("posts.map", like_json["post_id"])

            except KeyError as _:
                print(f"post with id {like_json['post_id']} not found")

            try:
                like_data["user_id"] = get_mapped_id("users.map", like_json["user_id"])

            except KeyError as _:
                print(f"user with id {like_json['user_id']} not found")

            like_data["created_at"] = to_timestamp(like_json["like_date"])

            if (
                like_data["user_id"] in likes
                and like_data["post_id"] in likes[like_data["user_id"]]
            ):
                print(
                    f"duplicate like found (user {like_data['user_id']} and post {like_data['post_id']})"
                )

            else:
                if like_data["user_id"] not in likes:
                    likes[like_data["user_id"]] = []

                likes[like_data["user_id"]].append(like_data["post_id"])

                print(f"user {like_data['user_id']} liked post {like_data['post_id']}")
                cursor.execute(query, tuple(like_data.values()))

    return True


if __name__ == "__main__":
    db_op("flarum", insert_likes)
