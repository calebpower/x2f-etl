import dbm
import json
import os
import random
import string

from common import db_op, to_timestamp, get_mapped_id, build_query

# -- flarum_discussion_tag --
# discussion_id - pull from thread json "thread_id" and convert from thread map
# tag_id - pull from thread json "forum_id" and convert from forum map
# created_at - thread json "post_date" and to_timestamp

# -- flarum_discussions --
# id - pull from thread json "thread_id" and convert from thread map
# title - pull from thread json "title"
# comment_count - from discussion agg
# participant_count - from discussion agg
# post_number_index - 0 (default)
# created_at - pull from thread json "post_date" and to_timestamp
# user_id - pull from thread json "user_id" and convert from forum map
# first_post_id - from discussion agg, but then posts map
# last_posted_at - from discussion agg
# last_posted_user_id - from discussion agg, but then users map
# last_post_id - from discussion agg, but then posts map
# last_post_number - the same as comment_count
# hidden_at - NULL (default)
# hidden_user_id - NULL (default)
# slug - from discussion agg
# is_private - 0 (default)
# is_approved - 1 (default)
# is_sticky - 0 (default)
# is_locked - 0 (default)

# -- flarum_posts --
# id - thread json, nested in "posts" arr as "post_id"
# discussion_id - from the thread json (filename, probably)
# number - from the posts agg, "number"
# created_at - nested "post" json, post_date, to_timestamp
# user_id - nested "post" json, user_id, users map
# type - "comment" (string literal)
# content - grab this one from the the posts dir (txt files)
# edited_at - nested "post" json, last_edit_date to_timestamp or NULL
# edited_user_id - nested "post" json, last_edit_user_id or NULL
# hidden_at - NULL (default)
# hidden_user_id - NULL (default)
# ip_address - get IP address from raw after pulling ip_id from nested post json
# is_private - 0 (default)
# is_approved - 1 (default)


def insert_discussions(cursor, _):
    discussion_data = {
        "id": None,
        "title": None,
        "comment_count": None,
        "participant_count": None,
        "created_at": None,
        "user_id": None,
        # "first_post_id": None,
        # "last_posted_at": None,
        # "last_posted_user_id": None,
        # "last_post_id": None,
        # "last_post_number": None,
        "slug": None,
    }

    query = build_query(discussion_data, "flarum_discussions")

    with dbm.open("data/transform/threads.map", "r") as thread_map:
        with dbm.open("data/transform/discussions.agg", "r") as discussion_agg:
            for discussion_id in sorted(
                thread_map.keys(), key=lambda x: int(x.decode("utf-8"))
            ):
                discussion_id_decoded = discussion_id.decode("utf-8")
                discussion_data["id"] = int(thread_map[discussion_id])
                discussion_agg_json = json.loads(discussion_agg[discussion_id])
                discussion_data["comment_count"] = discussion_agg_json["comment_count"]
                # discussion_data["last_post_number"] = discussion_data["last_post_number"]
                discussion_data["participant_count"] = discussion_agg_json[
                    "participant_count"
                ]
                # discussion_data["first_post_id"] = get_mapped_id(
                #    "posts.map",
                #    discussion_agg_json["first_post_id"]
                # )
                # discussion_data["last_posted_at"] = to_timestamp(discussion_agg_json["last_posted_at"])
                # discussion_data["last_posted_user_id"] = get_mapped_id(
                #    "users.map",
                #    discussion_agg_json["last_posted_user_id"]
                # )
                # discussion_data["last_post_id"] = get_mapped_id(
                #    "posts.map",
                #    discussion_agg_json["last_post_id"]
                # )
                discussion_data["slug"] = discussion_agg_json["slug"]
                with open(
                    f"data/raw/threads/{discussion_id_decoded}.json"
                ) as discussion_json_file:
                    discussion_json = json.load(discussion_json_file)
                    discussion_data["title"] = discussion_json["title"]
                    discussion_data["created_at"] = to_timestamp(
                        discussion_json["post_date"]
                    )
                    discussion_data["user_id"] = get_mapped_id(
                        "users.map", discussion_json["user_id"]
                    )

                print(discussion_data)

                cursor.execute(query, tuple(discussion_data.values()))

    return True


def tag_discussions(cursor, _):
    discussion_tag_data = {"discussion_id": None, "tag_id": None, "created_at": None}

    query = build_query(discussion_tag_data, "flarum_discussion_tag")

    with dbm.open("data/transform/threads.map") as thread_map:
        for thread_id in sorted(
            thread_map.keys(), key=lambda x: int(x.decode("utf-8"))
        ):
            thread_id_decoded = thread_id.decode("utf-8")
            with open(
                f"data/raw/threads/{thread_id_decoded}.json", "r", encoding="utf-8"
            ) as json_file:
                thread_json = json.load(json_file)
                discussion_tag_data["discussion_id"] = get_mapped_id(
                    "threads.map", thread_id_decoded
                )
                discussion_tag_data["tag_id"] = get_mapped_id(
                    "forums.map", thread_json["forum_id"]
                )
                discussion_tag_data["created_at"] = to_timestamp(
                    thread_json["post_date"]
                )

            print(discussion_tag_data)

            cursor.execute(query, tuple(discussion_tag_data.values()))

    return True


def insert_posts(cursor, _):
    post_data = {
        "id": None,
        "discussion_id": None,
        "number": None,
        "created_at": None,
        "user_id": None,
        "type": "comment",
        "content": None,
        "edited_at": None,
        "edited_user_id": None,
        "ip_address": None,
    }

    query = build_query(post_data, "flarum_posts")

    with dbm.open("data/transform/threads.map") as thread_map:
        for thread_id in sorted(
            thread_map.keys(), key=lambda x: int(x.decode("utf-8"))
        ):
            thread_id_decoded = thread_id.decode("utf-8")

            post_data["discussion_id"] = get_mapped_id(
                "threads.map", thread_id_decoded
            )

            with open(
                f"data/raw/threads/{thread_id_decoded}.json", "r", encoding="utf-8"
            ) as json_file:
                thread_json = json.load(json_file)
                posts_arr = thread_json["posts"]
                for post_json in posts_arr:
                    post_data["id"] = post_json["post_id"]
                    post_data["created_at"] = to_timestamp(post_json["post_date"])
                    post_data["user_id"] = get_mapped_id(
                        "users.map", post_json["user_id"]
                    )

                    post_data["edited_at"] = (
                        None
                        if post_json["last_edit_date"] == 0
                        else to_timestamp(post_json["last_edit_date"])
                    )

                    try:
                        post_data["edited_user_id"] = (
                            None
                            if post_json["last_edit_user_id"] == 0
                            else get_mapped_id("users.map", post_json["last_edit_user_id"])
                        )

                    except KeyError as _:
                        post_data["edited_user_id"] = None

                    try:
                        with open(f"data/raw/ips/{post_json['ip_id']}.json") as ip_file:
                            ip_json = json.load(ip_file)
                            post_data["ip_address"] = ip_json["ip"]

                    except FileNotFoundError as _:
                        post_data["ip_address"] = None

                    with open(
                        f"data/transform/posts/{post_json['post_id']}.txt"
                    ) as content_file:
                        post_data["content"] = content_file.read()

            print(post_data)

            cursor.execute(query, tuple(post_data.values()))

    return True


if __name__ == "__main__":
    db_op("flarum", insert_discussions)
    db_op("flarum", tag_discussions)
    db_op("flarum", insert_posts)
    # todo: update post meta
    # todo: update likes
