import dbm
import json

from common import db_op, to_timestamp, get_mapped_id, build_query

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

            post_data["discussion_id"] = get_mapped_id("threads.map", thread_id_decoded)

            with open(
                f"data/raw/threads/{thread_id_decoded}.json", "r", encoding="utf-8"
            ) as json_file:
                thread_json = json.load(json_file)
                posts_arr = thread_json["posts"]

                for post_json in posts_arr:
                    post_data["id"] = get_mapped_id("posts.map", post_json["post_id"])
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
                            else get_mapped_id(
                                "users.map", post_json["last_edit_user_id"]
                            )
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


def update_discussion_meta(cursor, _):
    discussion_meta = {
        "first_post_id": None,
        "last_posted_at": None,
        "last_posted_user_id": None,
        "last_post_id": None,
    }

    set_clause = ", ".join([f"{key} = %s" for key in discussion_meta.keys()])
    update_stmt = f"UPDATE flarum_discussions SET {set_clause} WHERE id = %s"

    with dbm.open("data/transform/threads.map", "r") as thread_map:
        with dbm.open("data/transform/discussions.agg", "r") as discussion_agg:
            for discussion_id in sorted(
                thread_map.keys(), key=lambda x: int(x.decode("utf-8"))
            ):
                discussion_agg_json = json.loads(discussion_agg[discussion_id])
                discussion_id_decoded = discussion_id.decode("utf-8")
                discussion_meta["id"] = int(thread_map[discussion_id])
                discussion_meta["first_post_id"] = get_mapped_id(
                    "posts.map", discussion_agg_json["first_post_id"]
                )
                discussion_meta["last_posted_at"] = to_timestamp(
                    discussion_agg_json["last_posted_at"]
                )
                discussion_meta["last_posted_user_id"] = get_mapped_id(
                    "users.map", discussion_agg_json["last_posted_user_id"]
                )
                discussion_meta["last_post_id"] = get_mapped_id(
                    "posts.map", discussion_agg_json["last_post_id"]
                )

                print(discussion_meta)

                cursor.execute(update_stmt, tuple(discussion_meta.values()))

    return True


if __name__ == "__main__":
    db_op("flarum", insert_posts)
    db_op("flarum", update_discussion_meta)
