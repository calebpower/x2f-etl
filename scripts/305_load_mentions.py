import dbm
import json

from common import db_op, to_timestamp, get_mapped_id, build_query

# -- flarum_post_mentions_user --
# post_id - pull from posts agg (pull from post map)
# user_id - pull from posts agg (already mapped)
# timestamp - maybe do a direct db call here

def insert_mentions(cursor, _):
    mention_data = {
        "post_id": None,
        "mentions_user_id": None,
    }

    query = """
    INSERT INTO flarum_post_mentions_user (post_id, mentions_user_id, created_at)
    SELECT %s, %s, created_at
    FROM flarum_posts
    WHERE id = %s
    """

    with dbm.open("data/transform/posts.agg") as posts_agg:
        with dbm.open("data/transform/posts.map") as posts_map:
            for post_id in sorted(
                posts_agg.keys(), key=lambda x: int(x.decode("utf-8"))
            ):
                post_id_decoded = posts_map[post_id].decode("utf-8")
                post_json = json.loads(posts_agg[post_id])
                mentioned_users = post_json["mentioned_users"]

                for mentioned_user in mentioned_users:
                    print(f"user {mentioned_user} mentioned in post {post_id_decoded}")
                    cursor.execute(query, (post_id_decoded, mentioned_user, post_id_decoded))

    return True


if __name__ == "__main__":
    db_op("flarum", insert_mentions)
