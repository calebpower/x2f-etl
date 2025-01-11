import dbm
import json
import os
import re


def resolve_compound_name(node_id):
    node_type = (
        "forums" if os.path.exists(f"data/raw/forums/{node_id}.json") else "categories"
    )
    with open(
        f"data/raw/{node_type}/{node_id}.json", "r", encoding="utf-8"
    ) as node_file:
        node_json = json.load(node_file)
        return (
            node_json["node_id"],
            (
                (resolve_compound_name(node_json["parent_node_id"])[1] + ": ")
                if "parent_node_id" in node_json
                else ""
            )
            + node_json["title"],
        )


def generate_stub(title):
    return re.sub(r'\s+', '_', re.sub(r'[^\w\s]', '', title))[:100].rstrip('_').lower()
    
if __name__ == "__main__":
    os.makedirs("data/transform", exist_ok=True)

    forum_meta = { }

    # note: if a forum has no threads, it won't have meta
    for thread_entry in os.listdir("data/raw/threads"):
        print(f"reading thread {thread_entry}")
        with open(f"data/raw/threads/{thread_entry}", "r", encoding="utf-8") as thread_file:
            thread = json.load(thread_file)
            forum_id = thread["forum_id"]
            first_post_date = thread["post_date"]
            last_post = thread["posts"][-1]

            last_post_id = None
            with dbm.open("data/transform/posts.map") as thread_db:
                last_post_id = int(thread_db[str(last_post["post_id"])])

            last_post_date = last_post["post_date"]

            last_post_user = None
            with dbm.open("data/transform/users.map") as user_db:
                last_post_user = int(user_db[str(last_post["user_id"])])
                
            comment_count = len(thread["posts"])
            
            if not forum_id in forum_meta:
                forum_meta[forum_id] = {
                    "created_at": first_post_date,
                    "discussion_count": 1,
                    "last_posted_at": last_post_date,
                    "last_posted_discussion_id": last_post_id,
                    "last_posted_user_id": last_post_user,
                    "post_count": comment_count
                }

            else:
                forum = forum_meta[forum_id]
                if forum["last_posted_at"] < last_post_date:
                    forum["last_posted_at"] = last_post_date
                    forum["last_posted_user_id"] = last_post_user
                    forum["last_posted_discussion_id"] = last_post_id
                forum["discussion_count"] += 1
                forum["post_count"] += comment_count
            
    
    with dbm.open("data/transform/tags.agg", "n") as db:
        for forum_entry in os.listdir("data/raw/forums"):
            tag_id = os.path.splitext(forum_entry)[0]
            tag_title = resolve_compound_name(tag_id)
            tag = {
                "title": tag_title[1],
                "stub": generate_stub(tag_title[1])
            }
            if int(tag_id) in forum_meta:
                tag = {**forum_meta[int(tag_id)], **tag}
            else:
                print(f"warning! could not merge tag {tag_id}")
                tag = {
                    "created_at": None,
                    "discussion_count": 0,
                    "last_posted_at": None,
                    "last_posted_discussion_id": None,
                    "last_posted_user_id": None,
                    "post_count": 0,
                    **tag
                }

            db[str(tag_id)] = json.dumps(tag)

    with dbm.open(f"data/transform/tags.agg", "r") as db:
        for key in db.keys():
            key_decoded = key.decode("utf-8")
            value_decoded = db[key].decode("utf-8")
            print(f"{key_decoded} -> {value_decoded}")
        
