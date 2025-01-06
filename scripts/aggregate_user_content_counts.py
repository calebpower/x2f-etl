import dbm
import json
import os


def increment_count(count_dict, user_id, delta):
    if user_id in count_dict:
        count_dict[user_id] += delta
    else:
        count_dict[user_id] = delta


def count_comments(posts):
    post_counts = {}
    for post in posts:
        increment_count(post_counts, post["user_id"], 1)
    return post_counts


def count_threads(in_dir, out_file):
    os.makedirs(os.path.dirname(out_file), exist_ok=True)

    thread_counts = {}
    post_counts = {}

    for thread_entry in os.listdir(in_dir):
        print(f"reading {thread_entry}")
        with open(f"{in_dir}/{thread_entry}", "r", encoding="utf-8") as thread_file:
            thread = json.load(thread_file)
            increment_count(thread_counts, thread["user_id"], 1)
            for user_id, comment_count in count_comments(thread["posts"]).items():
                increment_count(post_counts, user_id, comment_count)

    with dbm.open(out_file, "n") as db:
        for user_id in set(thread_counts.keys()) | set(post_counts.keys()):
            entry = {
                "threads": thread_counts[user_id] if user_id in thread_counts else 0,
                "posts": post_counts[user_id] if user_id in post_counts else 0,
            }
            db[str(user_id)] = json.dumps(entry)


if __name__ == "__main__":
    count_threads("data/raw/threads", "data/transform/threads.agg")

    with dbm.open(f"data/transform/threads.agg", "r") as db:
        for key in db.keys():
            key_decoded = key.decode("utf-8")
            value_decoded = db[key].decode("utf-8")
            print(f"{key_decoded} -> {value_decoded}")
