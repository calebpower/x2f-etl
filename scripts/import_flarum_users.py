import dbm
import json
import os

from common import dump

# id - from user map
# username - from raw
# email - from raw
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

if __name__ == "__main__":
    with dbm.open("data/transform/users.map") as user_db:
        for user_id in sorted(user_db.keys(), key=lambda x: int(x.decode("utf-8"))):
            user_id_decoded = user_id.decode('utf-8')
            user_data = {
                "id": user_db[user_id],
            }
            with open(
                f"data/raw/users/{user_id_decoded}.json", "r", encoding="utf-8"
            ) as json_file:
                json_data = json.load(json_file)
                user_data["username"] = json_data["username"]
                user_data["email"] = json_data["email"]
                user_data["is_email_confirmed"] = (
                    1 if "valid" == json_data["user_state"] else 0
                )
                user_data["password"] = json_data["password_hash"]
                user_data["joined_at"] = json_data["register_date"]
                user_data["last_seen_at"] = json_data["last_activity"]
                user_data["suspended_until"] = (
                    None if 0 == json_data["is_banned"] else "2038-01-01 00:00:00"
                )
            with dbm.open(f"data/transform/avatars.map", "r") as avatar_map:
                user_data["avatar_url"] = avatar_map[user_id] if user_id in avatar_map else None
            with dbm.open(f"data/transform/threads.agg", "r") as thread_agg:
                if user_id in thread_agg:
                    content_agg = json.loads(thread_agg[user_id])
                    user_data["discussion_count"] = content_agg["threads"]
                    user_data["comment_count"] = content_agg["posts"]
                else:
                    user_data["discussion_count"] = 0
                    user_data["comment_count"] = 0
            print(user_data)
