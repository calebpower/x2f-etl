import dbm
import json
import os

from common import build_query, db_op, to_timestamp, get_mapped_id

# -- flarum_polls --
# id - dynamically get this one after entry
# question - truncate to 255, from threads json polls arr obj (question)
# subtitle - if question > 255, then question; else NULL
# image - NULL (default)
# image_alt - NULL (default)
# post_id - first post id (post_id)
# user_id - first post user (user_id)
# public_poll - threads json polls arr obj (public_votes)
# end_date - threads json polls arr obj (close_date)
# created_at - first post create date (post_date)
# updated_at - first post edit date (last_edit_date)
# vote_count - sum of length of votes arrs in responses arr in polls arr in threads json
# allow_multiple_votes - 0 (default)
# max_votes - threads json polls arr obj (max_votes)
# settings - NULL (default)

# -- flarum_poll_options --
# id - dynamically get this one after entry
# answer - get this one from response arr in threads json polls arr (response)
# poll_id - pull this one from flarum_polls dynamically-generated key
# created_at - this should be the same as the flarum_polls created_at
# updated_at - this should be the same as the flarum_polls updated_at
# vote_count - this should be the number of votes in the votes arr under that response arr
# image_url - NULL (default)

# -- flarum_poll_votes --
# id - just dynamically generate this one
# poll_id - pull this one from the flarum_polls dynamically-generated key
# option_id - pull this ione from the flarum_poll_options dynamically-generated key
# user_id - pull this one from the votes arr obj in the threads json polls response arr (user_id)
# created_at - pull this one from the votes arr obj in the threads json polls response arr (vote_date)
# updated_at - NULL (default)


def insert_polls(cursor, _):
    poll_data = {
        "question": None,
        "subtitle": None,
        "post_id": None,
        "user_id": None,
        "public_poll": None,
        "end_date": None,
        "created_at": None,
        "updated_at": None,
        "vote_count": None,
        "max_votes": None,
        "settings": None,
    }

    settings_data = {
        "public_poll": False,
        "allow_multiple_votes": False,
        "max_votes": 0,
        "hide_votes": False,
        "allow_change_vote": True,
    }

    option_data = {
        "answer": None,
        "poll_id": None,
        "created_at": None,
        "updated_at": None,
        "vote_count": None,
    }

    vote_data = {
        "poll_id": None,
        "option_id": None,
        "user_id": None,
        "created_at": None,
    }

    ins_poll_stmt = build_query(poll_data, "flarum_polls")
    ins_option_stmt = build_query(option_data, "flarum_poll_options")
    ins_vote_stmt = build_query(vote_data, "flarum_poll_votes")

    for thread_entry in sorted(
        [int(os.path.splitext(f)[0]) for f in os.listdir("data/raw/threads")]
    ):
        with open(f"data/raw/threads/{thread_entry}.json") as thread_file:
            thread_json = json.load(thread_file)
            first_post = thread_json["posts"][0]

            poll_data["post_id"] = get_mapped_id("posts.map", first_post["post_id"])
            poll_data["user_id"] = get_mapped_id("users.map", first_post["user_id"])
            poll_data["created_at"] = to_timestamp(first_post["post_date"])
            poll_data["updated_at"] = (
                None
                if 0 == first_post["last_edit_date"]
                else to_timestamp(first_post["last_edit_date"])
            )

            option_data["created_at"] = poll_data["created_at"]
            option_data["updated_at"] = poll_data["updated_at"]

            for poll_json in thread_json["polls"]:

                poll_data["question"] = poll_json["question"].strip()
                if len(poll_data["question"]) > 255:
                    poll_data["subtitle"] = poll_data["question"]
                    last_space_idx = poll_data["question"].rfind(" ")
                    if -1 == last_space_idx:
                        last_space_idx = 252
                    poll_data["question"] = (
                        f'{poll_data["question"][:last_space_idx]}...'
                    )
                else:
                    poll_data["subtitle"] = None

                poll_data["public_poll"] = poll_json["public_votes"]
                poll_data["max_votes"] = poll_json["max_votes"]
                poll_data["vote_count"] = sum(
                    len(entry["votes"]) for entry in poll_json["responses"]
                )

                settings_data["public_poll"] = bool(poll_data["public_poll"])
                settings_data["allow_multiple_votes"] = 1 < poll_data["max_votes"]
                settings_data["max_votes"] = poll_data["max_votes"]
                poll_data["settings"] = json.dumps(settings_data)

                print(poll_data)
                cursor.execute(ins_poll_stmt, tuple(poll_data.values()))
                option_data["poll_id"] = cursor.lastrowid
                vote_data["poll_id"] = option_data["poll_id"]

                missing_vote_count_poll = 0

                for response_json in poll_json["responses"]:

                    option_data["answer"] = (
                        response_json["response"]
                        if len(response_json["response"]) <= 255
                        else f'{response_json["response"][:252]}...'
                    )
                    option_data["vote_count"] = len(response_json["votes"])

                    print(option_data)
                    cursor.execute(ins_option_stmt, tuple(option_data.values()))
                    vote_data["option_id"] = cursor.lastrowid

                    missing_vote_count_res = 0

                    for vote_json in response_json["votes"]:

                        try:
                            vote_data["user_id"] = get_mapped_id(
                                "users.map", vote_json["user_id"]
                            )
                            vote_data["created_at"] = to_timestamp(
                                vote_json["vote_date"]
                            )

                            print(vote_data)
                            cursor.execute(ins_vote_stmt, tuple(vote_data.values()))

                        except KeyError as _:
                            missing_vote_count_res += 1

                    if missing_vote_count_res:
                        missing_vote_count_poll += missing_vote_count_res

                        print(
                            f'remove {missing_vote_count_res} vote(s) from option {vote_data["option_id"]}'
                        )

                        cursor.execute(
                            "UPDATE flarum_poll_options SET vote_count = %s WHERE id = %s",
                            (
                                option_data["vote_count"] - missing_vote_count_res,
                                vote_data["option_id"],
                            ),
                        )

                if missing_vote_count_poll:

                    print(
                        f'remove {missing_vote_count_poll} vote(s) from poll {option_data["poll_id"]}'
                    )

                    cursor.execute(
                        "UPDATE flarum_polls SET vote_count = %s WHERE id = %s",
                        (
                            poll_data["vote_count"] - missing_vote_count_poll,
                            option_data["poll_id"],
                        ),
                    )

    return True


if __name__ == "__main__":
    db_op("flarum", insert_polls)
