import dbm
import json
import os
import re

from io import StringIO


def get_user_by_id(original_id):
    try:
        with dbm.open("data/transform/users.map") as user_db:
            with open(
                f"data/raw/users/{original_id}.json", "r", encoding="utf-8"
            ) as user_file:
                user_json = json.load(user_file)
                return (user_db[original_id], user_json["username"])

    except FileNotFoundError as e:
        return None


def get_user_by_name(username):
    with dbm.open("data/transform/users.rev") as user_db:
        if username not in user_db:
            return None
        return (user_db[username], username)


def transform_quote(match):
    substring = match.group()
    message = re.search(
        r"\[QUOTE.*?\](.*)\[\/QUOTE\]", substring, re.IGNORECASE | re.DOTALL
    ).group(1)
    string_builder = StringIO()
    string_builder.write(f"<QUOTE><i>&gt; </i><p>{message}")

    user_mapping = None

    try:
        user_id = re.search(
            r"\[QUOTE\=\".*?member: (\d+).*\"\]", substring, re.IGNORECASE
        ).group(1)
        user_mapping = get_user_by_id(user_id)

    except AttributeError as _:

        try:
            username = re.search(
                r"\[QUOTE\=\"?([^ ,]+).*?\]", substring, re.IGNORECASE
            ).group(1)
            user_mapping = get_user_by_name(username)

        except AttributeError as _:
            print("skipping quote attribution: {substring}")

    if user_mapping is not None:
        string_builder.write(
            f'<br/>~ <USERMENTION displayname="{user_mapping[1]}" id="{user_mapping[0]}">@"{user_mapping[1]}"#{user_mapping[0]}</USERMENTION>'
        )

    string_builder.write("</p></QUOTE>")

    return string_builder.getvalue()


def transform_mention(match):
    substring = match.group()
    print(substring)
    raw_username = re.search(
        r"\[USER.*?\](.*)\[\/USER\]", substring, re.IGNORECASE | re.DOTALL
    ).group(1)
    user_mapping = None

    try:
        user_id = re.search(r"\[USER\=(\d+?)\]", substring, re.IGNORECASE).group(1)
        user_mapping = get_user_by_id(user_id)

    except AttributeError as _:

        try:
            username = (
                raw_username[1:] if raw_username.startswith("@") else raw_username
            )
            user_mapping = get_user_by_name(username)

        except AttributeError as _:
            print("skipping quote attribution: {username}")
            return username

    return (
        raw_username
        if user_mapping is None
        else f'<USERMENTION displayname="{user_mapping[1]}" id="{user_mapping[0]}">@"{user_mapping[1]}"#{user_mapping[0]}</USERMENTION>'
    )


def convert_embeds(message, tag, fn):
    tag_template = r"\[{}(?!.*\[{}.*?\]).*?\](?!.*\[{}.*?\]).*?\[\/{}\]"

    while True:
        modified = re.sub(
            tag_template.format(tag, tag, tag, tag),
            fn,
            message,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if modified == message:
            return modified
            break
        message = modified


if __name__ == "__main__":
    os.makedirs("data/transform/posts", exist_ok=True)

    for thread_entry in os.listdir("data/raw/threads"):
        print(f"reading thread {thread_entry}")
        with open(
            f"data/raw/threads/{thread_entry}", "r", encoding="utf-8"
        ) as thread_file:
            thread_json = json.load(thread_file)
            for post in thread_json["posts"]:
                post_id = str(post["post_id"])
                post_message = post["message"]

                post_message = convert_embeds(post_message, "QUOTE", transform_quote)
                post_message = convert_embeds(post_message, "DOUBLEPOST", "")
                post_message = convert_embeds(post_message, "USER", transform_mention)

                with open(f"data/transform/posts/{post_id}.json", "w") as file:
                    file.write(post_message)

                print(f"transformed post {post_id}")
