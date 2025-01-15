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
                return (user_db[original_id].decode("utf-8"), user_json["username"])

    except FileNotFoundError as e:
        return None


def get_user_by_name(username):
    with dbm.open("data/transform/users.rev") as user_db:
        if username not in user_db:
            return None
        return get_user_by_id(user_db[username])


def transform_quote(match):
    substring = match.group()
    print(f"transform quote: {substring}")
    
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
    print(f"transform mention: {substring}")
    
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


def transform_media(match):
    substring = match.group()
    print(f"transform media: {substring}")

    media_types = {
        "dailymotion": "https://www.dailymotion.com/video/",
        "facebook": "https://www.facebook.com/video.php?v=",
        "googledocument": "https://docs.google.com/document/pub?id=",
        "vimeo": "https://vimeo.com/",
        "youtube": "https://youtu.be/"
    }
    
    media_type = "youtube"
    media_type_search = re.search(r"\[MEDIA=(.+?)\]", substring, re.IGNORECASE)
    if media_type_search:
        media_type = media_type_search.group(1)

    media_slug = re.search(
        r"\[MEDIA.*?\](.*?)\[\/MEDIA\]", substring, re.IGNORECASE | re.DOTALL
    ).group(1).strip()

    media_url = f"{media_types[media_type]}{media_slug}"
    return f"<URL url=\"{media_url}\"><s>[</s>{media_url}<e>]({media_url})</e></URL>"


def transform_image(match):
    substring = match.group()
    print(f"transform image: {substring}")

    image_url = re.search(
        r"\[IMG.*?\](.*?)\[\/IMG\]", substring, re.IGNORECASE | re.DOTALL
    ).group(1).strip()

    return f"<IMG alt=\"image\" src=\"{image_url}\"><s>![</s>image<e>]({image_url})</e></IMG>"


def transform_url(match):
    substring = match.group()
    print(f"transform url: {substring}")

    link_display = re.search(r"\[URL.*?\](.*?)\[\/URL\]", substring, re.IGNORECASE | re.DOTALL).group(1)
    link_url = link_display
    link_attr = re.search(r"\[URL=(.+?)\]", substring, re.IGNORECASE)
    if link_attr:
        link_url = link_attr.group(1)
    link_url = link_url.strip('\'"')

    old_forum_search = re.search(r"forum[s]?\.meepcraft\.com\/threads\/[^\.]*\.([^\/]*)\/?", link_url)
    if old_forum_search:
        old_forum = old_forum_search.group(1)
        try:
            with dbm.open("data/transform/threads.map", "r") as threads_map:
                thread_id = threads_map[old_forum].decode("utf-8")
                link_url = f"/d/{thread_id}"
                print(f"mapped link to thread {thread_id}")

        except KeyError as _:
            print(f"failed to relink to old thread {old_forum}")

    return f"<URL url=\"{link_url}\"><s>[</s>{link_display}<e>]({link_url})</e></URL>"


def transform_list(match):
    substring = match.group()
    print(f"transform list: {substring}")

    res = []

    for submatch in re.finditer(
        r"(?<=\[\*\])\s*(.*?)\s*(?=\[(\*|\/LIST))",
        substring,
        flags=re.IGNORECASE | re.DOTALL    
    ):
        res.append(f"<LI><s>- </s>{submatch.group(1).strip()}</LI>")

    return f'<LIST>{"".join(res)}</LIST>'


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
        message = modified

        
def wrap_code(match):
    substring = match.group()
    code = re.search(r"\[([^=\/\]]+).*?\]", substring, re.IGNORECASE).group(1)
    print(f"transform {code} code: {substring}")
    
    front_tag_search = re.search(
        r"\[({}=.*?)\]".format(code),
        substring,
        re.IGNORECASE)
    front_tag = front_tag_search.group(1) if front_tag_search else f"[{code}]"
    end_tag = f"[/{code}]"
    inner_content = re.search(
        r"\[{}.*?\](.*?)\[\/{}\]".format(code, code),
        substring,
        re.IGNORECASE | re.DOTALL).group(1)
    
    return f"<{front_tag.strip('[]')}><s>{front_tag}</s>{inner_content}<e>{end_tag}</e><{end_tag.strip('[]')}>"


def slugify(title):
    return re.sub(r"\s+", "_", re.sub(r"[^\w\s]", "", title))[:255].rstrip("_").lower()


if __name__ == "__main__":
    os.makedirs("data/transform/posts", exist_ok=True)

    with dbm.open("data/transform/discussions.agg", "n") as discussions_db:
        with dbm.open("data/transform/posts.agg", "n") as posts_db:

            for thread_entry in os.listdir("data/raw/threads"):
                print(f"reading thread {thread_entry}")
                with open(
                    f"data/raw/threads/{thread_entry}", "r", encoding="utf-8"
                ) as thread_file:
                    thread_json = json.load(thread_file)
                    thread_users = set()

                    for idx, post in enumerate(thread_json["posts"]):
                        post_id = str(post["post_id"])
                        post_message = post["message"]
                        thread_users.add(post["user_id"])

                        # tag conversions
                        
                        conversions = {
                            "DOUBLEPOST": "",
                            "QUOTE": transform_quote,
                            "USER": transform_mention,
                            "URL": transform_url,
                            "MEDIA": transform_media,
                            "IMG": transform_image,
                            "LIST": transform_list
                        }

                        for tag, op in conversions.items():
                            post_message = convert_embeds(post_message, tag, op)

                        # count user mentions

                        post_agg = {
                            "number": idx + 1,
                            "mentioned_users": list(
                                {
                                    match
                                    for match in re.findall(
                                        '<USERMENTION.*?id="(\d+?)">', post_message
                                    )
                                }
                            ),
                            "discussion": os.path.splitext(thread_entry)[0],
                        }

                        # convert the other BB codes

                        codes = ["B", "I", "U", "S", "COLOR", "SIZE", "LEFT", "CENTER", "RIGHT"]
                        for code in codes:
                            post_message = re.sub(
                                r"(?<!<s>)\[{}(=.*?)?\].*?\[\/{}\](?!<\/e>)".format(code, code),
                                wrap_code,
                                post_message,
                                flags=re.IGNORECASE | re.DOTALL
                            )

                        # wrap paragraphs, and add explicit breaks where necessary

                        post_parts = re.split(r'\n{2,}', post_message.strip())
                        post_paragraphs = ["<p>" + part.strip().replace('\n', '<br/>') + "</p>" for part in post_parts]
                        post_message = "".join(post_paragraphs)

                        # final wrapper - <t> for static templating and <r> for dynamic templating

                        wrapper = (
                            "r"
                            if re.search(r"<[^>]+>|\[\/?[a-zA-Z][^\]]*\|:[^\s]+:", post_message)
                            else "t"
                        )
                        post_message = f"<{wrapper}>{post_message}</{wrapper}>"

                        try:
                            with open(f"data/raw/ips/{post['ip_id']}.json") as ip_file:
                                ip_json = json.load(ip_file)
                                post_agg["ip_addr"] = ip_json["ip"]

                        except FileNotFoundError as _:
                            post_agg["ip_addr"] = None

                        with open(
                            f"data/transform/posts/{post_id}.txt", "w"
                        ) as text_file:
                            text_file.write(post_message)

                        posts_db[post_id] = json.dumps(post_agg)

                        print(f"transformed post {post_id}")

                    thread_agg = {
                        "comment_count": len(thread_json["posts"]),
                        "participant_count": len(thread_users),
                        "first_post_id": thread_json["posts"][0]["post_id"],
                        "last_posted_at": thread_json["posts"][-1]["post_date"],
                        "last_posted_user_id": thread_json["posts"][-1]["user_id"],
                        "last_post_id": thread_json["posts"][-1]["post_id"],
                        "slug": slugify(thread_json["title"]),
                    }

                    discussions_db[str(thread_json["thread_id"])] = json.dumps(
                        thread_agg
                    )
