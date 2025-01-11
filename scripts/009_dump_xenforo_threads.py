import re

from common import dump


def fetch_tags(content_id, cursor):
    query = """
  SELECT t.tag AS tag FROM xf_tag_content c
  INNER JOIN xf_tag t ON c.tag_id = t.tag_id
  WHERE c.content_id = %s
  """
    cursor.execute(query, (content_id,))
    res = cursor.fetchall()
    return [re.sub(r"\s+", "-", tag["tag"]) for tag in res]


def fetch_posts(thread_id, cursor):
    query = """
  SELECT post_id, user_id, post_date, message, ip_id, message_state, position,
    last_edit_date, last_edit_user_id, edit_count
  FROM xf_post
  WHERE thread_id = %s AND user_id <> %s
  ORDER BY post_id ASC
  """
    cursor.execute(query, (thread_id, 0))
    res = cursor.fetchall()

    posts = []
    for post in res:
        post_obj = {
            "post_id": post["post_id"],
            "user_id": post["user_id"],
            "post_date": post["post_date"],
            "message": post["message"],
            "ip_id": post["ip_id"],
            "message_state": post["message_state"],
            "position": post["position"],
            "last_edit_date": post["last_edit_date"],
            "last_edit_user_id": post["last_edit_user_id"],
            "edit_count": post["edit_count"],
        }
        posts.append(post_obj)

    return posts


def fetch_poll_votes(poll_id, poll_response_id, cursor):
    query = """
  SELECT user_id, vote_date
  FROM xf_poll_vote
  WHERE poll_response_id = %s AND poll_id = %s AND user_id <> %s
  """
    cursor.execute(
        query,
        (
            poll_response_id,
            poll_id,
            0,
        ),
    )
    res = cursor.fetchall()

    votes = []
    for vote in res:
        vote_obj = {"user_id": vote["user_id"], "vote_date": vote["vote_date"]}
        votes.append(vote_obj)

    return votes


def fetch_poll_responses(poll_id, cursor):
    query = """
  SELECT poll_response_id, response
  FROM xf_poll_response
  WHERE poll_id = %s
  """
    cursor.execute(query, (poll_id,))
    res = cursor.fetchall()

    responses = []
    for response in res:
        response_obj = {
            "response": response["response"],
            "votes": fetch_poll_votes(poll_id, response["poll_response_id"], cursor),
        }
        responses.append(response_obj)

    return responses


def fetch_polls(thread_id, cursor):
    query = """
  SELECT poll_id, question, public_votes, close_date,
    change_vote, view_results_unvoted, max_votes
  FROM xf_poll
  WHERE content_id = %s
  """
    cursor.execute(query, (thread_id,))
    res = cursor.fetchall()

    polls = []
    for poll in res:
        poll_obj = {
            "question": poll["question"],
            "public_votes": poll["public_votes"],
            "close_date": poll["close_date"],
            "change_vote": poll["change_vote"],
            "view_results_unvoted": poll["view_results_unvoted"],
            "max_votes": poll["max_votes"],
            "responses": fetch_poll_responses(poll["poll_id"], cursor),
        }
        polls.append(poll_obj)

    return polls


def mutate_thread(row, cursor):
    thread_id = row["thread_id"]
    row["tags"] = fetch_tags(thread_id, cursor)
    row["posts"] = fetch_posts(thread_id, cursor)
    row["polls"] = fetch_polls(thread_id, cursor)
    return thread_id


def mutate_like(row, cursor):
    return row["like_id"]


if __name__ == "__main__":
    dump(
        """
    SELECT thread_id, node_id AS forum_id, title, user_id, post_date, sticky,
      discussion_state, discussion_open, discussion_type, prefix_id
    FROM xf_thread
    WHERE user_id <> 0 AND discussion_type <> "redirect"
        """,
        "data/raw/threads",
        mutate_thread,
    )
    dump(
        """
    SELECT like_id, content_id AS post_id, like_user_id AS user_id, like_date
    FROM xf_liked_content
    WHERE like_user_id <> 0 AND content_user_id <> 0
        """,
        "data/raw/likes",
        mutate_like,
    )
