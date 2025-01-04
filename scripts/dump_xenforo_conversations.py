from common import dump


def fetch_conversation_messages(convo_id, cursor):
    query = """
  SELECT message_id, message_date, user_id, message, ip_id
  FROM xf_conversation_message
  WHERE conversation_id = %s ORDER BY message_date ASC
  """
    cursor.execute(query, (convo_id,))
    res = cursor.fetchall()

    messages = []
    for message in res:
        message_obj = {
            "message_id": message["message_id"],
            "message_date": message["message_date"],
            "user_id": message["user_id"],
            "message": message["message"],
            "ip_id": message["ip_id"],
        }
        messages.append(message)

    return messages


def mutate_conversation(row, cursor):
    convo_id = row["conversation_id"]
    row["messages"] = fetch_conversation_messages(convo_id, cursor)
    return convo_id


if __name__ == "__main__":
    dump(
        """
  SELECT conversation_id, title, user_id, start_date, open_invite, conversation_open
  FROM xf_conversation_master
  ORDER BY start_date ASC
  """,
        "data/raw/conversations",
        mutate_conversation,
    )
