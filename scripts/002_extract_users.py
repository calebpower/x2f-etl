import re

from common import dump, serialize_arr, serialize_blob


def fetch_recipient_conversations(user_id, cursor):
    query = "SELECT conversation_id, recipient_state, last_read_date FROM xf_conversation_recipient WHERE user_id = %s"
    cursor.execute(query, (user_id,))
    res = cursor.fetchall()

    conversations = []
    for convo in res:
        convo_obj = {
            "conversation_id": convo["conversation_id"],
            "recipient_state": convo["recipient_state"],
            "last_read_date": convo["last_read_date"],
        }
        conversations.append(convo_obj)

    return conversations


def fetch_user_alert_optouts(user_id, cursor):
    query = "SELECT alert FROM xf_user_alert_optout WHERE user_id = %s"
    cursor.execute(query, (user_id,))
    res = cursor.fetchall()
    return [alert["alert"].decode("utf-8") if alert["alert"] else None for alert in res]


def fetch_user_bans(user_id, cursor):
    query = "SELECT ban_user_id, ban_date, end_date, user_reason, triggered FROM xf_user_ban WHERE user_id = %s"
    cursor.execute(query, (user_id,))
    res = cursor.fetchall()

    ban_history = []
    for ban in res:
        ban_obj = {
            "banned_by": ban["ban_user_id"],
            "ban_date": ban["ban_date"],
            "end_date": ban["end_date"],
            "user_reason": ban["user_reason"],
            "triggered": ban["triggered"],
        }
        ban_history.append(ban_obj)

    return ban_history


def fetch_user_field_values(user_id, cursor):
    query = "SELECT field_id, field_value FROM xf_user_field_value WHERE user_id = %s"
    cursor.execute(query, (user_id,))
    res = cursor.fetchall()

    field_values = []
    for field_value in res:
        val = field_value["field_value"].strip()
        if "" != val:
            field_value_obj = {
                "field_id": field_value["field_id"].decode("utf-8"),
                "field_value": field_value["field_value"],
            }
            field_values.append(field_value_obj)

    return field_values


def fetch_user_follows(user_id, cursor):
    query = "SELECT follow_user_id, follow_date FROM xf_user_follow WHERE user_id = %s"
    cursor.execute(query, (user_id,))
    res = cursor.fetchall()

    follows = []
    for follow in res:
        follow_obj = {
            "follow_user": follow["follow_user_id"],
            "follow_date": follow["follow_date"],
        }
        follows.append(follow_obj)

    return follows


def fetch_user_group_relations(user_id, cursor):
    query = "SELECT user_group_id, is_primary FROM xf_user_group_relation WHERE user_id = %s"
    cursor.execute(query, (user_id,))
    res = cursor.fetchall()

    relations = []
    for relation in res:
        relation_obj = {
            "user_group": relation["user_group_id"],
            "is_primary": relation["is_primary"],
        }
        relations.append(relation_obj)

    return relations


def fetch_user_ignored(user_id, cursor):
    query = "SELECT ignored_user_id FROM xf_user_ignored WHERE user_id = %s"
    cursor.execute(query, (user_id,))
    res = cursor.fetchall()
    return [ignored["ignored_user_id"] for ignored in res]


def fetch_user_option(user_id, cursor):
    query = "SELECT * FROM xf_user_option WHERE user_id = %s LIMIT 1"
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()

    if result is None:
        return None

    result.pop("user_id", None)
    return result


def fetch_user_privacy(user_id, cursor):
    query = "SELECT * FROM xf_user_privacy WHERE user_id = %s LIMIT 1"
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()

    if result is None:
        return None

    result.pop("user_id", None)
    return result


def fetch_user_profile(user_id, cursor):
    query = "SELECT dob_day, dob_month, dob_year, signature, homepage, location, occupation, avatar_crop_x, avatar_crop_y, about FROM xf_user_profile WHERE user_id = %s LIMIT 1"
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()

    if result is None:
        return None
    return result


def mutate_user(row, cursor):
    user_id = row["user_id"]
    row["secondary_group_ids"] = serialize_arr(row["secondary_group_ids"])
    row["alert_optouts"] = fetch_user_alert_optouts(user_id, cursor)
    row["bans"] = fetch_user_bans(user_id, cursor)
    row["fields"] = fetch_user_field_values(user_id, cursor)
    row["follows"] = fetch_user_follows(user_id, cursor)
    row["group_relations"] = fetch_user_group_relations(user_id, cursor)
    row["ignored_users"] = fetch_user_ignored(user_id, cursor)
    row["options"] = fetch_user_option(user_id, cursor)
    row["privacy"] = fetch_user_privacy(user_id, cursor)
    row["profile"] = fetch_user_profile(user_id, cursor)
    row["conversations"] = fetch_recipient_conversations(user_id, cursor)
    password_hash_match = re.search(
        r"\$2[aby]?\$[0-9]{2}\$[A-Za-z0-9./]{53}", serialize_blob(row["password_hash"])
    )
    row["password_hash"] = password_hash_match.group(0) if password_hash_match else None
    return user_id


if __name__ == "__main__":
    dump(
        """
            SELECT a.user_id AS user_id, username, email, gender, custom_title, language_id, timezone,
            visible, user_group_id, secondary_group_ids, register_date, last_activity,
            user_state, is_moderator, is_admin, is_banned, is_staff, data AS password_hash
            FROM xf_user u
            INNER JOIN xf_user_authenticate a
            ON u.user_id = a.user_id
            """,
        "data/raw/users",
        mutate_user,
    )
