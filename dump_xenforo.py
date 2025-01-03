import os
import json
import pymysql
import re

DB_CONFIG = {
  'host': 'localhost',
  'port': 3306,
  'user': 'user',
  'password': 'password',
  'database': 'database',
  'charset': 'utf8mb4'
}

def load_db_config(file_path):
  if not os.path.exists(file_path):
    print(f"{file_path} not found--making it now")
    with open(file_path, 'w') as f:
      json.dump(DB_CONFIG, f, indent=2)
    return DB_CONFIG

  with open(file_path, 'r') as f:
    return json.load(f)

DB_CONFIG = load_db_config('db_config.json')

def serialize_blob(blob):
  if blob is None:
    return None
  return blob.decode('utf-8', errors='ignore')

def serialize_arr(data):
  if data is None:
    return []

  decoded = data.decode('utf-8')
  if '' == decoded.strip():
     return []

  return [
    int(x) if x.isdigit() else x
    for x in data.decode('utf-8').split(',')
  ]

def fetch_recipient_conversations(user_id, cursor):
  query = "SELECT conversation_id, recipient_state, last_read_date FROM xf_conversation_recipient WHERE user_id = %s"
  cursor.execute(query, (user_id,))
  res = cursor.fetchall()

  conversations = []
  for convo in res:
    convo_obj = {
      "conversation_id": convo['conversation_id'],
      "recipient_state": convo['recipient_state'],
      "last_read_date": convo['last_read_date']
    }
    conversations.append(convo_obj)

  return conversations

def fetch_conversation_messages(convo_id, cursor):
  query = '''
  SELECT message_id, message_date, user_id, message, ip_id
  FROM xf_conversation_message
  WHERE conversation_id = %s ORDER BY message_date ASC
  '''
  cursor.execute(query, (convo_id,))
  res = cursor.fetchall()

  messages = []
  for message in res:
    message_obj = {
      "message_id": message['message_id'],
      "message_date": message['message_date'],
      "user_id": message['user_id'],
      "message": message['message'],
      "ip_id": message['ip_id']
    }
    messages.append(message)

  return messages

def fetch_tags(content_id, cursor):
  query = '''
  SELECT t.tag AS tag FROM xf_tag_content c
  INNER JOIN xf_tag t ON c.tag_id = t.tag_id
  WHERE c.content_id = %s
  '''
  cursor.execute(query, (content_id,))
  res = cursor.fetchall()
  return [re.sub(r'\s+', '-', tag['tag']) for tag in res]

def fetch_posts(thread_id, cursor):
  query = '''
  SELECT post_id, user_id, post_date, message, ip_id, message_state, position,
    last_edit_date, last_edit_user_id, edit_count
  FROM xf_post
  WHERE thread_id = %s
  ORDER BY post_id ASC
  '''
  cursor.execute(query, (thread_id,))
  res = cursor.fetchall()

  posts = []
  for post in res:
    post_obj = {
      "post_id": post['post_id'],
      "user_id": post['user_id'],
      "post_date": post['post_date'],
      "message": post['message'],
      "ip_id": post['ip_id'],
      "message_state": post['message_state'],
      "position": post['position'],
      "last_edit_date": post['last_edit_date'],
      "last_edit_user_id": post['last_edit_user_id'],
      "edit_count": post['edit_count']
    }
    posts.append(post_obj)

  return posts

def fetch_poll_votes(poll_id, poll_response_id, cursor):
  query = '''
  SELECT user_id, vote_date
  FROM xf_poll_vote
  WHERE poll_response_id = %s AND poll_id = %s
  '''
  cursor.execute(query, (poll_response_id, poll_id,))
  res = cursor.fetchall()

  votes = []
  for vote in res:
    vote_obj = {
      "user_id": vote['user_id'],
      "vote_date": vote['vote_date']
    }
    votes.append(vote_obj)

  return votes

def fetch_poll_responses(poll_id, cursor):
  query = '''
  SELECT poll_response_id, response
  FROM xf_poll_response
  WHERE poll_id = %s
  '''
  cursor.execute(query, (poll_id,))
  res = cursor.fetchall()

  responses = []
  for response in res:
    response_obj = {
      "response": response['response'],
      "votes": fetch_poll_votes(poll_id, response['poll_response_id'], cursor)
    }
    responses.append(response_obj)

  return responses

def fetch_polls(thread_id, cursor):
  query = '''
  SELECT poll_id, question, public_votes, close_date,
    change_vote, view_results_unvoted, max_votes
  FROM xf_poll
  WHERE content_id = %s
  '''
  cursor.execute(query, (thread_id,))
  res = cursor.fetchall()

  polls = []
  for poll in res:
    poll_obj = {
      "question": poll['question'],
      "public_votes": poll['public_votes'],
      "close_date": poll['close_date'],
      "change_vote": poll['change_vote'],
      "view_results_unvoted": poll['view_results_unvoted'],
      "max_votes": poll['max_votes'],
      "responses": fetch_poll_responses(poll['poll_id'], cursor)
    }
    polls.append(poll_obj)

  return polls

def fetch_user_alert_optouts(user_id, cursor):
  query = "SELECT alert FROM xf_user_alert_optout WHERE user_id = %s"
  cursor.execute(query, (user_id,))
  res = cursor.fetchall()
  return [alert['alert'].decode('utf-8') if alert['alert'] else None for alert in res]

def fetch_user_bans(user_id, cursor):
  query = "SELECT ban_user_id, ban_date, end_date, user_reason, triggered FROM xf_user_ban WHERE user_id = %s"
  cursor.execute(query, (user_id,))
  res = cursor.fetchall()
  
  ban_history = []
  for ban in res:
    ban_obj = {
      "banned_by": ban['ban_user_id'],
      "ban_date": ban['ban_date'],
      "end_date": ban['end_date'],
      "user_reason": ban['user_reason'],
      "triggered": ban['triggered']
    }
    ban_history.append(ban_obj)

  return ban_history

def fetch_user_field_values(user_id, cursor):
  query = "SELECT field_id, field_value FROM xf_user_field_value WHERE user_id = %s"
  cursor.execute(query, (user_id,))
  res = cursor.fetchall()

  field_values = []
  for field_value in res:
    val = field_value['field_value'].strip()
    if '' != val:
      field_value_obj = {
        "field_id": field_value['field_id'].decode('utf-8'),
        "field_value": field_value['field_value']
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
      "follow_user": follow['follow_user_id'],
      "follow_date": follow['follow_date']
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
      "user_group": relation['user_group_id'],
      "is_primary": relation['is_primary']
    }
    relations.append(relation_obj)

  return relations

def fetch_user_ignored(user_id, cursor):
  query = "SELECT ignored_user_id FROM xf_user_ignored WHERE user_id = %s"
  cursor.execute(query, (user_id,))
  res = cursor.fetchall()
  return [ignored['ignored_user_id'] for ignored in res]

def fetch_user_option(user_id, cursor):
  query = "SELECT * FROM xf_user_option WHERE user_id = %s LIMIT 1"
  cursor.execute(query, (user_id,))
  result = cursor.fetchone()

  if result is None:
    return None

  result.pop('user_id', None)
  return result

def fetch_user_privacy(user_id, cursor):
  query = "SELECT * FROM xf_user_privacy WHERE user_id = %s LIMIT 1"
  cursor.execute(query, (user_id,))
  result = cursor.fetchone()

  if result is None:
    return None

  result.pop('user_id', None)
  return result

def fetch_profile_comments(post_id, cursor):
  query = '''
  SELECT profile_post_comment_id, user_id, comment_date, message, ip_id, message_state
  FROM xf_profile_post_comment
  WHERE profile_post_id = %s
  ORDER BY comment_date ASC
  '''
  cursor.execute(query, (post_id,))
  res = cursor.fetchall()

  comments = []
  for comment in res:
    comment_obj = {
      "comment_id": comment['profile_post_comment_id'],
      "user_id": comment['user_id'],
      "comment_date": comment['comment_date'],
      "message": comment['message'],
      "ip_id": comment['ip_id'],
      "message_state": comment['message_state']
    }
    comments.append(comment_obj)

  return comments

def fetch_user_profile(user_id, cursor):
  query = "SELECT dob_day, dob_month, dob_year, signature, homepage, location, occupation, avatar_crop_x, avatar_crop_y, about FROM xf_user_profile WHERE user_id = %s LIMIT 1"
  cursor.execute(query, (user_id,))
  result = cursor.fetchone()

  if result is None:
    return None
  return result

def dump(query, out_dir, row_ops):
  try:
    os.makedirs(out_dir, exist_ok=True)
    con = pymysql.connect(**DB_CONFIG)
    cursor = con.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    rows = cursor.fetchall()

    for row in rows:
      row_id = row_ops(row, cursor)    
      out_file = os.path.join(out_dir, f"{row_id}.json")

      with open(out_file, 'w', encoding='utf-8') as file:
        json.dump(row, file, indent=2, ensure_ascii=False)

      print(f"wrote {out_file}")

  except Exception as e:
    print(f"error: {e}")

  finally:
    if 'cursor' in locals():
      cursor.close()
    if 'con' in locals():
      con.close()

def mutate_attachment(row, cursor):
  attachment_id = row['attachment_id']
  row['content_type'] = row['content_type'].decode('utf-8')
  return attachment_id

def mutate_conversation(row, cursor):
  convo_id = row['conversation_id']
  row['messages'] = fetch_conversation_messages(convo_id, cursor)
  return convo_id

def mutate_forum(row, cursor):
  node_id = row['node_id']
  return node_id

def mutate_thread(row, cursor):
  thread_id = row['thread_id']
  row['tags'] = fetch_tags(thread_id, cursor)
  row['posts'] = fetch_posts(thread_id, cursor)
  row['polls'] = fetch_polls(thread_id, cursor)
  return thread_id

def mutate_user(row, cursor):
  user_id = row['user_id']
  row['secondary_group_ids'] = serialize_arr(row['secondary_group_ids'])
  row['advapps'] = serialize_blob(row['advapps'])
  row['alert_optouts'] = fetch_user_alert_optouts(user_id, cursor)
  row['bans'] = fetch_user_bans(user_id, cursor)
  row['fields'] = fetch_user_field_values(user_id, cursor)
  row['follows'] = fetch_user_follows(user_id, cursor)
  row['group_relations'] = fetch_user_group_relations(user_id, cursor)
  row['ignored_users'] = fetch_user_ignored(user_id, cursor)
  row['options'] = fetch_user_option(user_id, cursor)
  row['privacy'] = fetch_user_privacy(user_id, cursor)
  row['profile'] = fetch_user_profile(user_id, cursor)
  row['conversations'] = fetch_recipient_conversations(user_id, cursor)
  return user_id

def mutate_user_field(row, cursor):
  field_id = row['field_id'].decode('utf-8')
  row['field_id'] = field_id
  row['field_choices'] = serialize_blob(row['field_choices'])
  return field_id

def mutate_user_group(row, cursor):
  group_id = row['user_group_id']
  row['nts_server_group_ids'] = serialize_arr(row['nts_server_group_ids'])
  return group_id

def mutate_profile_post(row, cursor):
  post_id = row['profile_post_id']
  row['comments'] = fetch_profile_comments(post_id, cursor)
  return post_id

if __name__ == "__main__":
  dump('''
  SELECT a.attachment_id AS attachment_id,
    a.data_id AS data_id,
    a.content_type AS content_type,
    a.content_id AS content_id,
    a.attach_date AS attach_date,
    a.unassociated AS unassociated,
    a.view_count AS view_count,
    d.user_id AS user_id,
    d.filename AS filename,
    d.file_size AS file_size,
    d.file_hash AS file_hash,
    d.file_path AS file_path,
    d.width AS width,
    d.height AS height,
    d.thumbnail_width AS thumbnail_width,
    d.thumbnail_height AS thumbnail_height,
    d.attach_count
  FROM xf_attachment a
  INNER JOIN xf_attachment_data d
    ON a.data_id = d.data_id
  ''', 'data/raw/attachments', mutate_attachment)
  dump('''
  SELECT conversation_id, title, user_id, start_date, open_invite, conversation_open
  FROM xf_conversation_master
  ORDER BY start_date ASC
  ''', 'data/raw/conversations', mutate_conversation)
  dump('''
  SELECT f.node_id AS node_id, title, description, node_name, parent_node_id,
    display_order, display_in_list, moderate_replies, allow_posting, default_prefix_id,
    require_prefix, allowed_watch_notifications, moderate_threads, allow_poll, min_tags
  FROM xf_forum f
  INNER JOIN xf_node n
    ON f.node_id = n.node_id
  ''', 'data/raw/forums', mutate_forum)
  dump('''
  SELECT thread_id, node_id AS forum_id, title, user_id, post_date, sticky,
    discussion_state, discussion_open, discussion_type, prefix_id
  FROM xf_thread
  ''', 'data/raw/threads', mutate_thread)
  dump('SELECT * FROM xf_user', 'data/raw/users', mutate_user)
  dump('SELECT * FROM xf_user_field', 'data/raw/user_fields', mutate_user_field)
  dump('SELECT * FROM xf_user_group', 'data/raw/user_groups', mutate_user_group)
  dump('''
  SELECT profile_post_id, profile_user_id, user_id, post_date, message, ip_id, message_state
  FROM xf_profile_post
  ORDER BY post_date ASC
  ''', 'data/raw/profile_posts', mutate_profile_post)
