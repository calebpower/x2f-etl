from common import dump

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

def mutate_profile_post(row, cursor):
  post_id = row['profile_post_id']
  row['comments'] = fetch_profile_comments(post_id, cursor)
  return post_id

if __name__ == "__main__":
  dump('''
  SELECT profile_post_id, profile_user_id, user_id, post_date, message, ip_id, message_state
  FROM xf_profile_post
  ORDER BY post_date ASC
  ''', 'data/raw/profile_posts', mutate_profile_post)
