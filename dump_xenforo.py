import os
import json
import pymysql

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

def fetch_user_profile(user_id, cursor):
  query = "SELECT dob_day, dob_month, dob_year, signature, homepage, location, occupation, avatar_crop_x, avatar_crop_y, about FROM xf_user_profile WHERE user_id = %s LIMIT 1"
  cursor.execute(query, (user_id,))
  result = cursor.fetchone()

  if result is None:
    return None
  return result


def dump(table, out_dir, row_ops):
  try:
    os.makedirs(out_dir, exist_ok=True)
    con = pymysql.connect(**DB_CONFIG)
    cursor = con.cursor(pymysql.cursors.DictCursor)
    cursor.execute(f"SELECT * FROM {table}")
    rows = cursor.fetchall()

    for row in rows:
      row_id = row_ops(row, cursor)    
      out_file = os.path.join(out_dir, f"{row_id}.json")

      with open(out_file, 'w', encoding='utf-8') as file:
        json.dump(row, file, indent=2, ensure_ascii=False)

      print(f"{table} {row_id} written to {out_file}")

  except Exception as e:
    print(f"error: {e}")

  finally:
    if 'cursor' in locals():
      cursor.close()
    if 'con' in locals():
      con.close()

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

if __name__ == "__main__":
  dump('xf_user', 'data/raw/users', mutate_user)
  dump('xf_user_field', 'data/raw/user_fields', mutate_user_field)
  dump('xf_user_group', 'data/raw/user_groups', mutate_user_group)
