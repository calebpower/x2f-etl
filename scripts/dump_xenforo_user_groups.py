from common import dump, serialize_arr

def mutate_user_group(row, cursor):
  group_id = row['user_group_id']
  row['nts_server_group_ids'] = serialize_arr(row['nts_server_group_ids'])
  return group_id

if __name__ == "__main__":
  dump('SELECT * FROM xf_user_group', 'data/raw/user_groups', mutate_user_group)
