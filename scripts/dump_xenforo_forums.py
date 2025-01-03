from common import dump

def mutate_forum(row, cursor):
  node_id = row['node_id']
  return node_id

if __name__ == "__main__":
  dump('''
  SELECT f.node_id AS node_id, title, description, node_name, parent_node_id,
    display_order, display_in_list, moderate_replies, allow_posting, default_prefix_id,
    require_prefix, allowed_watch_notifications, moderate_threads, allow_poll, min_tags
  FROM xf_forum f
  INNER JOIN xf_node n
    ON f.node_id = n.node_id
  ''', 'data/raw/forums', mutate_forum)
