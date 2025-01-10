import dbm
import os
import random
import string

from common import db_op

# id - from forum map
# name - from tags transform
# slug - from tags transform
# description - ???
# color - ""
# background_path - NULL
# background_mode - NULL
# position - from tags transform
# parent_id - NULL
# default_sort - NULL
# is_restricted - 0
# is_hidden - 0
# discussion_count - from tags transform
# last_posted_at - from tags transform
# last_posted_discussion_id - NULL, but update this later!
# last_posted_user_id - NULL, but update this later!
# icon - ""
# created_at - from forums
# updated_at - same as created_date
# post_count - from tags transform
