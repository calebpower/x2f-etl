# id - from user map
# username - from raw
# email - from raw
# is_email_confirmed - set 1
# password - from raw
# avatar_url - from avatar map
# preferences - set NULL
# joined_at - from raw (register_date)
# last_seen_at - from raw (last_activity)
# marked_all_as_read_at - set NULL
# read_nodifications_at - set NULL
# discussion_count - AGGREGATE! - discussion count only
# comment_count - AGGREGATE! - comments include discussion count + comments
# read_flags_at - set NULL
# suspended_until - set NULL if raw not is_banned or 2038-01-01 00:00:00 if is_banned
# suspend_reason - set NULL
# suspend message - set NULL
# clarkwinkelmann_status_emoji - set NULL
# clarkwinkelmann_status_text - set NULL
