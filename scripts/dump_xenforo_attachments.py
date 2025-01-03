from common import dump

def mutate_attachment(row, cursor):
  attachment_id = row['attachment_id']
  row['content_type'] = row['content_type'].decode('utf-8')
  return attachment_id

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
