import socket

from common import dump, serialize_blob


def mutate_ip(row, cursor):
    row["content_type"] = serialize_blob(row["content_type"])
    row["action"] = serialize_blob(row["action"])
    row["ip"] = socket.inet_ntop(socket.AF_INET, row["ip"])
    return row["ip_id"]


if __name__ == "__main__":
    dump("SELECT * FROM xf_ip", "data/raw/ips", mutate_ip)
