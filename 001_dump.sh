#!/usr/local/bin/bash

if [ ! -f xenforo/index.php ]; then
  printf 'You need to drop the Xenforo site in the "xenforo" folder.\n' >&2
  exit 1
fi

for file in $(ls scripts | grep dump_xenforo); do
  python scripts/$file
done
