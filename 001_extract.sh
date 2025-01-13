#!/usr/local/bin/bash

if [ ! -f xenforo/index.php ]; then
  printf 'You need to drop the Xenforo site in the "xenforo" folder.\n' >&2
  exit 1
fi

for script in $(ls scripts/1*.py); do
  python $script
done
