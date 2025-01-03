#!/usr/local/bin/bash

for file in $(ls scripts | grep dump_xenforo); do
  python scripts/$file
done
