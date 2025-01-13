#!/usr/local/bin/bash

for script in $(ls scripts/2*.py); do
  python $script
done
