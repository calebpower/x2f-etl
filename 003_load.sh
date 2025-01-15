#!/usr/local/bin/bash

for script in $(ls scripts/3*.py); do
  python $script
done
