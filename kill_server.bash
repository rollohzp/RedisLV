#!/bin/bash

ip="127.0.0.1:9898"
if [ $# -eq 1 ]; then
  ip=$1
fi

lsof -n -P -i4 | awk -v ip="${ip}" '$9 == ip' | awk '{print $2}' | xargs kill
