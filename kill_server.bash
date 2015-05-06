#!/bin/bash

lsof -n -P -i4 | awk '$9 == "127.0.0.1:9898"' | awk '{print $2}' | xargs kill
