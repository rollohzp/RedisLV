#!/bin/bash

find . -iname "*.[ch]" | xargs grep --color -n -i $1
