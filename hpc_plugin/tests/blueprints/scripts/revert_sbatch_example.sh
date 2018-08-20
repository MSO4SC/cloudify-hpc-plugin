#!/bin/bash -l

FILE="touch.script"

if [ -f $FILE ]; then
    rm $FILE
fi
