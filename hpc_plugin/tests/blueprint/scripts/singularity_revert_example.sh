#!/bin/bash -l

if [ -f $1/$2 ]; then
    rm $1/$2
fi