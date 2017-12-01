#!/bin/bash -l

module load singularity/2.3.1

if [ ! -f $1/$2 ]; then
    cp $SINGULARITY_REPO/$2 $1
fi