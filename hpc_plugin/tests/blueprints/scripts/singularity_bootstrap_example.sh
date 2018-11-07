#!/bin/bash -l

module load singularity/2.4.2

if [ ! -f $1/$2 ]; then
    cd $1
    singularity pull $3
fi