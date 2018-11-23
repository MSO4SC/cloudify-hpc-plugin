#!/bin/bash -l

REMOTE_URL=$1
IMAGE_URI=$2
IMAGE_NAME=$3

# cd $CURRENT_WORKDIR ## not needed, already started there
rm $IMAGE_NAME
ARCHIVE=$(basename $REMOTE_URL)
rm $ARCHIVE
DIRNAME=$(basename $ARCHIVE .tgz)
rm -r $DIRNAME
rm run_generated.param
