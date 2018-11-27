#!/bin/bash -l

module load singularity/2.4.2

REMOTE_URL=$1
IMAGE_URI=$2
IMAGE_NAME=$3

# cd $CURRENT_WORKDIR ## not needed, already started there
singularity pull --name $IMAGE_NAME $IMAGE_URI
wget $REMOTE_URL
ARCHIVE=$(basename $REMOTE_URL)
tar zxvf $ARCHIVE
DIRNAME=$(basename $ARCHIVE .tgz)
DECK=$(ls $DIRNAME/*.DATA)
cat << EOF > run_generated.param
ecl-deck-file-name=$(readlink -m $CURRENT_WORKDIR)/$DECK
EOF

singularity pull --name remotelogger-cli.simg shub://sregistry.srv.cesga.es/mso4sc/remotelogger-cli:latest
mkdir -p simoutput
JOB_LOG_FILTER_FILE='logfilter.yaml'
cat << EOF > $JOB_LOG_FILTER_FILE
[
    {
        "filename": "$OUTPUTDIR/.$PREFIXDECK.DEBUG",
        "filters": [
            {pattern: "^================    End of simulation     ===============", severity: "OK"},
            {pattern: "^Time step",  severity: "INFO", maxprogress: 247},
            {pattern: "^Report step",  severity: "WARNING", progress: "+1"},
            {pattern: "^[\\\\s]*[:|=]", verbosity: 2},
            {pattern: "^Keyword", verbosity: 1},
            {pattern: "[\\\\s\\\\S]*", skip: True},
        ]
    }
]
EOF




