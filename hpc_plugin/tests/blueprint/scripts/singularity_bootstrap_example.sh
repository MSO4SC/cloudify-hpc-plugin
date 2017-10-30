#!/bin/bash

module load singularity/2.3.1

if [ ! -f $LUSTRE/openmpi_1.10.7_ring.img ]; then
    cp $SINGULARITY_REPO/openmpi_1.10.7_ring.img $LUSTRE/openmpi_1.10.7_ring.img
fi