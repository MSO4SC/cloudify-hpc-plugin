#!/bin/bash

if [ -f deploy_$1.test ]; then
    rm deploy_$1.test
fi
