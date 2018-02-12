#!/bin/bash

tox -e single_py27 -- hpc_plugin/tests/workflow_tests.py:TestPlugin.test_$1