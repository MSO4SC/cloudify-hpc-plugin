# cloudify-hpc-plugin
Plugin to allow Cloudify to deploy and orchestrate HPC resources

## Usage

TODO

## Tests

Currently the tests are thought to run on the CESGA HPC Finis Terrae II, though they should work with any HPC using Slurm.

The first time, credentials file should be provided and requirements installed.
First move hpc_plugin/tests/credentials and copy sample_hpc.json as cesga.json. Then change it with your HPC connection parameters.
Finally `dev-requirements.txt` should be installed:
```
pip install -r dev-requirements.txt
```

To run the tests, run tox on the root folder
```
tox -e flake8,py27
```


