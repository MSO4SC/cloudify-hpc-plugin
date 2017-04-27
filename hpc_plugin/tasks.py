########
# Copyright (c) 2017 MSO4SC - javier.carnero@atos.net
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Holds the plugin tasks """
import os
import json

from cloudify import ctx
from cloudify.decorators import operation

from hpc_plugin.ssh import SshClient


@operation
def login_connection(credentials_path, **kwargs):
    """ Tries to connect to a login node
    TODO Generate an error if connectio is not possible
    TODO Error Handling
    """
    ctx.logger.info('Connecting to login node. Credentials_file: {0}'
                    .format(os.path.join(os.getcwd(), credentials_path)))

    with open(credentials_path) as credentials_file:
        credentials = json.load(credentials_file)

        client = SshClient(credentials['host'],
                           credentials['user'],
                           credentials['password'])
        result, _ = client.send_command('uname')
    ctx.instance.runtime_properties['login'] = result


@operation
def preconfigure_job(credentials_path, workload_type, **kwargs):
    """ Set the job with the HPC credentials """
    ctx.logger.info('Preconfiguring HPC job. Credentials_file: {0}'
                    .format(os.path.join(os.getcwd(), credentials_path)))

    with open(credentials_path) as credentials_file:
        credentials = json.load(credentials_file)

        client = SshClient(credentials['host'],
                           credentials['user'],
                           credentials['password'])
        client.send_command('touch preconfigure.test')

        ctx.source.instance.runtime_properties['credentials'] = credentials
        ctx.source.instance.runtime_properties['workload_manager'] = \
            workload_type


@operation
def send_job(_command, **kwargs):
    """ Sends a job to the HPC """
    # setting node instance runtime property
    ctx.instance.runtime_properties['job_sent'] = _command

    ctx.logger.info('Connecting to login node. Workload: {0}. Command: {1}'
                    .format(ctx.instance.
                            runtime_properties['workload_manager'],
                            _command))

    credentials = ctx.instance.runtime_properties['credentials']

    client = SshClient(credentials['host'],
                       credentials['user'],
                       credentials['password'])
    client.send_command(_command)
