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
import ast

from cloudify import ctx
from cloudify.decorators import operation

from hpc_plugin.ssh import SshClient
from hpc_plugin.slurm import submit_job


@operation
def login_connection(credentials_path, **kwargs):  # pylint: disable=W0613
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
        _, exit_code = client.send_command('uname', want_output=True)
    ctx.instance.runtime_properties['login'] = (exit_code == 0)


@operation
def preconfigure_job(credentials_path,
                     workload_type,
                     **kwargs):  # pylint: disable=W0613
    """ Set the job with the HPC credentials """
    ctx.logger.info('Preconfiguring HPC job. Credentials_file: {0}'
                    .format(os.path.join(os.getcwd(), credentials_path)))

    with open(credentials_path) as credentials_file:
        credentials = json.load(credentials_file)

        ctx.source.instance.runtime_properties['credentials'] = credentials
        ctx.source.instance.runtime_properties['workload_manager'] = \
            workload_type


@operation
def send_job(job_settings_json, **kwargs):  # pylint: disable=W0613
    """ Sends a job to the HPC """

    ctx.logger.info('Connecting to login node using workload manager: {0}.'
                    .format(ctx.instance.
                            runtime_properties['workload_manager']))

    # Transform & load json to double quotes (single quote not supported)
    job_settings = ast.literal_eval(job_settings_json)

    credentials = ctx.instance.runtime_properties['credentials']

    client = SshClient(credentials['host'],
                       credentials['user'],
                       credentials['password'])

    ctx.logger.debug('Id: {0}.'.format(ctx.instance.id))

    # TODO(emepetres): use workload manager type
    is_submitted = submit_job(client, ctx.instance.id, job_settings)

    client.close_connection()

    if is_submitted:
        ctx.logger.info('Job ' + ctx.instance.id + ' sent.')
    else:
        # TODO(empetres): Raise error
        ctx.logger.error('Job ' + ctx.instance.id + ' sent.')

    ctx.instance.runtime_properties['job_name'] = ctx.instance.id
