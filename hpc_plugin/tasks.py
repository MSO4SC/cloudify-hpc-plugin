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
from cloudify import ctx
from cloudify.decorators import operation

from hpc_plugin.ssh import SshClient
from hpc_plugin import slurm


@operation
def login_connection(credentials, simulate, **kwargs):  # pylint: disable=W0613
    """ Tries to connect to a login node
    TODO Generate an error if connection is not possible
    TODO Error Handling
    """
    ctx.logger.info('Connecting to login node..')

    if not simulate:
        client = SshClient(credentials['host'],
                           credentials['user'],
                           credentials['password'])
        _, exit_code = client.send_command('uname', want_output=True)

        ctx.instance.runtime_properties['login'] = exit_code is 0
    else:
        ctx.instance.runtime_properties['login'] = True
        ctx.logger.warning('HPC login connection simulated')


@operation
def preconfigure_job(credentials,
                     workload_manager,
                     simulate,
                     **kwargs):  # pylint: disable=W0613
    """ Set the job with the HPC credentials """
    ctx.logger.info('Preconfiguring HPC job..')

    ctx.source.instance.runtime_properties['credentials'] = credentials
    ctx.source.instance.runtime_properties['workload_manager'] = \
        workload_manager
    ctx.source.instance.runtime_properties['simulate'] = simulate


@operation
def deploy_job(deployment,
               **kwarsgs):  # pylint: disable=W0613
    """ Deploy a job using a script that receives SSH credentials as imput """
    if not deployment:
        return

    ctx.logger.info('Deploying job..')

    simulate = ctx.instance.runtime_properties['simulate']
    if simulate:
        ctx.logger.warning('HPC Deployment simulated')
        return

    # Build the execution call
    call = os.path.normcase(deployment['file'])
    credentials = ctx.instance.runtime_properties['credentials']
    call += ' ' + credentials['host'] + ' ' + \
        credentials['user'] + ' ' + \
        credentials['password']
    if 'inputs' in deployment:
        for dinput in deployment['inputs']:
            call += ' ' + dinput

    print call
    # Execute and print output
    output = os.popen(call).read()
    ctx.logger.info(output)

    # TODO(emepetres): Handle errors
    ctx.logger.info('..job deployed')


@operation
def send_job(prefix_name, job_options, **kwargs):  # pylint: disable=W0613
    """ Sends a job to the HPC """
    simulate = ctx.instance.runtime_properties['simulate']
    ctx.logger.info('Connecting to login node using workload manager: {0}.'
                    .format(ctx.instance.
                            runtime_properties['workload_manager']))

    credentials = ctx.instance.runtime_properties['credentials']

    instance_components = ctx.instance.id.split('_')
    name = prefix_name + instance_components[-1]

    if not simulate:
        client = SshClient(credentials['host'],
                           credentials['user'],
                           credentials['password'])

        # TODO(emepetres): use workload manager type
        is_submitted = slurm.submit_job(client,
                                        name,
                                        job_options)

        client.close_connection()
    else:
        ctx.logger.warning('Instance ' + ctx.instance.id + ' simulated')
        is_submitted = True

    if is_submitted:
        ctx.logger.info('Job ' + name + ' (' + ctx.instance.id + ') sent.')
    else:
        # TODO(empetres): Raise error
        ctx.logger.error('Job ' + name + ' (' + ctx.instance.id +
                         ') not sent.')
        return

    ctx.instance.runtime_properties['job_name'] = name
