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
import requests
from cloudify import ctx
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

from hpc_plugin.ssh import SshClient
from hpc_plugin import slurm


@operation
def login_connection(config, simulate, **kwargs):  # pylint: disable=W0613
    """ Tries to connect to a login node """
    ctx.logger.info('Connecting to login node..')
    credentials = config['credentials']
    if not simulate:
        client = SshClient(credentials['host'],
                           credentials['user'],
                           credentials['password'])
        _, exit_code = client.send_command('uname', want_output=True)

        if exit_code is not 0:
            raise NonRecoverableError(
                "failed to connect to HPC: exit code " + str(exit_code))

        ctx.instance.runtime_properties['login'] = exit_code is 0
    else:
        ctx.instance.runtime_properties['login'] = True
        ctx.logger.warning('HPC login connection simulated')


@operation
def preconfigure_job(config,
                     monitor_entrypoint,
                     monitor_port,
                     monitor_orchestrator_port,
                     job_prefix,
                     simulate,
                     **kwargs):  # pylint: disable=W0613
    """ Set the job with the HPC credentials """
    ctx.logger.info('Preconfiguring HPC job..')

    ctx.source.instance.runtime_properties['credentials'] = \
        config['credentials']
    ctx.source.instance.runtime_properties['monitor_entrypoint'] = \
        monitor_entrypoint
    ctx.source.instance.runtime_properties['monitor_port'] = monitor_port
    ctx.source.instance.runtime_properties['monitor_orchestrator_port'] = \
        monitor_orchestrator_port
    ctx.source.instance.runtime_properties['workload_manager'] = \
        config['workload_manager']
    ctx.source.instance.runtime_properties['simulate'] = simulate
    ctx.source.instance.runtime_properties['job_prefix'] = job_prefix


@operation
def start_monitoring_hpc(config,
                         monitor_entrypoint,
                         monitor_port,
                         monitor_orchestrator_port,
                         simulate,
                         **kwargs):  # pylint: disable=W0613
    """ blah """
    ctx.logger.info('Starting infrastructure monitor..')

    if not simulate:
        credentials = config['credentials']
        workload_manager = config['workload_manager']
        country_tz = config['country_tz']

        url = 'http://' + monitor_entrypoint + \
            monitor_orchestrator_port + '/exporters/add'

        payload = ("{\n\t\"host\": \"" + credentials['host'] +
                   "\",\n\t\"type\": \"" + workload_manager +
                   "\",\n\t\"persistent\": false,\n\t\"args\": {\n\t\t\""
                   "user\": \"" + credentials['user'] + "\",\n\t\t\""
                   "pass\": \"" + credentials['password'] + "\",\n\t\t\""
                   "tz\": \"" + country_tz + "\",\n\t\t\""
                   "log\": \"debug\"\n\t}\n}")
        headers = {
            'content-type': "application/json",
            'cache-control': "no-cache",
        }

        response = requests.request("POST", url, data=payload, headers=headers)

        if response.status_code != 201:
            raise NonRecoverableError(
                "failed to start node monitor: " + str(response.status_code))
    else:
        ctx.logger.warning('HPC monitor simulated')


@operation
def stop_monitoring_hpc(config,
                        monitor_entrypoint,
                        monitor_port,
                        monitor_orchestrator_port,
                        simulate,
                        **kwargs):  # pylint: disable=W0613
    """ blah """
    ctx.logger.info('Stoping infrastructure monitor..')

    if not simulate:
        credentials = config['credentials']
        workload_manager = config['workload_manager']
        country_tz = config['country_tz']

        url = 'http://' + monitor_entrypoint + \
            monitor_orchestrator_port + '/exporters/remove'

        payload = ("{\n\t\"host\": \"" + credentials['host'] +
                   "\",\n\t\"type\": \"" + workload_manager +
                   "\",\n\t\"persistent\": false,\n\t\"args\": {\n\t\t\""
                   "user\": \"" + credentials['user'] + "\",\n\t\t\""
                   "pass\": \"" + credentials['password'] + "\",\n\t\t\""
                   "tz\": \"" + country_tz + "\",\n\t\t\""
                   "log\": \"debug\"\n\t}\n}")
        headers = {
            'content-type': "application/json",
            'cache-control': "no-cache",
        }

        response = requests.request("POST", url, data=payload, headers=headers)

        if response.status_code != 200:
            raise NonRecoverableError(
                "failed to stop node monitor: " + str(response.status_code))
    else:
        ctx.logger.warning('HPC monitor simulated')


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

    # Execute and print output
    output = os.popen(call).read()
    ctx.logger.info(output)

    # TODO(emepetres): Handle errors
    ctx.logger.info('..job deployed')


@operation
def send_job(job_options, **kwargs):  # pylint: disable=W0613
    """ Sends a job to the HPC """
    simulate = ctx.instance.runtime_properties['simulate']
    ctx.logger.info('Connecting to login node using workload manager: {0}.'
                    .format(ctx.instance.
                            runtime_properties['workload_manager']))

    credentials = ctx.instance.runtime_properties['credentials']
    name = kwargs['name']

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
        ctx.logger.error('Job ' + name + ' (' + ctx.instance.id +
                         ') not sent.')
        raise NonRecoverableError('Job ' + name + ' (' + ctx.instance.id +
                                  ') not sent.')

    ctx.instance.runtime_properties['job_name'] = name
