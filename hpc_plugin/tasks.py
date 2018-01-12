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
        _, exit_code = client.send_command('uname', wait_result=True)

        if exit_code is not 0:
            raise NonRecoverableError(
                "failed to connect to HPC: exit code " + str(exit_code))

        ctx.instance.runtime_properties['login'] = exit_code is 0
    else:
        ctx.instance.runtime_properties['login'] = True
        ctx.logger.warning('HPC login connection simulated')


@operation
def preconfigure_job(config,
                     external_monitor_entrypoint,
                     external_monitor_port,
                     external_monitor_type,
                     external_monitor_orchestrator_port,
                     job_prefix,
                     simulate,
                     **kwargs):  # pylint: disable=W0613
    """ Set the job with the HPC credentials """
    ctx.logger.info('Preconfiguring HPC job..')

    ctx.source.instance.runtime_properties['credentials'] = \
        config['credentials']
    ctx.source.instance.runtime_properties['monitor_entrypoint'] = \
        external_monitor_entrypoint
    ctx.source.instance.runtime_properties['monitor_port'] = \
        external_monitor_port
    ctx.source.instance.runtime_properties['monitor_type'] = \
        external_monitor_type
    ctx.source.instance.runtime_properties['monitor_orchestrator_port'] = \
        external_monitor_orchestrator_port
    ctx.source.instance.runtime_properties['workload_manager'] = \
        config['workload_manager']
    ctx.source.instance.runtime_properties['simulate'] = simulate
    ctx.source.instance.runtime_properties['job_prefix'] = job_prefix


@operation
def start_monitoring_hpc(config,
                         external_monitor_entrypoint,
                         external_monitor_port,
                         external_monitor_orchestrator_port,
                         simulate,
                         **kwargs):  # pylint: disable=W0613
    """ Starts monitoring using the Monitor orchestrator """
    if external_monitor_entrypoint:
        ctx.logger.info('Starting infrastructure monitor..')

        if not simulate:
            credentials = config['credentials']
            workload_manager = config['workload_manager']
            country_tz = config['country_tz']

            url = 'http://' + external_monitor_entrypoint + \
                external_monitor_orchestrator_port + '/exporters/add'

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

            response = requests.request(
                "POST", url, data=payload, headers=headers)

            if response.status_code != 201:
                raise NonRecoverableError(
                    "failed to start node monitor: " + str(response
                                                           .status_code))
        else:
            ctx.logger.warning('HPC monitor simulated')


@operation
def stop_monitoring_hpc(config,
                        external_monitor_entrypoint,
                        external_monitor_port,
                        external_monitor_orchestrator_port,
                        simulate,
                        **kwargs):  # pylint: disable=W0613
    """ Stops monitoring using the Monitor Orchestrator """
    if external_monitor_entrypoint:
        ctx.logger.info('Stoping infrastructure monitor..')

        if not simulate:
            credentials = config['credentials']
            workload_manager = config['workload_manager']
            country_tz = config['country_tz']

            url = 'http://' + external_monitor_entrypoint + \
                external_monitor_orchestrator_port + '/exporters/remove'

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

            response = requests.request(
                "POST", url, data=payload, headers=headers)

            if response.status_code != 200:
                if response.status_code == 409:
                    ctx.logger.warning(
                        'Already removed on the exporter orchestrator.')
                else:
                    raise NonRecoverableError(
                        "failed to stop node monitor: " + str(response
                                                              .status_code))
        else:
            ctx.logger.warning('HPC monitor simulated')


@operation
def bootstrap_job(deployment, **kwarsgs):  # pylint: disable=W0613
    """Bootstrap a job with a script that receives SSH credentials as imput"""
    if not deployment:
        return

    ctx.logger.info('Bootstraping job..')
    simulate = ctx.instance.runtime_properties['simulate']

    if not simulate and 'bootstrap' in deployment:
        inputs = deployment['inputs'] if 'inputs' in deployment else []
        credentials = ctx.instance.runtime_properties['credentials']
        name = "bootstrap_" + ctx.instance.id + ".sh"

        is_bootstraped = deploy_job(
            deployment['bootstrap'], inputs, credentials, name, ctx.logger)
    else:
        is_bootstraped = True

    if is_bootstraped:
        ctx.logger.info('..job bootstraped')
    else:
        ctx.logger.error('Job not bootstraped.')
        raise NonRecoverableError("Bootstrap failed")


@operation
def revert_job(deployment, **kwarsgs):  # pylint: disable=W0613
    """Revert a job using a script that receives SSH credentials as input"""
    if not deployment:
        return

    ctx.logger.info('Reverting job..')
    simulate = ctx.instance.runtime_properties['simulate']

    if not simulate and 'revert' in deployment:
        inputs = deployment['inputs'] if 'inputs' in deployment else []
        credentials = ctx.instance.runtime_properties['credentials']
        name = "revert_" + ctx.instance.id + ".sh"

        is_reverted = deploy_job(
            deployment['revert'], inputs, credentials, name, ctx.logger)
    else:
        is_reverted = True

    if is_reverted:
        ctx.logger.info('..job reverted')
    else:
        ctx.logger.error('Job not reverted.')


def deploy_job(script,
               inputs,
               credentials,
               name,
               logger):  # pylint: disable=W0613
    """ Exec a eployment job script that receives SSH credentials as input """

    # Build the execution call
    script_data = ctx.get_resource(script).replace("$", "\\$")

    # Execute and print output
    client = SshClient(credentials['host'],
                       credentials['user'],
                       credentials['password'])

    create_call = "echo \"" + script_data + "\" >> " + name + \
        "; chmod +x " + name
    _, exit_code = client.send_command(create_call, wait_result=True)
    if exit_code is not 0:
        logger.error(
            "failed to create deploy script: call '" + create_call +
            "', exit code " + str(exit_code))
    else:
        call = "./" + name
        for dinput in inputs:
            call += ' ' + dinput
        _, exit_code = client.send_command(call, wait_result=True)
        if exit_code is not 0:
            logger.warning(
                "failed to deploy job: call '" + call + "', exit code " +
                str(exit_code))

        if not client.send_command("rm " + name):
            logger.warning("failed removing bootstrap script")

    client.close_connection()

    return exit_code is 0


@operation
def send_job(job_options, **kwargs):  # pylint: disable=W0613
    """ Sends a job to the HPC """
    simulate = ctx.instance.runtime_properties['simulate']

    credentials = ctx.instance.runtime_properties['credentials']
    name = kwargs['name']
    is_singularity = 'hpc.nodes.singularity_job' in ctx.node.\
        type_hierarchy

    if not simulate:

        client = SshClient(credentials['host'],
                           credentials['user'],
                           credentials['password'])

        # TODO(emepetres): use workload manager type
        is_submitted = slurm.submit_job(client,
                                        name,
                                        job_options,
                                        is_singularity,
                                        ctx.logger)
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


@operation
def clean_job_aux_files(job_options, avoid, **kwargs):  # pylint: disable=W0613
    """Clean the aux files of the job in the HPC"""
    if avoid:
        return

    simulate = ctx.instance.runtime_properties['simulate']
    name = kwargs['name']
    if not simulate:
        is_singularity = 'hpc.nodes.singularity_job' in ctx.node.\
            type_hierarchy
        credentials = ctx.instance.runtime_properties['credentials']

        client = SshClient(credentials['host'],
                           credentials['user'],
                           credentials['password'])

        # TODO(emepetres): use workload manager type
        is_clean = slurm.clean_job_aux_files(client,
                                             name,
                                             job_options,
                                             is_singularity,
                                             ctx.logger)

        client.close_connection()
    else:
        ctx.logger.warning('Instance ' + ctx.instance.id + ' simulated')
        is_clean = True

    if is_clean:
        ctx.logger.info('Job ' + name + ' (' + ctx.instance.id + ') cleaned.')
    else:
        ctx.logger.error('Job ' + name + ' (' + ctx.instance.id +
                         ') not cleaned.')


@operation
def stop_job(job_options, **kwargs):  # pylint: disable=W0613
    """ Stops a job in the HPC """
    simulate = ctx.instance.runtime_properties['simulate']

    credentials = ctx.instance.runtime_properties['credentials']
    name = kwargs['name']
    is_singularity = 'hpc.nodes.singularity_job' in ctx.node.\
        type_hierarchy

    if not simulate:
        client = SshClient(credentials['host'],
                           credentials['user'],
                           credentials['password'])

        # TODO(emepetres): use workload manager type
        is_stopped = slurm.stop_job(client,
                                    name,
                                    job_options,
                                    is_singularity,
                                    ctx.logger)

        client.close_connection()
    else:
        ctx.logger.warning('Instance ' + ctx.instance.id + ' simulated')
        is_stopped = True

    if is_stopped:
        ctx.logger.info('Job ' + name + ' (' + ctx.instance.id + ') stopped.')
    else:
        ctx.logger.error('Job ' + name + ' (' + ctx.instance.id +
                         ') not stopped.')
        raise NonRecoverableError('Job ' + name + ' (' + ctx.instance.id +
                                  ') not stopped.')
