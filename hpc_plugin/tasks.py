########
# Copyright (c) 2017-2018 MSO4SC - javier.carnero@atos.net
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
import traceback
from cloudify import ctx
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

from ssh import SshClient
from workload_managers.workload_manager import WorkloadManager
from external_repositories.external_repository import ExternalRepository


@operation
def preconfigure_wm(config,
                    simulate,
                    **kwargs):  # pylint: disable=W0613
    """ Get workload manager config from infrastructure """
    ctx.logger.info('Preconfiguring workload manager..')

    if not simulate:
        credentials = config['credentials']
        credentials_modified = False

        if 'ip' in ctx.target.instance.runtime_properties:
            credentials['host'] = \
                ctx.target.instance.runtime_properties['ip']
            credentials_modified = True

        for rel in ctx.target.instance.relationships:
            node = rel.target.node
            if node.type == 'cloudify.openstack.nodes.KeyPair':
                if 'private_key_path' in node.properties:
                    with open(node.properties['private_key_path'], 'r') \
                            as keyfile:
                        private_key = keyfile.read()
                        credentials['private_key'] = private_key
                        credentials_modified = True

        if credentials_modified:
            ctx.source.instance.runtime_properties['credentials'] = \
                credentials

        if 'networks' in ctx.source.instance.runtime_properties:
            ctx.source.instance.runtime_properties['networks'] = \
                ctx.target.instance.runtime_properties['networks']
        else:
            ctx.source.instance.runtime_properties['networks'] = {}
        ctx.logger.info('..preconfigured ')
    else:
        ctx.logger.warning('Workload manager simulated')


@operation
def configure_execution(config,
                        base_dir,
                        workdir_prefix,
                        simulate,
                        **kwargs):  # pylint: disable=W0613
    """ Creates the working directory for the execution """
    ctx.logger.info('Connecting to workload manager..')
    if not simulate:
        wm_type = config['workload_manager']
        ctx.logger.info(' - manager: {wm_type}'.format(wm_type=wm_type))

        wm = WorkloadManager.factory(wm_type)
        if not wm:
            raise NonRecoverableError(
                "Workload Manager '" +
                wm_type +
                "' not supported.")

        credentials = config['credentials']
        if 'credentials' in ctx.instance.runtime_properties:
            credentials = ctx.instance.runtime_properties['credentials']
        try:
            client = SshClient(credentials)
        except Exception as exp:
            raise NonRecoverableError(
                "Failed trying to connect to workload manager: " + str(exp))

        # TODO: use command according to wm
        _, exit_code = client.execute_shell_command(
            'uname',
            wait_result=True)

        if exit_code is not 0:
            client.close_connection()
            raise NonRecoverableError(
                "Failed executing on the workload manager: exit code " +
                str(exit_code))

        ctx.instance.runtime_properties['login'] = exit_code is 0

        prefix = workdir_prefix
        if workdir_prefix is "":
            prefix = ctx.blueprint.id

        workdir = wm.create_new_workdir(client, base_dir, prefix)
        client.close_connection()
        if workdir is None:
            raise NonRecoverableError(
                "failed to create the working directory, base dir: " +
                base_dir)
        ctx.instance.runtime_properties['workdir'] = workdir
        ctx.logger.info('..workload manager ready to be used on ' + workdir)
    else:
        ctx.logger.info(' - [simulation]..')
        ctx.instance.runtime_properties['login'] = True
        ctx.instance.runtime_properties['workdir'] = "simulation"
        ctx.logger.warning('Workload manager connection simulated')


@operation
def cleanup_execution(config,
                      skip,
                      simulate,
                      **kwargs):  # pylint: disable=W0613
    """ Cleans execution working directory """
    if skip:
        return

    ctx.logger.info('Cleaning up...')
    if not simulate:
        workdir = ctx.instance.runtime_properties['workdir']
        wm_type = config['workload_manager']
        wm = WorkloadManager.factory(wm_type)
        if not wm:
            raise NonRecoverableError(
                "Workload Manager '" +
                wm_type +
                "' not supported.")

        credentials = config['credentials']
        if 'credentials' in ctx.instance.runtime_properties:
            credentials = ctx.instance.runtime_properties['credentials']
        client = SshClient(credentials)
        _, exit_code = client.execute_shell_command(
            'rm -r ' + workdir,
            wait_result=True)
        client.close_connection()
        ctx.logger.info('..all clean.')
    else:
        ctx.logger.warning('clean up simulated.')


@operation
def start_monitoring_hpc(config,
                         external_monitor_entrypoint,
                         external_monitor_port,
                         external_monitor_orchestrator_port,
                         simulate,
                         **kwargs):  # pylint: disable=W0613
    """ Starts monitoring using the Monitor orchestrator """
    external_monitor_entrypoint = None  # FIXME: external monitor disabled
    if external_monitor_entrypoint:
        ctx.logger.info('Starting infrastructure monitor..')

        if not simulate:
            credentials = config['credentials']
            if 'credentials' in ctx.instance.runtime_properties:
                credentials = ctx.instance.runtime_properties['credentials']
            workload_manager = config['workload_manager']
            country_tz = config['country_tz']

            url = 'http://' + external_monitor_entrypoint + \
                external_monitor_orchestrator_port + '/exporters/add'

            # FIXME: credentials doesn't have to have a password anymore
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
    external_monitor_entrypoint = None  # FIXME: external monitor disabled
    if external_monitor_entrypoint:
        ctx.logger.info('Stoping infrastructure monitor..')

        if not simulate:
            credentials = config['credentials']
            if 'credentials' in ctx.instance.runtime_properties:
                credentials = ctx.instance.runtime_properties['credentials']
            workload_manager = config['workload_manager']
            country_tz = config['country_tz']

            url = 'http://' + external_monitor_entrypoint + \
                external_monitor_orchestrator_port + '/exporters/remove'

            # FIXME: credentials doesn't have to have a password anymore
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
def preconfigure_job(config,
                     external_monitor_entrypoint,
                     external_monitor_port,
                     external_monitor_type,
                     external_monitor_orchestrator_port,
                     job_prefix,
                     monitor_period,
                     simulate,
                     **kwargs):  # pylint: disable=W0613
    """ Set the job with the HPC credentials """
    ctx.logger.info('Preconfiguring HPC job..')

    if 'credentials' not in ctx.target.instance.runtime_properties:
        ctx.source.instance.runtime_properties['credentials'] = \
            config['credentials']
    else:
        ctx.source.instance.runtime_properties['credentials'] = \
            ctx.target.instance.runtime_properties['credentials']

    ctx.source.instance.runtime_properties['external_monitor_entrypoint'] = \
        external_monitor_entrypoint
    ctx.source.instance.runtime_properties['external_monitor_port'] = \
        external_monitor_port
    ctx.source.instance.runtime_properties['external_monitor_type'] = \
        external_monitor_type
    ctx.source.instance.runtime_properties['monitor_orchestrator_port'] = \
        external_monitor_orchestrator_port
    ctx.source.instance.runtime_properties['workload_manager'] = \
        config['workload_manager']
    ctx.source.instance.runtime_properties['simulate'] = simulate
    ctx.source.instance.runtime_properties['job_prefix'] = job_prefix
    ctx.source.instance.runtime_properties['monitor_period'] = monitor_period

    ctx.source.instance.runtime_properties['workdir'] = \
        ctx.target.instance.runtime_properties['workdir']


@operation
def bootstrap_job(deployment,
                  skip_cleanup,
                  **kwarsgs):  # pylint: disable=W0613
    """Bootstrap a job with a script that receives SSH credentials as imput"""
    if not deployment:
        return

    ctx.logger.info('Bootstraping job..')
    simulate = ctx.instance.runtime_properties['simulate']

    if not simulate and 'bootstrap' in deployment:
        inputs = deployment['inputs'] if 'inputs' in deployment else []
        credentials = ctx.instance.runtime_properties['credentials']
        workdir = ctx.instance.runtime_properties['workdir']
        name = "bootstrap_" + ctx.instance.id + ".sh"
        wm_type = ctx.instance.runtime_properties['workload_manager']

        is_bootstraped = deploy_job(
            deployment['bootstrap'],
            inputs,
            credentials,
            wm_type,
            workdir,
            name,
            ctx.logger,
            skip_cleanup)
    else:
        is_bootstraped = True

    if is_bootstraped:
        ctx.logger.info('..job bootstraped')
    else:
        ctx.logger.error('Job not bootstraped.')
        raise NonRecoverableError("Bootstrap failed")


@operation
def revert_job(deployment, skip_cleanup, **kwarsgs):  # pylint: disable=W0613
    """Revert a job using a script that receives SSH credentials as input"""
    if not deployment:
        return

    ctx.logger.info('Reverting job..')
    try:
        simulate = ctx.instance.runtime_properties['simulate']

        if not simulate and 'revert' in deployment:
            inputs = deployment['inputs'] if 'inputs' in deployment else []
            credentials = ctx.instance.runtime_properties['credentials']
            workdir = ctx.instance.runtime_properties['workdir']
            name = "revert_" + ctx.instance.id + ".sh"
            wm_type = ctx.instance.runtime_properties['workload_manager']

            is_reverted = deploy_job(
                deployment['revert'],
                inputs,
                credentials,
                wm_type,
                workdir,
                name,
                ctx.logger,
                skip_cleanup)
        else:
            is_reverted = True

        if is_reverted:
            ctx.logger.info('..job reverted')
        else:
            ctx.logger.error('Job could not be reverted.')
    except KeyError:
        # The job wasn't configured properly, so there was no bootstrap
        ctx.logger.warning('Job was not reverted as it was not configured.')


def deploy_job(script,
               inputs,
               credentials,
               wm_type,
               workdir,
               name,
               logger,
               skip_cleanup):  # pylint: disable=W0613
    """ Exec a eployment job script that receives SSH credentials as input """

    wm = WorkloadManager.factory(wm_type)
    if not wm:
        raise NonRecoverableError(
            "Workload Manager '" +
            wm_type +
            "' not supported.")

    # Execute the script and manage the output
    client = SshClient(credentials)
    if wm._create_shell_script(client,
                               name,
                               ctx.get_resource(script),
                               logger,
                               workdir=workdir):
        call = "./" + name
        for dinput in inputs:
            if ('\n' in dinput or ' ' in dinput) and dinput[0] != '"':
                call += ' "' + dinput + '"'
            else:
                call += ' ' + dinput
        _, exit_code = client.execute_shell_command(
            call,
            workdir=workdir,
            wait_result=True)
        if exit_code is not 0:
            logger.warning(
                "failed to deploy job: call '" + call + "', exit code " +
                str(exit_code))

        if not skip_cleanup:
            if not client.execute_shell_command(
                    "rm " + name,
                    workdir=workdir):
                logger.warning("failed removing bootstrap script")

    client.close_connection()

    return exit_code is 0


@operation
def send_job(job_options, **kwargs):  # pylint: disable=W0613
    """ Sends a job to the HPC """
    simulate = ctx.instance.runtime_properties['simulate']

    name = kwargs['name']
    is_singularity = 'hpc.nodes.SingularityJob' in ctx.node.\
        type_hierarchy

    if not simulate:
        workdir = ctx.instance.runtime_properties['workdir']
        wm_type = ctx.instance.runtime_properties['workload_manager']
        client = SshClient(ctx.instance.runtime_properties['credentials'])

        wm = WorkloadManager.factory(wm_type)
        if not wm:
            client.close_connection()
            raise NonRecoverableError(
                "Workload Manager '" +
                wm_type +
                "' not supported.")
        context_vars = {
            'CFY_EXECUTION_ID': ctx.execution_id,
            'CFY_JOB_NAME': name
        }
        is_submitted = wm.submit_job(client,
                                     name,
                                     job_options,
                                     is_singularity,
                                     ctx.logger,
                                     workdir=workdir,
                                     context=context_vars)
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
def cleanup_job(job_options, skip, **kwargs):  # pylint: disable=W0613
    """Clean the aux files of the job in the HPC"""
    if skip:
        return

    try:
        simulate = ctx.instance.runtime_properties['simulate']
    except KeyError:
        # The job wasn't configured properly, so no cleanup needed
        ctx.logger.warning('Job was not cleaned up as it was not configured.')

    try:
        name = kwargs['name']
        if not simulate:
            is_singularity = 'hpc.nodes.SingularityJob' in ctx.node.\
                type_hierarchy
            workdir = ctx.instance.runtime_properties['workdir']
            wm_type = ctx.instance.runtime_properties['workload_manager']

            client = SshClient(ctx.instance.runtime_properties['credentials'])

            wm = WorkloadManager.factory(wm_type)
            if not wm:
                client.close_connection()
                raise NonRecoverableError(
                    "Workload Manager '" +
                    wm_type +
                    "' not supported.")
            is_clean = wm.clean_job_aux_files(client,
                                              name,
                                              job_options,
                                              is_singularity,
                                              ctx.logger,
                                              workdir=workdir)

            client.close_connection()
        else:
            ctx.logger.warning('Instance ' + ctx.instance.id + ' simulated')
            is_clean = True

        if is_clean:
            ctx.logger.info(
                'Job ' + name + ' (' + ctx.instance.id + ') cleaned.')
        else:
            ctx.logger.error('Job ' + name + ' (' + ctx.instance.id +
                             ') not cleaned.')
    except Exception as exp:
        print(traceback.format_exc())
        ctx.logger.error(
            'Something happend when trying to clean up: ' + exp.message)


@operation
def stop_job(job_options, **kwargs):  # pylint: disable=W0613
    """ Stops a job in the HPC """
    try:
        simulate = ctx.instance.runtime_properties['simulate']
    except KeyError:
        # The job wasn't configured properly, no need to be stopped
        ctx.logger.warning('Job was not stopped as it was not configured.')

    try:
        name = kwargs['name']
        is_singularity = 'hpc.nodes.SingularityJob' in ctx.node.\
            type_hierarchy

        if not simulate:
            workdir = ctx.instance.runtime_properties['workdir']
            wm_type = ctx.instance.runtime_properties['workload_manager']
            client = SshClient(ctx.instance.runtime_properties['credentials'])

            wm = WorkloadManager.factory(wm_type)
            if not wm:
                client.close_connection()
                raise NonRecoverableError(
                    "Workload Manager '" +
                    wm_type +
                    "' not supported.")
            is_stopped = wm.stop_job(client,
                                     name,
                                     job_options,
                                     is_singularity,
                                     ctx.logger,
                                     workdir=workdir)

            client.close_connection()
        else:
            ctx.logger.warning('Instance ' + ctx.instance.id + ' simulated')
            is_stopped = True

        if is_stopped:
            ctx.logger.info(
                'Job ' + name + ' (' + ctx.instance.id + ') stopped.')
        else:
            ctx.logger.error('Job ' + name + ' (' + ctx.instance.id +
                             ') not stopped.')
            raise NonRecoverableError('Job ' + name + ' (' + ctx.instance.id +
                                      ') not stopped.')
    except Exception as exp:
        print(traceback.format_exc())
        ctx.logger.error(
            'Something happend when trying to stop: ' + exp.message)


@operation
def publish(publish_options, **kwargs):
    """ Publish the job outputs """
    try:
        simulate = ctx.instance.runtime_properties['simulate']
    except KeyError as exp:
        # The job wasn't configured properly, no need to publish
        ctx.logger.warning(
            'Job outputs where not published as' +
            ' the job was not configured properly.')
        return

    try:
        name = kwargs['name']
        published = True
        if not simulate:
            workdir = ctx.instance.runtime_properties['workdir']
            client = SshClient(ctx.instance.runtime_properties['credentials'])

            for publish_item in publish_options:
                if not published:
                    break
                er = ExternalRepository.factory(publish_item)
                if not er:
                    client.close_connection()
                    raise NonRecoverableError(
                        "External repository '" +
                        publish_item['type'] +
                        "' not supported.")
                published = er.publish(client, ctx.logger, workdir)

            client.close_connection()
        else:
            ctx.logger.warning('Instance ' + ctx.instance.id + ' simulated')

        if published:
            ctx.logger.info(
                'Job ' + name + ' (' + ctx.instance.id + ') published.')
        else:
            ctx.logger.error('Job ' + name + ' (' + ctx.instance.id +
                             ') not published.')
            raise NonRecoverableError('Job ' + name + ' (' + ctx.instance.id +
                                      ') not published.')
    except Exception as exp:
        print(traceback.format_exc())
        ctx.logger.error(
            'Cannot publish: ' + exp.message)
