########
# Copyright (c) 2018 HLRS - hpcgogol@hlrs.de
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

""" Holds the Torque functions """
from workload_manager import WorkloadManager

from hpc_plugin.utilities import shlex_quote

class Torque(WorkloadManager):

    def _build_container_script(self, name, job_settings, logger):
        # check input information correctness
        if not isinstance(job_settings, dict) or\
                not isinstance(name, basestring):
            logger.error("Singularity Script malformed")
            return None

        if 'image' not in job_settings or 'command' not in job_settings or\
                'max_time' not in job_settings:
            logger.error("Singularity Script malformed")
            return None

        script = '#!/bin/bash -l\n\n'
        # script += '#PBS -N "' + name + '"\n'

        # Torque settings
        if 'nodes' in job_settings:
            resources_request = "nodes={}".format(job_settings['nodes'])

            if 'tasks_per_node' in job_settings:
                resources_request += ':ppn={}'.format(job_settings['tasks_per_node'])

            script += '#PBS -l walltime={}\n'.format(resources_request)
        else:
            if 'tasks_per_node' in job_settings:
                logger.error(r"Specify 'tasks_per_node' while 'nodes' is not specified")

        # if 'tasks' in job_settings:
        #     script += '#qsub -n ' + str(job_settings['tasks']) + '\n'

        script += '#PBS -l walltime={}\n'.format(job_settings['max_time'])
        script += '\n'

        # load extra modules
        if 'modules' in job_settings:
            script += 'module load {}\n\n'.format(' '.join(job_settings['modules']))

        script += 'mpirun singularity exec '

        # TODO: why not cd job_settings['home']?
        if 'home' in job_settings and job_settings['home'] != '':
            script += '-H ' + job_settings['home'] + ' '

        if 'volumes' in job_settings:
            for volume in job_settings['volumes']:
                script += '-B ' + volume + ' '

        # add executable and arguments
        script += job_settings['image'] + ' ' + job_settings['command'] + '\n'

        # disable output
        # script += ' >/dev/null 2>&1';

        return script

    def _build_job_submission_call(self, name, job_settings, logger):
        # basic checks for validity of input
        if not isinstance(job_settings, dict) or\
                not isinstance(name, basestring):
            return {'error': "Incorrect inputs"}

        if 'type' not in job_settings or 'command' not in job_settings:
            return {'error': "'type' and 'command' " +
                    "must be defined in job settings"}

        # Build single line command
        torque_call = ''

        # load extra modules
        if 'modules' in job_settings:
            torque_call = 'module load {}; '.format(' '.join(job_settings['modules']))

        # Torque settings
        if job_settings['type'] == 'SBATCH':
            # qsub command plus job name
            torque_call += "qsub -V -N {}".format(shlex_quote(name))
        else:
            return {'error': "Job type '" + job_settings['type'] +
                    "'not supported"}

        resources_request = ""
        if 'nodes' in job_settings:
            resources_request = "nodes={}".format(job_settings['nodes'])

            if 'tasks_per_node' in job_settings:
                resources_request += ':ppn={}'.format(job_settings['tasks_per_node'])
        else:
            if 'tasks_per_node' in job_settings:
                logger.error(r"Specify 'tasks_per_node' while 'nodes' is not specified")

        if 'max_time' in job_settings:
            if len(resources_request) > 0: resources_request +=','
            resources_request += 'walltime={}'.format(job_settings['max_time'])

        if len(resources_request) > 0:
            torque_call += ' -l {}'.format(resources_request)

        if 'queue' in job_settings: # more precisely is it a destination [queue][@server]
            torque_call += " -q {}".format(shlex_quote(job_settings['queue']))

        if 'rerunable' in job_settings:
            torque_call += " -r {}".format('y' if job_settings['rerunable'] else 'n')

        if 'working_directory' in job_settings:
            torque_call += " -w {}".format(shlex_quote(job_settings['working_directory']))

        additional_attributes = {}
        if 'groupname' in job_settings:
            additional_attributes["group_list"]=shlex_quote(job_settings['groupname'])

        if len(additional_attributes) > 0:
            torque_call += " -W {}".format(','.join("{0}={1}".format(k,v)\
                for k, v in additional_attributes.items()))

        # if 'tasks' in job_settings:
        #     torque_call += ' -n ' + str(job_settings['tasks'])

        # add executable and arguments
        torque_call += ' ' + job_settings['command']

        # disable output
        # torque_call += ' >/dev/null 2>&1';

        return {'call': torque_call}

    def _build_job_cancellation_call(self, name, job_settings, logger):
        return r"qselect -N {} | xargs qdel".format(shlex_quote(name))

# Monitor

    def _get_jobids_by_name(self, ssh_client, job_names):
        """
        Get JobID from qstat command

        This function uses qstat command to query Torque. In this case Torque
        strongly recommends that the code should performs these queries once
        every 60 seconds or longer. Using these commands contacts the master
        controller directly, the same process responsible for scheduling all
        work on the cluster. Polling more frequently, especially across all
        users on the cluster, will slow down response times and may bring
        scheduling to a crawl. Please don't.
        """
        # TODO(emepetres) set first day of consulting
        # (qstat only check current day)
        call = "qstat -i `echo {} | xargs -n 1 qselect -N` | tail -n+6 | awk '{{ print $4 \" \" $1 }}'".\
            format( shlex_quote(' '.join(map(shlex_quote, job_names))) )
        output, exit_code = self._execute_shell_command(ssh_client,
                                                        call,
                                                        wait_result=True)

        ids = {}
        if exit_code == 0:
            ids = self.parse_qstat(output)

        return ids

    def _get_status(self, ssh_client, job_ids):
        """
        Get Status from qstat command

        This function uses qstat command to query Torque. In this case Torque
        strongly recommends that the code should performs these queries once
        every 60 seconds or longer. Using these commands contacts the master
        controller directly, the same process responsible for scheduling all
        work on the cluster. Polling more frequently, especially across all
        users on the cluster, will slow down response times and may bring
        scheduling to a crawl. Please don't.
        """
        # TODO(emepetres) set first day of consulting
        # (qstat only checks current day)
        call = "qstat -i {} | tail -n+6 | awk '{{ print $1 \" \" $10 }}'".\
            format( ' '.join(job_ids) )
        output, exit_code = self._execute_shell_command(ssh_client,
                                                        call,
                                                        wait_result=True)

        states = {}
        if exit_code == 0:
            states = self.parse_qstat(output)

        return states

    def parse_qstat(self, qstat_output):
        """ Parse two colums qstat entries into a dict """
        jobs = qstat_output.splitlines()
        parsed = {}
        if jobs and (len(jobs) > 1 or jobs[0] is not ''):
            for job in jobs:
                first, second = job.strip().split()
                parsed[first] = second

        return parsed
