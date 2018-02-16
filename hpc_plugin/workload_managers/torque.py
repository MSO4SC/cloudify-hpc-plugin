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

    @staticmethod
    def _build_container_script(name, job_settings, logger):
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
            script += '-H {} '.format(job_settings['home'])

        if 'volumes' in job_settings:
            for volume in job_settings['volumes']:
                script += '-B {} '.format(volume)

        # add executable and arguments
        script += "{image} {command}\n".format(image=job_settings['image'], command=job_settings['command'])

        # disable output
        # script += ' >/dev/null 2>&1';

        return script

    @staticmethod
    def _build_job_submission_call(name, job_settings, logger):
        # basic checks for validity of input
        if not isinstance(job_settings, dict) or\
                not isinstance(name, basestring):
            return {'error': "Incorrect inputs"}

        if 'type' not in job_settings or 'command' not in job_settings:
            return {'error': "'type' and 'command' " +
                    "must be defined in job settings"}

        if 'type' in job_settings and job_settings['type'] != 'SBATCH':
            return {'error': "Job type '" + job_settings['type'] +
                    "'not supported. Torque support only batched jobs."}

        # Build single line command
        torque_call = ''

        # load extra modules
        if 'modules' in job_settings:
            torque_call = 'module load {}; '.format(' '.join(job_settings['modules']))

        # Torque settings
        # qsub command plus job name
        torque_call += "qsub -V -N {}".format(shlex_quote(name))

        resources_request = ""
        if 'nodes' in job_settings:
            resources_request = "nodes={}".format(job_settings['nodes'])

            # number of cores requested per node
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

        if 'rerunnable' in job_settings: # same to requeue in SLURM
            torque_call += " -r {}".format('y' if job_settings['rerunnable'] else 'n')

        if 'work_dir' in job_settings:
            torque_call += " -w {}".format(shlex_quote(job_settings['work_dir']))

        additional_attributes = {}
        if 'group_name' in job_settings:
            additional_attributes["group_list"]=shlex_quote(job_settings['group_name'])

        if len(additional_attributes) > 0:
            torque_call += " -W {}".format(','.join("{0}={1}".format(k,v)\
                for k, v in additional_attributes.iteritems()))

        # if 'tasks' in job_settings:
        #     torque_call += ' -n ' + str(job_settings['tasks'])

        response = {}
        if 'scale' in job_settings and \
                job_settings['scale'] > 1:
            scale_max = job_settings['scale']
            # set the job array
            torque_call += ' -J 0-{}'.format(scale_max - 1)
            # set the max of parallel jobs
            if 'scale_max_in_parallel' in job_settings and \
                    job_settings['scale_max_in_parallel'] > 0:
                torque_call += '%{}'.format(job_settings['scale_max_in_parallel'])
                scale_max = job_settings['scale_max_in_parallel']
            # map the orchestrator variables after last sbatch
            scale_env_mapping_call = \
                "sed -i ':a;N;$! ba;s/\\n.*#SBATCH.*\\n/&" \
                "SCALE_INDEX=$PBS_ARRAYID\\n" \
                "SCALE_COUNT=" + str(scale_max) + "\\n" \
                "SCALE_MAX=" + str(scale_max) + "\\n\\n/' " + \
                job_settings['command'].split()[0]  # get only the file
            response['scale_env_mapping_call'] = scale_env_mapping_call

        # add executable and arguments
        torque_call += ' {}'.format(job_settings['command'])

        # disable output
        # torque_call += ' >/dev/null 2>&1';

        return {'call': torque_call}

    def _build_job_cancellation_call(self, name, job_settings, logger):
        return r"qselect -N {} | xargs qdel".format(shlex_quote(name))

# Monitor

    def get_states(self, ssh_client, names, logger):
        """
        Get job states by job names

        This function uses `qstat` command to query Torque.
        Please don't launch this call very friquently. Polling it
        frequently, especially across all users on the cluster,
        will slow down response times and may bring
        scheduling to a crawl.
        """
        # TODO(emepetres) set start day of consulting
        call = "qstat -i `echo {} | xargs -n 1 qselect -N` | tail -n+6 | awk '{{ print $4 \"|\" $10 }}'".\
            format( shlex_quote(' '.join(map(shlex_quote, job_names))) )
        output, exit_code = self._execute_shell_command(ssh_client,
                                                        call,
                                                        wait_result=True)

        if exit_code == 0:
            # @TODO: use full parsing of `qstat` tabular output without `tail/awk` preprocessing on the remote HPC
            return self._parse_qstat(output)
        else:
            return {}

    @staticmethod
    def _parse_qstat(qstat_output):
        """ Parse two colums `qstat` entries into a dict """
        jobs = qstat_output.splitlines()
        parsed = {}
        if jobs and (len(jobs) > 1 or jobs[0] is not ''):
            parsed = dict(map(str.strip, job.split('|')) for job in jobs)

        return parsed
