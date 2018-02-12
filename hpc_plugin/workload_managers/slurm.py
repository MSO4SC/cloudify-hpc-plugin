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

""" Holds the slurm functions """
from workload_manager import WorkloadManager


class Slurm(WorkloadManager):

    def _build_container_script(self, name, job_settings, logger):
        # check input information correctness
        if not isinstance(job_settings, dict) or not isinstance(name,
                                                                basestring):
            logger.error("Singularity Script malformed")
            return None

        if 'image' not in job_settings or 'command' not in job_settings or\
                'max_time' not in job_settings:
            logger.error("Singularity Script malformed")
            return None

        script = '#!/bin/bash -l\n\n'
        # script += '#SBATCH --parsable\n'
        # script += '#SBATCH -J "' + name + '"\n'

        # Slurm settings
        if 'partition' in job_settings:
            script += '#SBATCH -p ' + job_settings['partition'] + '\n'

        if 'nodes' in job_settings:
            script += '#SBATCH -N ' + str(job_settings['nodes']) + '\n'

        if 'tasks' in job_settings:
            script += '#SBATCH -n ' + str(job_settings['tasks']) + '\n'

        if 'tasks_per_node' in job_settings:
            script += '#SBATCH --ntasks-per-node=' + \
                str(job_settings['tasks_per_node']) + '\n'

        if 'reservation' in job_settings:
            script += '#SBATCH --reservation=' + \
                str(job_settings['reservation']) + '\n'

        script += '#SBATCH -t ' + job_settings['max_time'] + '\n'

        script += '\n'

        # first set modules
        if 'modules' in job_settings:
            script += 'module load'
            for module in job_settings['modules']:
                script += ' ' + module
            script += '\n'

        script += '\nmpirun singularity exec '

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
        # check input information correctness
        if not isinstance(job_settings, dict) or not isinstance(name,
                                                                basestring):
            return {'error': "Incorrect inputs"}

        if 'type' not in job_settings or 'command' not in job_settings:
            return {'error': "'type' and 'command' " +
                    "must be defined in job settings"}

        # first set modules
        slurm_call = ''
        if 'modules' in job_settings:
            slurm_call += 'module load'
            for module in job_settings['modules']:
                slurm_call += ' ' + module
            slurm_call += '; '

        if job_settings['type'] == 'SBATCH':
            # sbatch command plus job name
            slurm_call += "sbatch --parsable -J '" + name + "'"
        elif job_settings['type'] == 'SRUN':
            slurm_call += "nohup srun -J '" + name + "'"
        else:
            return {'error': "Job type '" + job_settings['type'] +
                    "'not supported"}

        # Slurm settings
        if 'partition' in job_settings:
            slurm_call += ' -p ' + job_settings['partition']

        if 'nodes' in job_settings:
            slurm_call += ' -N ' + str(job_settings['nodes'])

        if 'tasks' in job_settings:
            slurm_call += ' -n ' + str(job_settings['tasks'])

        if 'tasks_per_node' in job_settings:
            slurm_call += ' --ntasks-per-node=' + \
                str(job_settings['tasks_per_node'])

        if 'reservation' in job_settings:
            slurm_call += ' --reservation=' + \
                str(job_settings['reservation'])

        if 'max_time' in job_settings:
            slurm_call += ' -t ' + job_settings['max_time']
        elif job_settings['type'] == 'SRUN':
            return {'error': "'SRUN' jobs must define the 'max_time' property"}

        response = {}
        if 'scale' in job_settings and \
                job_settings['scale'] > 1:
            if job_settings['type'] == 'SRUN':
                return {'error': "'SRUN' does not allow scale property"}
            # set the job array
            slurm_call += ' --array=0-' + str(job_settings['scale'] - 1)
            # set the max of parallel jobs
            scale_max = job_settings['scale']
            if 'scale_max_in_parallel' in job_settings and \
                    job_settings['scale_max_in_parallel'] > 0:
                slurm_call += '%' + str(job_settings['scale_max_in_parallel'])
                scale_max = job_settings['scale_max_in_parallel']
            # map the orchestrator variables after last sbatch
            scale_env_mapping_call = \
                "sed -i ':a;N;$! ba;s/\\n.*#SBATCH.*\\n/&" +\
                "SCALE_INDEX=$SLURM_ARRAY_TASK_ID\\n" +\
                "SCALE_COUNT=$SLURM_ARRAY_TASK_COUNT\\n" +\
                "SCALE_MAX=" + str(scale_max) + "\\n\\n/' " +\
                job_settings['command'].split()[0]  # get only the file
            response['scale_env_mapping_call'] = scale_env_mapping_call
            print scale_env_mapping_call

        # add executable and arguments
        slurm_call += ' ' + job_settings['command']

        # disable output
        # slurm_call += ' >/dev/null 2>&1';

        if job_settings['type'] == 'SRUN':
            slurm_call += ' &'

        response['call'] = slurm_call
        return response

    def _build_job_cancellation_call(self, name, job_settings, logger):
        return "scancel --name " + name

# Monitor

    def _get_jobids_by_name(self, ssh_client, job_names):
        """
        Get JobID from sacct command

        This function uses sacct command to query Slurm. In this case Slurm
        strongly recommends that the code should performs these queries once
        every 60 seconds or longer. Using these commands contacts the master
        controller directly, the same process responsible for scheduling all
        work on the cluster. Polling more frequently, especially across all
        users on the cluster, will slow down response times and may bring
        scheduling to a crawl. Please don't.
        """
        # TODO(emepetres) set first day of consulting
        # (sacct only check current day)
        # DOCUMENTS for further changes
        # sacct -n -o jobname%32,jobid -X -P --name=mso_zse43j
        # mso_zse43j|930223_1
        # mso_zse43j|930223_0
        # sacct -n -o jobname%32,jobIDRaw -X -P --name=mso_zse43j
        # mso_zse43j|930223
        # mso_zse43j|930226
        call = "sacct -n -o jobname%32,jobid -X --name=" + \
            ','.join(job_names)
        output, exit_code = self._execute_shell_command(ssh_client,
                                                        call,
                                                        wait_result=True)

        ids = {}
        if exit_code == 0:
            ids = self.parse_sacct(output)

        return ids

    def _get_status(self, ssh_client, job_ids):
        """
        Get Status from sacct command

        This function uses sacct command to query Slurm. In this case Slurm
        strongly recommends that the code should performs these queries once
        every 60 seconds or longer. Using these commands contacts the master
        controller directly, the same process responsible for scheduling all
        work on the cluster. Polling more frequently, especially across all
        users on the cluster, will slow down response times and may bring
        scheduling to a crawl. Please don't.
        """
        # TODO(emepetres) set first day of consulting
        # (sacct only check current day)
        call = "sacct -n -o jobid,state -X --jobs=" + ','.join(job_ids)
        output, exit_code = self._execute_shell_command(ssh_client,
                                                        call,
                                                        wait_result=True)

        states = {}
        if exit_code == 0:
            states = self.parse_sacct(output)

        return states

    def parse_sacct(self, sacct_output):
        """ Parse two colums sacct entries into a dict """
        jobs = sacct_output.splitlines()
        parsed = {}
        if jobs and (len(jobs) > 1 or jobs[0] is not ''):
            for job in jobs:
                first, second = job.strip().split()
                parsed[first] = second

        return parsed
