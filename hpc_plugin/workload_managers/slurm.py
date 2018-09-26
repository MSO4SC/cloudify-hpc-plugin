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


from workload_manager import WorkloadManager, get_prevailing_state


class Slurm(WorkloadManager):

    def _build_container_script(self, name, job_settings, logger):
        # check input information correctness
        if not isinstance(job_settings, dict) or \
                not isinstance(name, basestring):
            logger.error("Singularity Script malformed")
            return None

        if 'image' not in job_settings or 'command' not in job_settings or\
                'max_time' not in job_settings:
            logger.error("Singularity Script malformed")
            return None

        script = '#!/bin/bash -l\n\n'
        script += self._parse_slurm_job_settings(name,
                                                 job_settings,
                                                 '#SBATCH', '\n')

        script += '\n# DYNAMIC VARIABLES\n\n'

        # load extra modules
        if 'modules' in job_settings and job_settings['modules']:
            script += 'module load {}\n'.format(
                ' '.join(job_settings['modules']))

        script += '\nmpirun singularity exec '

        if 'home' in job_settings and job_settings['home'] != '':
            script += '-H ' + job_settings['home'] + ' '

        if 'volumes' in job_settings:
            for volume in job_settings['volumes']:
                script += '-B ' + volume + ' '

        # add executable and arguments
        script += job_settings['image'] + ' ' + job_settings['command'] + '\n'

        return script

    def _build_job_submission_call(self, name, job_settings, logger):
        # check input information correctness
        if not isinstance(job_settings, dict) or \
                not isinstance(name, basestring):
            return {'error': "Incorrect inputs"}

        if 'type' not in job_settings or 'command' not in job_settings:
            return {'error': "'type' and 'command' " +
                    "must be defined in job settings"}

        # Build single line command
        slurm_call = ''

        # load extra modules
        if 'modules' in job_settings and job_settings['modules']:
            slurm_call = 'module load {}; '.format(
                ' '.join(job_settings['modules']))

        if job_settings['type'] == 'SBATCH':
            # sbatch command plus job name
            slurm_call += "sbatch --parsable -J '" + name + "'"
        elif job_settings['type'] == 'SRUN':
            slurm_call += "nohup srun -J '" + name + "'"
        else:
            return {'error': "Job type '" + job_settings['type'] +
                    "'not supported"}

        if 'max_time' not in job_settings and job_settings['type'] == 'SRUN':
            return {'error': "'SRUN' jobs must define the 'max_time' property"}

        slurm_call += self._parse_slurm_job_settings(name,
                                                     job_settings,
                                                     None, None)

        response = {}
        if 'scale' in job_settings and \
                int(job_settings['scale']) > 1:
            if job_settings['type'] == 'SRUN':
                return {'error': "'SRUN' does not allow scale property"}
            # set the max of parallel jobs
            scale_max = job_settings['scale']
            # set the job array
            slurm_call += ' --array=0-{}'.format(scale_max - 1)
            if 'scale_max_in_parallel' in job_settings and \
                    int(job_settings['scale_max_in_parallel']) > 0:
                slurm_call += '%' + str(job_settings['scale_max_in_parallel'])
                scale_max = job_settings['scale_max_in_parallel']
            # map the orchestrator variables after last sbatch
            scale_env_mapping_call = \
                "sed -i '/# DYNAMIC VARIABLES/a\\" +\
                "SCALE_INDEX=$SLURM_ARRAY_TASK_ID\\n" +\
                "SCALE_COUNT=$SLURM_ARRAY_TASK_COUNT\\n" +\
                "SCALE_MAX=" + str(scale_max) + "' " +\
                job_settings['command'].split()[0]  # get only the file
            response['scale_env_mapping_call'] = scale_env_mapping_call

        # add executable and arguments
        slurm_call += ' ' + job_settings['command']

        if job_settings['type'] == 'SRUN':
            slurm_call += ' &'

        response['call'] = slurm_call
        return response

    def _build_job_cancellation_call(self, name, job_settings, logger):
        return "scancel --name " + name

# Monitor
    def get_states(self, ssh_client, names, logger):
        # TODO(emepetres) set start time of consulting
        # (sacct only check current day)
        call = "sacct -n -o JobName,State -X -P --name=" + ','.join(names)
        output, exit_code = ssh_client.execute_shell_command(
            call,
            wait_result=True)

        states = {}
        if exit_code == 0:
            states = self._parse_sacct(output)

        return states

    def _parse_sacct(self, sacct_output):
        """ Parse two colums sacct entries into a dict """
        jobs = sacct_output.splitlines()
        parsed = {}
        if jobs and (len(jobs) > 1 or jobs[0] is not ''):
            for job in jobs:
                first, second = job.strip().split('|')
                if first in parsed:
                    parsed[first] = get_prevailing_state(parsed[first], second)
                else:
                    parsed[first] = second

        return parsed

    def _parse_slurm_job_settings(self, job_id, job_settings, prefix, suffix):
        _prefix = prefix if prefix else ''
        _suffix = suffix if suffix else ''
        _settings = ''

        # Check if exists and has content
        def check_job_settings_key(job_settings, key):
            return key in job_settings and str(job_settings[key]).strip()

        # Slurm settings
        if check_job_settings_key(job_settings, 'stderr_file'):
            _settings += _prefix + ' -e ' + \
                str(job_settings['stderr_file']) + _suffix
        else:
            _settings += _prefix + ' -e ' + \
                str(job_id + '.err') + _suffix

        if check_job_settings_key(job_settings, 'stdout_file'):
            _settings += _prefix + ' -o ' + \
                str(job_settings['stdout_file']) + _suffix
        else:
            _settings += _prefix + ' -o ' + \
                str(job_id + '.out') + _suffix

        if check_job_settings_key(job_settings, 'max_time'):
            _settings += _prefix + ' -t ' + \
                str(job_settings['max_time']) + _suffix

        if check_job_settings_key(job_settings, 'partition'):
            _settings += _prefix + ' -p ' + \
                str(job_settings['partition']) + _suffix

        if check_job_settings_key(job_settings, 'nodes'):
            _settings += _prefix + ' -N ' + \
                str(job_settings['nodes']) + _suffix

        if check_job_settings_key(job_settings, 'tasks'):
            _settings += _prefix + ' -n ' + \
                str(job_settings['tasks']) + _suffix

        if check_job_settings_key(job_settings, 'tasks_per_node'):
            _settings += _prefix + ' --ntasks-per-node=' + \
                str(job_settings['tasks_per_node']) + _suffix

        if check_job_settings_key(job_settings, 'memory'):
            _settings += _prefix + ' --mem=' + \
                str(job_settings['memory']) + _suffix

        if check_job_settings_key(job_settings, 'reservation'):
            _settings += _prefix + ' --reservation=' + \
                str(job_settings['reservation']) + _suffix

        if check_job_settings_key(job_settings, 'qos'):
            _settings += _prefix + ' --qos=' + \
                str(job_settings['qos']) + _suffix

        if check_job_settings_key(job_settings, 'mail_user'):
            _settings += _prefix + ' --mail-user=' + \
                str(job_settings['mail_user']) + _suffix

        if check_job_settings_key(job_settings, 'mail_type'):
            _settings += _prefix + ' --mail-type=' + \
                str(job_settings['mail_type']) + _suffix

        return _settings
