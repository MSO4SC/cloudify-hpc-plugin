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

    # Check if exists and has content
    def check_job_settings_key(self, job_settings, key):
        return key in job_settings and str(job_settings[key]).strip()

    def _parse_slurm_job_settings(self, job_id, job_settings, prefix, suffix, logger):

        _prefix = prefix if prefix else ''
        _suffix = suffix if suffix else ''
        _settings = ''

        # Slurm settings
        if self.check_job_settings_key(job_settings, 'stderr_file'):
            _settings += _prefix + ' -e ' + str(job_settings['stderr_file']) + _suffix
        else:
            _settings += _prefix + ' -e ' + str(job_id + '.err') + _suffix

        if self.check_job_settings_key(job_settings, 'stdout_file'):
            _settings += _prefix + ' -o ' + str(job_settings['stdout_file']) + _suffix
        else:
            _settings += _prefix + ' -e ' + str(job_id + '.out') + _suffix

        if self.check_job_settings_key(job_settings, 'max_time'):
            _settings += _prefix + ' -t ' + str(job_settings['max_time']) + _suffix

        if self.check_job_settings_key(job_settings, 'partition'):
            _settings += _prefix + ' -p ' +  str(job_settings['partition']) + _suffix

        if self.check_job_settings_key(job_settings, 'nodes'):
            _settings += _prefix + ' -N ' +  str(job_settings['nodes']) + _suffix

        if self.check_job_settings_key(job_settings, 'tasks'):
            _settings += _prefix + ' -n ' + str(job_settings['tasks']) + _suffix

        if self.check_job_settings_key(job_settings, 'tasks_per_node'):
            _settings += _prefix + ' --ntasks-per-node=' + str(job_settings['tasks_per_node']) + _suffix

        if self.check_job_settings_key(job_settings, 'memory'):
            _settings += _prefix + ' --mem=' + str(job_settings['memory']) + _suffix

        if self.check_job_settings_key(job_settings, 'reservation'):
            _settings += _prefix + ' --reservation=' + str(job_settings['reservation']) + _suffix

        if self.check_job_settings_key(job_settings, 'qos'):
            _settings += _prefix + ' --qos=' + str(job_settings['qos']) + _suffix

        if self.check_job_settings_key(job_settings, 'mail_user'):
            _settings += _prefix + ' --mail-user=' + str(job_settings['mail_user']) + _suffix

        if self.check_job_settings_key(job_settings, 'mail_type'):
            _settings += _prefix + ' --mail-type=' + str(job_settings['mail_type']) + _suffix
        # logger.info("settings=%s"%_settings)

        return _settings

    def _build_container_script(self, name, job_settings, logger):
        # check input information correctness
        # logger.info("----_slurm: _build_container_script-----------")
        # logger.info("job_settings:" )
        # for key, value in job_settings.iteritems() :
        #     logger.info("job_settings[%s]=%s"%(key, str(value)) )

        # logger.info("check job_settings type:%s"%isinstance(job_settings, dict) )
        # logger.info("name=%s is_string=%s"%(name,isinstance(name, basestring)) )
        if not isinstance(job_settings, dict) or not isinstance(name, basestring):
            logger.error("Singularity Script malformed")
            return None

        # logger.info("check for image and command key in job_settings" )
        if 'image' not in job_settings or 'command' not in job_settings or 'max_time' not in job_settings:
            logger.error("Singularity Script malformed")
            return None

        script = '#!/bin/bash -l\n\n'
        script += self._parse_slurm_job_settings(name,
                                                 job_settings,
                                                 '#SBATCH', '\n', logger)
        
        script += '\n# DYNAMIC VARIABLES\n\n'

        # first set modules
        if 'modules' and 'modules' in job_settings:
            modules_lst = ''
            for module in job_settings['modules']:
                modules_lst += ' ' + module
            if len(modules_lst) != 0:
                script += 'module load' + modules_lst + '\n'

        script += '\nmpirun singularity exec '

        if 'home' in job_settings and job_settings['home'] != '':
            script += '-H ' + job_settings['home'] + ' '

        if 'volumes' in job_settings:
            for volume in job_settings['volumes']:
                script += '-B ' + volume + ' '

        # add executable and arguments
        script += job_settings['image'] + ' ' + job_settings['command'] + '\n'
        # logger.info("script=%s"%script)

        return script

    def _build_email_script(self, name, slurm_job_id, job_settings, logger):
        # logger.info("_build_email_script")
        script = '#!/bin/bash -l\n\n'
        if 'mail_user' in job_settings:
            script += 'if [ -f slurm-' + slurm_job_id + '.out ]; then'
            script += '\n\t echo \"output results\" | mail -s \"output results for ' + str(name)
            script += ' \" -a slurm-' + slurm_job_id + '.out '
            script += job_settings['mail_user'] + '\n'
            script += 'fi\n'
            # logger.info("_build_email_script: email_content=%s"%script)
        return script

    def _build_job_submission_call(self, name, job_settings, logger):
        # logger.info("----_slurm: _build_job_submission_call-----------")
        # check input information correctness
        if not isinstance(job_settings, dict) or not isinstance(name,
                                                                basestring):
            return {'error': "Incorrect inputs"}

        if 'type' not in job_settings or 'command' not in job_settings:
            return {'error': "'type' and 'command' must be defined in job settings"}

        # first set modules
        slurm_call = ''
        if 'modules' in job_settings and job_settings['modules']:
            modules_lst = ''
            for module in job_settings['modules']:
                modules_lst += ' ' + module
            if len(modules_lst) != 0:
                slurm_call += 'module load' + modules_lst + '; '

        if job_settings['type'] == 'SBATCH':
            # sbatch command plus job name
            slurm_call += "sbatch --parsable -J '" + name + "'"
        elif job_settings['type'] == 'SRUN':
            slurm_call += "nohup srun -J '" + name + "'"
        else:
            return {'error': "Job type '" + job_settings['type'] + "'not supported"}

        if 'max_time' not in job_settings and job_settings['type'] == 'SRUN':
            return {'error': "'SRUN' jobs must define the 'max_time' property"}

        # logger.info("_build_job_submission_call: parse_slurm_job_settings")
        slurm_call += self._parse_slurm_job_settings(name,
                                                     job_settings,
                                                     None, None, logger)
        response = {}
        if 'scale' in job_settings and \
                int(job_settings['scale']) > 1:
            if job_settings['type'] == 'SRUN':
                return {'error': "'SRUN' does not allow scale property"}
            # set the job array
            slurm_call += ' --array=0-' + str(int(job_settings['scale']) - 1)
            # set the max of parallel jobs
            scale_max = job_settings['scale']
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

        # disable output
        # slurm_call += ' >/dev/null 2>&1';

        if job_settings['type'] == 'SRUN':
            slurm_call += ' &'

        response['call'] = slurm_call
        return response

    def _build_job_cancellation_call(self, name, job_settings, logger):
        return "scancel --name " + name

# Monitor
    def get_partitions(self, ssh_client, logger):
        # TODO(emepetres) set start time of consulting
        # (sacct only check current day)
        # logger.info("----getting partitions")
        call = "sinfo -h -a -o \"%R\" "
        output, exit_code = ssh_client.execute_shell_command(call,
                                                             wait_result=True)

        partitions = []
        if exit_code == 0:
            data = output.split('\n')
            data.pop() # last item is an empty string
            partitions = data #self._parse_partitions(output)
            # logger.info("partitions=%s"%partitions)
        else:
            logger.error("failed to get_partitions with code " + str(exit_code) + ":\n" + output)

        # logger.info("----getting partitions done")
        return partitions

    def get_reservations(self, ssh_client, logger):
        # TODO(emepetres) set start time of consulting
        # (sacct only check current day)
        # logger.info("----getting reservations")
        call = "sinfo -h -T "
        output, exit_code = ssh_client.execute_shell_command(call,
                                                             wait_result=True)

        partitions = []
        if exit_code == 0:
            data = output.split('\n') # should read only the 1srt item per lines
            data.pop() # last item is an empty string
            partitions = data #self._parse_partitions(output)
        else:
            logger.error("failed to get_reservations with code " + str(exit_code) + ":\n" + output)

        return partitions

    def get_states(self, ssh_client, names, logger):
        # TODO(emepetres) set start time of consulting
        # (sacct only check current day)
        # logger.info("----get_states (names=%s)"%names)
        call = "sacct -n -o JobName,State -X -P --name=" + ','.join(names)
        output, exit_code = ssh_client.execute_shell_command(call, wait_result=True)
        # logger.info("----get_states output=%s, exit_code=%s"%(output, exit_code))

        states = {}
        if exit_code == 0:
            states = self._parse_sacct(output)
        else:
            logger.error("failed to get_states with code " + str(exit_code) + ":\n" + output)
        logger.info("----get_states states=%s"%states)
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
