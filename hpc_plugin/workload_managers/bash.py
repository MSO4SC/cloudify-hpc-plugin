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


import workload_manager


class Bash(workload_manager.WorkloadManager):

    def _build_job_submission_call(self, name, job_settings, logger):
        # check input information correctness
        if not isinstance(job_settings, dict) or not isinstance(name,
                                                                basestring):
            return {'error': "Incorrect inputs"}

        if 'type' not in job_settings or 'command' not in job_settings:
            return {'error': "'type' and 'command' " +
                    "must be defined in job settings"}

        if job_settings['type'] != 'SHELL':
            return {'error': "Job type '" + job_settings['type'] +
                    "'not supported"}

        # add executable and arguments, and save exit code on env
        slurm_call = 'nohup bash -c "' + \
            job_settings['command'] + ' ' +\
            '; echo ' + name + '=$? >> msomonitor.data" ' + \
            '&'

        response = {}
        response['call'] = slurm_call
        return response

    def _build_job_cancellation_call(self, name, job_settings, logger):
        return "pkill -f " + name

# Monitor
    def build_raw_states_call(self, ssh_client, names, logger):
        return "cat msomonitor.data"

    def parse_states(self, raw_states, logger):
        """ Parse two colums exit codes into a dict """
        jobs = raw_states.splitlines()
        parsed = {}
        if jobs and (len(jobs) > 1 or jobs[0] is not ''):
            for job in jobs:
                first, second = job.strip().split(',')
                parsed[first] = self._parse_exit_codes(second)

        return parsed

    def _parse_exit_codes(self, exit_code):
        if exit_code == '0':  # exited normally
            return workload_manager.JOBSTATESLIST[workload_manager.COMPLETED]
        elif exit_code == '1':  # general error
            return workload_manager.JOBSTATESLIST[workload_manager.FAILED]
        elif exit_code == '126':  # cannot execute
            return workload_manager.JOBSTATESLIST[workload_manager.REVOKED]
        elif exit_code == '127':  # not found
            return workload_manager.JOBSTATESLIST[workload_manager.BOOT_FAIL]
        elif exit_code == '130':  # terminated by ctrl+c
            return workload_manager.JOBSTATESLIST[workload_manager.CANCELLED]
        else:
            return workload_manager.JOBSTATESLIST[workload_manager.FAILED]
