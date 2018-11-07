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


from hpc_plugin.ssh import SshClient
from workload_manager import WorkloadManager
from hpc_plugin.utilities import shlex_quote


class Torque(WorkloadManager):
    """ Holds the Torque functions. Acts similarly to the class `Slurm`."""

    def _build_container_script(self, name, job_settings, logger):
        """ Check input information correctness """
        if not isinstance(job_settings, dict) or \
                not isinstance(name, basestring):
            logger.error("Singularity Script malformed")
            return None

        if 'image' not in job_settings or 'command' not in job_settings or\
                'max_time' not in job_settings:
            logger.error("Singularity Script malformed")
            return None

        script = '#!/bin/bash -l\n\n'

        # NOTE an uploaded script could also be interesting to execute
        if 'pre' in job_settings:
            for entry in job_settings['pre']:
                script += entry + '\n'

#       ################### Torque settings ###################
        if 'nodes' in job_settings:
            resources_request = "nodes={}".format(job_settings['nodes'])

            if 'tasks_per_node' in job_settings:
                resources_request += ':ppn={}'.format(
                    job_settings['tasks_per_node'])

            script += '#PBS -l walltime={}\n'.format(resources_request)
        else:
            if 'tasks_per_node' in job_settings:
                logger.error(
                    r"Specify 'tasks_per_node' while 'nodes' is not specified")

        # if 'tasks' in job_settings:
        #     script += '#qsub -n ' + str(job_settings['tasks']) + '\n'

        script += '#PBS -l walltime={}\n\n'.format(job_settings['max_time'])
#       #######################################################

        script += '\n# DYNAMIC VARIABLES\n\n'

        # NOTE an uploaded script could also be interesting to execute
        if 'pre' in job_settings:
            for entry in job_settings['pre']:
                script += entry + '\n'

        script += '\nmpirun singularity exec '

        if 'home' in job_settings and job_settings['home'] != '':
            script += '-H ' + job_settings['home'] + ' '

        if 'volumes' in job_settings:
            for volume in job_settings['volumes']:
                script += '-B ' + volume + ' '

        # add executable and arguments
        script += job_settings['image'] + ' ' + job_settings['command'] + '\n'

        # NOTE an uploaded script could also be interesting to execute
        if 'post' in job_settings:
            for entry in job_settings['post']:
                script += entry + '\n'

        return script

    def _build_job_submission_call(self, name, job_settings, logger):
        # check input information correctness
        if not isinstance(job_settings, dict) or \
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

        # NOTE an uploaded script could also be interesting to execute
        if 'pre' in job_settings:
            for entry in job_settings['pre']:
                torque_call += entry + '; '

#       ################### Torque settings ###################
        # qsub command plus job name
        torque_call += "qsub -V -N {}".format(shlex_quote(name))

        resources_request = ""
        if 'nodes' in job_settings:
            resources_request = "nodes={}".format(job_settings['nodes'])

            # number of cores requested per node
            if 'tasks_per_node' in job_settings:
                resources_request += ':ppn={}'.format(
                    job_settings['tasks_per_node'])
        else:
            if 'tasks_per_node' in job_settings:
                logger.error(
                    r"Specify 'tasks_per_node' while 'nodes' is not specified")

        if 'max_time' in job_settings:
            if len(resources_request) > 0:
                resources_request += ','
            resources_request += 'walltime={}'.format(job_settings['max_time'])

        if len(resources_request) > 0:
            torque_call += ' -l {}'.format(resources_request)

        # more precisely is it a destination [queue][@server]
        if 'queue' in job_settings:
            torque_call += " -q {}".format(shlex_quote(job_settings['queue']))

        if 'rerunnable' in job_settings:  # same to requeue in SLURM
            torque_call += " -r {}".format(
                'y' if job_settings['rerunnable'] else 'n')

        if 'work_dir' in job_settings:
            torque_call += " -w {}".format(
                shlex_quote(job_settings['work_dir']))

        additional_attributes = {}
        if 'group_name' in job_settings:
            additional_attributes["group_list"] = shlex_quote(
                job_settings['group_name'])

        if len(additional_attributes) > 0:
            torque_call += " -W {}".format(
                ','.join("{0}={1}".format(k, v)
                         for k, v in additional_attributes.iteritems()))

        # if 'tasks' in job_settings:
        #     torque_call += ' -n ' + str(job_settings['tasks'])
#       #######################################################

        response = {}
        if 'scale' in job_settings and \
                int(job_settings['scale']) > 1:
            # set the max of parallel jobs
            scale_max = job_settings['scale']
            # set the job array
            torque_call += ' -J 0-{}'.format(scale_max - 1)
            if 'scale_max_in_parallel' in job_settings and \
                    int(job_settings['scale_max_in_parallel']) > 0:
                torque_call += '%{}'.format(
                    job_settings['scale_max_in_parallel'])
                scale_max = job_settings['scale_max_in_parallel']
            # map the orchestrator variables after last sbatch
            scale_env_mapping_call = \
                "sed -i '/# DYNAMIC VARIABLES/a\\" \
                "SCALE_INDEX=$PBS_ARRAYID\\n" \
                "SCALE_COUNT={scale_count}\\n" \
                "SCALE_MAX={scale_max}' {command}".format(
                    scale_count=job_settings['scale'],
                    scale_max=scale_max,
                    command=job_settings['command'].split()[0])  # file only
            response['scale_env_mapping_call'] = scale_env_mapping_call

        # add executable and arguments
        torque_call += ' {}'.format(job_settings['command'])

        # NOTE an uploaded script could also be interesting to execute
        if 'post' in job_settings:
            torque_call += '; '
            for entry in job_settings['post']:
                torque_call += entry + '; '

        response['call'] = torque_call
        return response

    def _build_job_cancellation_call(self, name, job_settings, logger):
        return r"qselect -N {} | xargs qdel".format(shlex_quote(name))

# Monitor
    def get_states(self, workdir, credentials, job_names, logger):
        return self._get_states_detailed(
            workdir,
            credentials,
            job_names,
            logger) if len(job_names) > 0 else {}

    @staticmethod
    def _get_states_detailed(workdir, credentials, job_names, logger):
        """
        Get job states by job names

        This function uses `qstat` command to query Torque.
        Please don't launch this call very friquently. Polling it
        frequently, especially across all users on the cluster,
        will slow down response times and may bring
        scheduling to a crawl.

        It allows to a precise mapping of Torque states to
        Slurm states by taking into account `exit_code`.
        Unlike `get_states_tabular` it parses output on host
        and uses several SSH commands.
        """
        # identify job ids
        call = "echo {} | xargs -n 1 qselect -N".format(
            shlex_quote(' '.join(map(shlex_quote, job_names))))

        client = SshClient(credentials)

        output, exit_code = client.execute_shell_command(
            call,
            workdir=workdir,
            wait_result=True)
        job_ids = Torque._parse_qselect(output)
        if not job_ids:
            return {}

        # get detailed information about jobs
        call = "qstat -f {}".format(' '.join(map(str, job_ids)))

        output, exit_code = client.execute_shell_command(
            call,
            workdir=workdir,
            wait_result=True)
        client.close_connection()
        try:
            job_states = Torque._parse_qstat_detailed(output)
        except SyntaxError as e:
            logger.warning(
                "cannot parse state response for job ids=[{}]".format(
                    ','.join(map(str, job_ids))))
            logger.warning(
                "{err}\n`qstat -f` output to parse:\n\\[\n{text}\n\\]".format(
                    err=str(e), text=output))
            # TODO: think whether error ignoring is better
            #       for the correct lifecycle
            raise e

        return job_states

    @staticmethod
    def _parse_qselect(qselect_output):
        """ Parse `qselect` output and returns
        list of job ids without host names """
        jobs = qselect_output.splitlines()
        if not jobs or (len(jobs) == 1 and jobs[0] is ''):
            return []
        return [int(job.split('.')[0]) for job in jobs]

    @staticmethod
    def _parse_qstat_detailed(qstat_output):
        from StringIO import StringIO
        jobs = {}
        for job in Torque._tokenize_qstat_detailed(StringIO(qstat_output)):
            # ignore job['Job_Id'], use identification by name
            name = job.get('Job_Name', '')
            state_code = job.get('job_state', None)
            if name and state_code:
                if state_code == 'C':
                    exit_status = int(job.get('exit_status', 0))
                    state = Torque._job_exit_status.get(
                        exit_status, "FAILED")  # unknown failure by default
                else:
                    state = Torque._job_states[state_code]
            jobs[name] = state
        return jobs

    @staticmethod
    def _tokenize_qstat_detailed(fp):
        import re
        # regexps for tokenization (buiding AST) of `qstat -f` output
        pattern_attribute_first = re.compile(
            r"^(?P<key>Job Id): (?P<value>(\w|\.)+)", re.M)
        pattern_attribute_next = re.compile(
            r"^    (?P<key>\w+(\.\w+)*) = (?P<value>.*)", re.M)
        pattern_attribute_continue = re.compile(
            r"^\t(?P<value>.*)", re.M)

        # tokenizes stream output and
        job_attr_tokens = {}
        for line_no, line in enumerate(fp.readlines()):
            line = line.rstrip('\n\r')  # strip trailing newline character
            if len(line) > 1:  # skip empty lines
                # find match for the new attribute
                match = pattern_attribute_first.match(line)
                if match:      # start to handle new job descriptor
                    if len(job_attr_tokens) > 0:
                        yield job_attr_tokens
                    job_attr_tokens = {}
                else:
                    match = pattern_attribute_next.match(line)

                if match:      # line corresponds to the new attribute
                    attr = match.group('key').replace(' ', '_')
                    job_attr_tokens[attr] = match.group('value')
                else:          # either multiline attribute or broken line
                    match = pattern_attribute_continue.match(line)
                    if match:  # multiline attribute value continues
                        job_attr_tokens[attr] += match.group('value')
                    elif len(job_attr_tokens[attr]) > 0\
                            and job_attr_tokens[attr][-1] == '\\':
                        # multiline attribute with newline character
                        job_attr_tokens[attr] = "{0}\n{1}".format(
                            job_attr_tokens[attr][:-1], line)
                    else:
                        raise SyntaxError(
                            'failed to parse l{no}: "{line}"'.format(
                                no=line_no, line=line))
        if len(job_attr_tokens) > 0:
            yield job_attr_tokens

    _job_states = dict(
        # C includes completion by both success and fail: "COMPLETED",
        #     "TIMEOUT", "FAILED","CANCELLED", #"BOOT_FAIL", and "REVOKED"
        C="COMPLETED",   # Job is completed after having run
        E="COMPLETING",  # Job is exiting after having run
        H="PENDING",     # (@TODO like "RESV_DEL_HOLD" in Slurm) Job is held
        Q="PENDING",     # Job is queued, eligible to run or routed
        R="RUNNING",     # Job is running
        T="PENDING",     # (nothng in Slurm) Job is being moved to new location
        W="PENDING",     # (nothng in Slurm) Job is waiting for the time after
        #                  which the job is eligible for execution (`qsub -a`)
        S="SUSPENDED",   # (Unicos only) Job is suspended
        # The latter states have no analogues
        #   "CONFIGURING", "STOPPED", "NODE_FAIL", "PREEMPTED", "SPECIAL_EXIT"
    )

    _job_exit_status = {
        0:   "COMPLETED",  # OK             Job execution successful
        -1:  "FAILED",     # FAIL1          Job execution failed, before
        #                                   files, no retry
        -2:  "FAILED",     # FAIL2          Job execution failed, after
        #                                    files, no retry
        -3:  "FAILED",     # RETRY          Job execution failed, do retry
        -4:  "BOOT_FAIL",  # INITABT        Job aborted on MOM initialization
        -5:  "BOOT_FAIL",  # INITRST        Job aborted on MOM init, chkpt,
        #                                    no migrate
        -6:  "BOOT_FAIL",  # INITRMG        Job aborted on MOM init, chkpt,
        #                                    ok migrate
        -7:  "FAILED",     # BADRESRT       Job restart failed
        -8:  "FAILED",     # CMDFAIL        Exec() of user command failed
        -9:  "NODE_FAIL",  # STDOUTFAIL     Couldn't create/open stdout/stderr
        -10: "NODE_FAIL",  # OVERLIMIT_MEM  Job exceeded a memory limit
        -11: "NODE_FAIL",  # OVERLIMIT_WT   Job exceeded a walltime limit
        -12: "TIMEOUT",    # OVERLIMIT_CPUT Job exceeded a CPU time limit
    }

    @staticmethod
    def _get_states_tabular(ssh_client, job_names, logger):
        """
        Get job states by job names

        This function uses `qstat` command to query Torque.
        Please don't launch this call very friquently. Polling it
        frequently, especially across all users on the cluster,
        will slow down response times and may bring
        scheduling to a crawl.

        It invokes `tail/awk` to make simple parsing on the remote HPC.
        """
        # TODO:(emepetres) set start day of consulting
        # @caution This code fails to manage the situation
        #          if several jobs have the same name
        call = "qstat -i `echo {} | xargs -n 1 qselect -N` "\
            "| tail -n+6 | awk '{{ print $4 \"|\" $10 }}'".format(
                shlex_quote(' '.join(map(shlex_quote, job_names))))
        output, exit_code = ssh_client.send_command(call, wait_result=True)

        return Torque._parse_qstat_tabular(output) if exit_code == 0 else {}

    @staticmethod
    def _parse_qstat_tabular(qstat_output):
        """ Parse two colums `qstat` entries into a dict """
        def parse_qstat_record(record):
            name, state_code = map(str.strip, record.split('|'))
            return name, Torque._job_states[state_code]

        jobs = qstat_output.splitlines()
        parsed = {}
        # @TODO: think of catch-and-log parsing exceptions
        if jobs and (len(jobs) > 1 or jobs[0] is not ''):
            parsed = dict(map(parse_qstat_record, jobs))

        return parsed
