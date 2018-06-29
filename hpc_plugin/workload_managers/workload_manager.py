import string
import random
from datetime import datetime
from hpc_plugin.ssh import SshClient


BOOTFAIL = 0
CANCELLED = 1
COMPLETED = 2
CONFIGURING = 3
COMPLETING = 4
FAILED = 5
NODEFAIL = 6
PENDING = 7
PREEMPTED = 8
REVOKED = 9
RUNNING = 10
SPECIALEXIT = 11
STOPPED = 12
SUSPENDED = 13
TIMEOUT = 14

JOBSTATESLIST = [
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "CONFIGURING",
    "COMPLETING",
    "FAILED",
    "NODE_FAIL",
    "PENDING",
    "PREEMPTED",
    "REVOKED",
    "RUNNING",
    "SPECIAL_EXIT",
    "STOPPED",
    "SUSPENDED",
    "TIMEOUT",
]

JOBSTATESDICT = {
    "BOOT_FAIL": 0,
    "CANCELLED": 1,
    "COMPLETED": 2,
    "CONFIGURING": 3,
    "COMPLETING": 4,
    "FAILED": 5,
    "NODE_FAIL": 6,
    "PENDING": 7,
    "PREEMPTED": 8,
    "REVOKED": 9,
    "RUNNING": 10,
    "SPECIAL_EXIT": 11,
    "STOPPED": 12,
    "SUSPENDED": 13,
    "TIMEOUT": 14,
}

_STATES_PRECEDENCE = [
    FAILED,
    NODEFAIL,
    BOOTFAIL,
    CANCELLED,
    REVOKED,
    TIMEOUT,
    SPECIALEXIT,
    STOPPED,
    SUSPENDED,
    PREEMPTED,
    RUNNING,
    CONFIGURING,
    PENDING,
    COMPLETING,
    COMPLETED
]


def state_int_to_str(value):
    """state on its int value to its string value"""
    return JOBSTATESLIST[int(value)]


def state_str_to_int(value):
    """state on its string value to its int value"""
    return JOBSTATESDICT[value]


def get_prevailing_state(state1, state2):
    """receives two string states and decides which one prevails"""
    _st1 = state_str_to_int(state1)
    _st2 = state_str_to_int(state2)

    if _st1 == _st2:
        return state1

    for state in _STATES_PRECEDENCE:
        if _st1 == state or _st2 == state:
            return JOBSTATESLIST[state]

    return state1


class WorkloadManager(object):

    def factory(workload_manager):
        if workload_manager == "SLURM":
            from slurm import Slurm
            return Slurm()
        else:
            return None
    factory = staticmethod(factory)

    def submit_job(self,
                   ssh_client,
                   name,
                   job_settings,
                   is_singularity,
                   logger,
                   workdir=None):
        """
        Sends a job to the HPC

        @type ssh_client: SshClient
        @param ssh_client: ssh client connected to an HPC login node
        @type name: string
        @param name: name of the job
        @type job_settings: dictionary
        @param job_settings: dictionary with the job options
        @type is_singularity: bool
        @param is_singularity: True if the job is in a container
        @rtype string
        @return Slurm's job name sent. None if an error arise.
        """

        # logger.debug("job_settings:" )
        # for key, value in job_settings.iteritems() :
        #     logger.debug("job_settings[%s]=%s"%(key, value) )
        # logger.debug("is_singularity=%s"%(is_singularity) )

        if not SshClient.check_ssh_client(ssh_client, logger):
            logger.error("check_ssh_client failed")
            return False

        if is_singularity:
            # generate script content for singularity
            hpc_partitions = self.get_partitions(ssh_client, logger)
            if job_settings['partition'] and not job_settings['partition'] in hpc_partitions:
                logger.error("inconherent partition name %s" % job_settings['partition'])
                logger.error("valid partitions=%s" % (hpc_partitions))
                return ("", False)

            # check for consistent data
            if job_settings['nodes'] != '' and job_settings['tasks_per_node'] != '':
                if int(job_settings['tasks_per_node']) * int(job_settings['nodes']) != int(job_settings['tasks']):
                    logger.error("inconsistent tasks %d , nodes %d and tasks_per_node %d" % (int(job_settings['tasks']), int(job_settings['nodes']), int(job_settings['tasks_per_node'])))
                    return ("", False)

            script_content = self._build_container_script(name, job_settings, logger)
            if script_content is None:
                logger.error("submit_job: failed to create script_content")
                return ("", False)

            if not self._create_shell_script(ssh_client,
                                             name + ".script",
                                             script_content,
                                             logger,
                                             workdir=workdir):
                logger.error("submit_job: create_shell_script %s failed" % (name + ".script"))
                return ("", False)

            settings = {
                "type": "SBATCH",
                "command": name + ".script"
            }

            if 'scale' in job_settings:
                settings['scale'] = job_settings['scale']
                if 'scale_max_in_parallel' in job_settings:
                    settings['scale_max_in_parallel'] = \
                        job_settings['scale_max_in_parallel']
        else:
            settings = job_settings

        # build the call to submit the job
        response = self._build_job_submission_call(name,
                                                   settings,
                                                   logger)

        if 'error' in response:
            logger.error(
                "Couldn't build the call to send the job: " +
                response['error'])
            return ("", False)

        # prepare the scale env variables
        if 'scale_env_mapping_call' in response:
            scale_env_mapping_call = response['scale_env_mapping_call']
            output, exit_code = ssh_client.execute_shell_command(
                scale_env_mapping_call,
                workdir=workdir,
                wait_result=True)
            if exit_code is not 0:
                logger.error("Scale env vars mapping '" +
                             scale_env_mapping_call +
                             "' failed with code " +
                             str(exit_code) + ":\n" + output)
                return ("", False)

        # submit the job
        call = response['call']
        # logger.debug("submit_job: call=%"%call)
        output, exit_code = ssh_client.execute_shell_command(
            call,
            workdir=workdir,
            wait_result=True)

        # Get output of slurm and add to logger
        slurm_job_id = output.replace("Submitted batch job ", '').strip()
        cmd = 'cat slurm-%s.out' % slurm_job_id
        cmd_out, cmd_code = ssh_client.execute_shell_command(
            cmd,
            workdir=workdir,
            wait_result=True)
        if cmd_code is 0:
            logger.info("slurm-%s.out:\n%s" % (slurm_job_id, cmd_out))

        if exit_code is not 0:
            logger.error("Job submission '" + call + "' exited with code " +
                         str(exit_code) + ":\n" + output)
            return ("", False)

        return (slurm_job_id, True)

    def display_job_output(self,
                           ssh_client,
                           name,
                           logger,
                           workdir=None):
        """
        Add the content of name to logger
        """

        output, exit_code = ssh_client.execute_shell_command(
                                             'cat ' + name,
                                             workdir=workdir,
                                             wait_result=True)
        if exit_code == 0 and output:
            logger.info('job_output (err=%s) :\n%s' % (exit_code, output))
        return exit_code

    def send_notify_user(self,
                         ssh_client,
                         name,
                         logger,
                         workdir=None):
        """
        Execute sendmail.sh script to send after job is completed
        """
        exit_code = ssh_client.execute_shell_command('./sendmail.sh',
                                                     workdir)
        return exit_code

    def notify_user(self,
                    ssh_client,
                    name,
                    slurm_job_id,
                    job_settings,
                    logger,
                    workdir=None):
        """
        Create a script to send after job is completed
        """

        if 'mail_user' in job_settings:
            email_content = self._build_email_script(name,
                                                     slurm_job_id,
                                                     job_settings,
                                                     logger)

            self._create_shell_script(ssh_client,
                                      "sendmail.sh",
                                      email_content,
                                      logger,
                                      workdir=workdir)
        return True

    def clean_job_aux_files(self,
                            ssh_client,
                            name,
                            job_options,
                            is_singularity,
                            logger,
                            workdir=None):
        """
        Cleans no more needed job files in the HPC

        @type ssh_client: SshClient
        @param ssh_client: ssh client connected to an HPC login node
        @type name: string
        @param name: name of the job
        @type job_settings: dictionary
        @param job_settings: dictionary with the job options
        @type is_singularity: bool
        @param is_singularity: True if the job is in a container
        @rtype string
        @return Slurm's job name stopped. None if an error arise.
        """
        if not SshClient.check_ssh_client(ssh_client, logger):
            return False

        if is_singularity:
            return ssh_client.execute_shell_command(
                "rm " + name + ".script",
                workdir=workdir)
        return True

    def stop_job(self,
                 ssh_client,
                 name,
                 job_options,
                 is_singularity,
                 logger,
                 workdir=None):
        """
        Stops a job from the HPC

        @type ssh_client: SshClient
        @param ssh_client: ssh client connected to an HPC login node
        @type name: string
        @param name: name of the job
        @type job_settings: dictionary
        @param job_settings: dictionary with the job options
        @type is_singularity: bool
        @param is_singularity: True if the job is in a container
        @rtype string
        @return Slurm's job name stopped. None if an error arise.
        """
        if not SshClient.check_ssh_client(ssh_client, logger):
            return False

        call = self._build_job_cancellation_call(name,
                                                 job_options,
                                                 logger)
        if call is None:
            return False

        return ssh_client.execute_shell_command(
            call,
            workdir=workdir)

    def create_new_workdir(self, ssh_client, base_dir, base_name, logger):
        workdir = self._get_time_name(base_name)

        # we make sure that the workdir does not exists
        base_name = workdir
        while self._exists_path(ssh_client, base_dir + "/" + workdir):
            workdir = self._get_random_name(base_name)

        full_path = base_dir + "/" + workdir
        exit_code = ssh_client.execute_shell_command("mkdir -p " + base_dir + "/" + workdir)

        if not exit_code:
            logger.error("failed to create %s (error=%s)" % (base_dir + "+" + workdir, exit_code))
            return None

        return full_path

    def get_states(self, ssh_client, names, logger):
        """
        Get the states of the jobs names

        @type credentials: dictionary
        @param credentials: dictionary with the HPC SSH credentials
        @type names: list
        @param names: list of the job names to retrieve their states
        @rtype dict
        @return a dictionary of job names and its states
        """
        raise NotImplementedError("'get_states' not implemented.")

    def _build_email_script(self,
                            name,
                            job_id,
                            settings,
                            logger):
        """
        Creates a script to send slurm output by email

        @type name: string
        @param name: name of the job
        @type job_id: integer
        @type job_settings: dictionary
        @param job_settings: dictionary with the container job options
        @rtype string
        @return string to with the email script. None if an error arise.
        """
        raise NotImplementedError("'_build_email_script' not implemented.")

    def _build_container_script(self,
                                name,
                                settings,
                                logger):
        """
        Creates a script to run Singularity

        @type name: string
        @param name: name of the job
        @type job_settings: dictionary
        @param job_settings: dictionary with the container job options
        @rtype string
        @return string to with the sbatch script. None if an error arise.
        """
        raise NotImplementedError("'_build_container_script' not implemented.")

    def _build_job_submission_call(self,
                                   name,
                                   job_settings,
                                   logger):
        """
        Generates submission command line as a string

        @type name: string
        @param name: name of the job
        @type job_settings: dictionary
        @param job_settings: dictionary with the job options
        @rtype dict
        @return dict with two keys:
         'call' string to call slurm with its parameters, and
         'scale_env_mapping_call' to push the scale env variables on
         the batch scripts
            None if an error arise.
        """
        raise NotImplementedError(
            "'_build_job_submission_call' not implemented.")

    def _build_job_cancellation_call(self,
                                     name,
                                     job_settings,
                                     logger):
        """
        Generates cancel command line as a string

        @type name: string
        @param name: name of the job
        @type job_settings: dictionary
        @param job_settings: dictionary with the job options
        @rtype string
        @return string to call slurm with its parameters.
            None if an error arise.
        """
        raise NotImplementedError(
            "'_build_job_cancellation_call' not implemented.")

    def _create_shell_script(self,
                             ssh_client,
                             name,
                             script_content,
                             logger,
                             workdir=None):

        logger.debug('wm: _create_shell_script.. name=%s' % name)
        # escape for echo command
        script_data = script_content \
            .replace("\\", "\\\\") \
            .replace("$", "\\$") \
            .replace("`", "\\`") \
            .replace('"', '\\"')

        create_call = "echo \"" + script_data + "\" >> " + name + \
            "; chmod +x " + name
        logger.debug('wm: sshclient.execute_shell_command.. create_call=%s' % create_call)
        output, exit_code = ssh_client.execute_shell_command(create_call,
                                                             workdir=workdir,
                                                             wait_result=True)

        if exit_code is not 0:
            logger.error(
                "failed to create script: call '" + create_call +
                "', exit code " + str(exit_code))
            if output:
                logger.error(
                    "failed to create script: call '" + create_call +
                    "', output " + str(output))
            return False

        return True

    def _get_random_name(self, base_name):
        """ Get a random name with a prefix """
        return base_name + '_' + self.__id_generator()

    def _get_time_name(self, base_name):
        """ Get a random name with a prefix """
        return base_name + '_' + datetime.utcnow().strftime('%Y%m%d_%H%M%S')

    def __id_generator(self,
                       size=6,
                       chars=string.digits + string.ascii_letters):
        return ''.join(random.SystemRandom().choice(chars)
                       for _ in range(size))

    def _exists_path(self, ssh_client, path):
        _, exit_code = ssh_client.execute_shell_command(
            '[ -d "' + path + '" ]',
            wait_result=True)

        if exit_code == 0:
            return True

        return False
