import string
import random
from datetime import datetime
from hpc_plugin.ssh import SshClient


class WorkloadManager(object):

    def factory(workload_manager):
        if workload_manager == "SLURM":
            from slurm import Slurm
            return Slurm()
        assert 0, "Bad workload manager creation: " +\
            workload_manager
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
        if not self._checkSshClient(ssh_client, logger):
            return False

        if is_singularity:
            # generate script content for singularity
            script_content = self._build_container_script(name,
                                                          job_settings,
                                                          logger)
            if script_content is None:
                return False

            if not self._create_shell_script(ssh_client,
                                             name + ".script",
                                             script_content,
                                             logger,
                                             workdir=workdir):
                return False

            settings = {
                "type": "SBATCH",
                "command": name + ".script"
            }

            if 'scale' in job_settings and \
                    job_settings['scale'] > 1:
                settings['scale'] = job_settings['scale']
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
            return False

        # prepare the scale env variables
        if 'scale_env_mapping_call' in response:
            scale_env_mapping_call = response['scale_env_mapping_call']
            output, exit_code = self._execute_shell_command(
                ssh_client,
                scale_env_mapping_call,
                workdir=workdir,
                wait_result=True)
            if exit_code is not 0:
                logger.error("Scale env vars mapping '" +
                             scale_env_mapping_call +
                             "' failed with code " +
                             str(exit_code) + ":\n" + output)
                return False

        # submit the job
        call = response['call']
        output, exit_code = self._execute_shell_command(ssh_client,
                                                        call,
                                                        workdir=workdir,
                                                        wait_result=True)
        if exit_code is not 0:
            logger.error("Job submission '" + call + "' exited with code " +
                         str(exit_code) + ":\n" + output)
            return False
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
        if not self._checkSshClient(ssh_client, logger):
            return False

        if is_singularity:
            return self._execute_shell_command(ssh_client,
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
        if not self._checkSshClient(ssh_client, logger):
            return False

        call = self._build_job_cancellation_call(name,
                                                 job_options,
                                                 logger)
        if call is None:
            return False

        return self._execute_shell_command(ssh_client,
                                           call,
                                           workdir=workdir)

    def create_new_workdir(self, ssh_client, base_dir, base_name):
        workdir = self._get_time_name(base_name)

        # we make sure that the workdir does not exists
        base_name = workdir
        while self._exists_path(ssh_client, base_dir + "/" + workdir):
            workdir = self._get_random_name(base_name)

        full_path = base_dir + "/" + workdir
        if self._execute_shell_command(
                ssh_client,
                "mkdir -p " + base_dir + "/" + workdir):
            return full_path
        else:
            return None

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
        logger.error("'_build_container_script' not implemented.")
        return None

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
        @rtype string
        @return string to call slurm with its parameters.
            None if an error arise.
        """
        return {'error': "'_build_job_submission_call' not implemented."}

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
        logger.error("'_build_job_cancellation_call' not implemented.")
        return None

    def _checkSshClient(self,
                        ssh_client,
                        logger):
        if not isinstance(ssh_client, SshClient) or not ssh_client.is_open():
            logger.error("SSH Client can't be used")
            return False
        return True

    def _create_shell_script(self,
                             ssh_client,
                             name,
                             script_content,
                             logger,
                             workdir=None):
        # escape for echo command
        script_data = script_content \
            .replace("\\", "\\\\") \
            .replace("$", "\\$") \
            .replace("`", "\\`") \
            .replace('"', '\\"')

        create_call = "echo \"" + script_data + "\" >> " + name + \
            "; chmod +x " + name
        _, exit_code = self._execute_shell_command(ssh_client,
                                                   create_call,
                                                   workdir=workdir,
                                                   wait_result=True)
        if exit_code is not 0:
            logger.error(
                "failed to create script: call '" + create_call +
                "', exit code " + str(exit_code))
            return False

        return True

    def _execute_shell_command(self,
                               ssh_client,
                               cmd,
                               workdir=None,
                               wait_result=False):
        if not workdir:
            return ssh_client.send_command(cmd,
                                           wait_result=wait_result)
        else:
            call = "export CURRENT_WORKDIR=" + workdir + " && "
            call += "cd " + workdir + " && "
            call += cmd
            return ssh_client.send_command(call,
                                           wait_result=wait_result)

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
        _, exit_code = self._execute_shell_command(ssh_client,
                                                   '[ -d "' + path + '" ]',
                                                   wait_result=True)

        if exit_code == 0:
            return True
        else:
            return False
