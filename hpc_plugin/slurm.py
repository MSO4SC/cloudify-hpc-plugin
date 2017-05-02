########
# Copyright (c) 2017 MSO4SC - javier.carnero@atos.net
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
import string
import random
from hpc_plugin.ssh import SshClient


def submit_job(ssh_client, base_name, job_settings):
    """
    Sends a job to the HPC using Slurm

    @type ssh_client: SshClient
    @param ssh_client: ssh client connected to an HPC login node
    @type base_name: string
    @param base_name: base name of the job in slurm
    @type job_settings: dictionary
    @param job_settings: dictionary with the job options
    @rtype string
    @return Slurm's job name sent. None if an error arise.
    """
    if not isinstance(ssh_client, SshClient) or not ssh_client.is_open():
        # TODO(emepetres): Raise error
        return None

    name = get_random_name(base_name)
    call = get_slurm_call(name, job_settings)

    if call is None:
        # TODO(emepetres): Raise error
        return None

    _, exit_code = ssh_client.send_command(call, want_exitcode=True)

    if exit_code == 0:
        return name
    else:
        # TODO(emepetres): Raise error
        return None


def get_slurm_call(name, job_settings):
    """
    Generates slurm command line as a string

    @type name: string
    @param name: name of the job in slurm
    @type job_settings: dictionary
    @param job_settings: dictionary with the job options
    @rtype string
    @return string to call slurm with its parameters. None if an error arise.
    """

    # check input information correctness
    if not isinstance(job_settings, dict) or not isinstance(name,
                                                            basestring):
        # TODO(emepetres): Raise error
        return None

    if 'type' not in job_settings or 'command' not in job_settings:
        # TODO(emepetres): Raise error
        return None

    # first set modules
    slurm_call = ''
    if 'modules' in job_settings:
        slurm_call += 'module load'
        for module in job_settings['modules']:
            slurm_call += ' ' + module
        slurm_call += '; '

    if job_settings['type'] == 'SBATCH':
        # sbatch command plus job name
        slurm_call += "sbatch -J '" + name + "'"
    elif job_settings['type'] == 'SRUN':
        slurm_call += "nohup srun -J '" + name + "'"
    else:
        # TODO(empetres): Raise error
        return None

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

    if 'max_time' in job_settings:
        slurm_call += ' -t ' + job_settings['max_time']
    elif job_settings['type'] == 'SRUN':
        # TODO(empetres): Raise error
        return None

    # add executable and arguments
    slurm_call += ' ' + job_settings['command']

    # disable output
    # slurm_call += ' >/dev/null 2>&1';

    if job_settings['type'] == 'SRUN':
        slurm_call += ' &'

    return slurm_call


def get_random_name(base_name):
    return base_name + '_' + __id_generator()


def __id_generator(size=6, chars=string.digits + string.ascii_letters):
    return ''.join(random.SystemRandom().choice(chars) for _ in range(size))


"""
  int callSlurm(const string& slurm_call) const {
    ssh_channel channel;
    int rc;
    char buffer[256];
    int nbytes;
    channel = ssh_channel_new(my_ssh_session);
    if (channel == NULL) return SSH_ERROR;
    rc = ssh_channel_open_session(channel);
    if (rc != SSH_OK) {
      ssh_channel_free(channel);
      return rc;
    }
    rc = ssh_channel_request_exec(channel, slurm_call.c_str());
    if (rc != SSH_OK) {
      ssh_channel_close(channel);
      ssh_channel_free(channel);
      return rc;
    }

    ssh_channel_send_eof(channel);
    ssh_channel_close(channel);
    ssh_channel_free(channel);
    return SSH_OK;
  }

  /** FIXME
   * We highly recommend that people writing meta-schedulers or that wish to
   * interrogate SLURM in scripts do so using the squeue and sacct commands. We
   * strongly recommend that your code performs these queries once every 60
   * seconds or longer. Using these commands contacts the master controller
   * directly, the same process responsible for scheduling all work on the
   * cluster. Polling more frequently, especially across all users on the
   * cluster, will slow down response times and may bring scheduling to a crawl
   * Please don't.
   */
  int getJobIdByName(const string& name, ulong* jobid) const {
    string command = "sacct -n -o jobid -X --name='" + name + "'";

    ssh_channel channel;
    int rc;
    char buffer[256];
    int nbytes;
    channel = ssh_channel_new(my_ssh_session);
    if (channel == NULL) return SSH_ERROR;
    rc = ssh_channel_open_session(channel);
    if (rc != SSH_OK) {
      ssh_channel_free(channel);
      return rc;
    }
    rc = ssh_channel_request_exec(channel, command.c_str());
    if (rc != SSH_OK) {
      ssh_channel_close(channel);
      ssh_channel_free(channel);
      return rc;
    }

    stringstream output;
    nbytes = ssh_channel_read(channel, buffer, sizeof(buffer), 0);
    while (nbytes > 0) {
      output.write(buffer, nbytes);
      nbytes = ssh_channel_read(channel, buffer, sizeof(buffer), 0);
    }

    if (nbytes < 0) {
      ssh_channel_close(channel);
      ssh_channel_free(channel);
      return SSH_ERROR;
    } else if (output.str().size() > 0) {
      *jobid = std::stoul(output.str());
    }

    ssh_channel_send_eof(channel);
    ssh_channel_close(channel);
    ssh_channel_free(channel);
    return SSH_OK;
  }

  /** FIXME
   * We highly recommend that people writing meta-schedulers or that wish to
   * interrogate SLURM in scripts do so using the squeue and sacct commands. We
   * strongly recommend that your code performs these queries once every 60
   * seconds or longer. Using these commands contacts the master controller
   * directly, the same process responsible for scheduling all work on the
   * cluster. Polling more frequently, especially across all users on the
   * cluster, will slow down response times and may bring scheduling to a crawl
   * Please don't.
   */
  int getJobStatus(const ulong& jobid, TaskState* state) const {
    string command = "sacct -n -o state -X -P -j " + std::to_string(jobid);

    ssh_channel channel;
    int rc;
    char buffer[256];
    int nbytes;
    channel = ssh_channel_new(my_ssh_session);
    if (channel == NULL) return SSH_ERROR;
    rc = ssh_channel_open_session(channel);
    if (rc != SSH_OK) {
      ssh_channel_free(channel);
      return rc;
    }
    rc = ssh_channel_request_exec(channel, command.c_str());
    if (rc != SSH_OK) {
      ssh_channel_close(channel);
      ssh_channel_free(channel);
      return rc;
    }

    stringstream output;
    nbytes = ssh_channel_read(channel, buffer, sizeof(buffer), 0);
    while (nbytes > 0) {
      output.write(buffer, nbytes);
      nbytes = ssh_channel_read(channel, buffer, sizeof(buffer), 0);
    }

    if (nbytes < 0) {
      ssh_channel_close(channel);
      ssh_channel_free(channel);
      return SSH_ERROR;
    } else if (output.str().size() > 0) {
      string state_str = output.str();
      state_str.pop_back();  // delete end of line character
      // cout << "DEBUG RECEIVED: " << state_str << endl;

      if (state_str == "PENDING" || state_str == "CONFIGURING") {
        *state = TaskState::TASK_STARTING;
      } else if (state_str == "RUNNING" || state_str == "COMPLETING") {
        *state = TaskState::TASK_RUNNING;

      } else if (state_str == "COMPLETED" || state_str == "PREEMPTED") {
        *state = TaskState::TASK_FINISHED;
      } else if (state_str == "BOOT_FAIL" || state_str == "CANCELLED" ||
                 state_str == "DEADLINE" || state_str == "FAILED" ||
                 state_str == "TIMEOUT") {
        *state = TaskState::TASK_FAILED;
      } else {  // RESIZING, SUSPENDED
        *state = TaskState::TASK_FAILED;
        cout << "ERROR: State '" << state_str << "' could not be recognized."
             << endl;
      }
    }

    ssh_channel_send_eof(channel);
    ssh_channel_close(channel);
    ssh_channel_free(channel);
    return SSH_OK;
  }

  string getRandomString(const int len) {
    stringstream ramdom_ss;

    boost::random::random_device rng;
    boost::random::uniform_int_distribution<> index_dist(0,
                                                         alphanum.size() - 1);
    for (int i = 0; i < len; ++i) {
      ramdom_ss << alphanum[index_dist(rng)];
    }

    return ramdom_ss.str();
  }
};
"""
