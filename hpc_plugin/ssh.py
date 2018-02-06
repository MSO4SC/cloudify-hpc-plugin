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

"""Wrap of paramiko to send ssh commands

Todo:
    * read stderr and return it
    * control SSH exceptions and return failures
"""
import select
from paramiko import client


class SshClient(object):
    """Represents a ssh client"""
    _client = None

    def __init__(self, address, username, password, port=22):
        # print "Connecting to server ", str(address)+":"+str(port)
        self._client = client.SSHClient()
        self._client.set_missing_host_key_policy(client.AutoAddPolicy())
        self._client.connect(
            address,
            port=port,
            username=username,
            password=password,
            look_for_keys=False
        )

    def is_open(self):
        """Check if connection is open"""
        return self._client is not None

    def close_connection(self):
        """Closes opened connection"""
        if self._client is not None:
            self._client.close()

    def send_command(self,
                     command,
                     exec_timeout=3000,
                     read_chunk_timeout=500,
                     wait_result=False):
        """Sends a command and returns stdout, stderr and exitcode"""

        # Check if connection is made previously
        if self._client is not None:
            # there is one channel per command
            stdin, stdout, stderr = self._client.exec_command(
                command,
                # get_pty=True, # Ask for shell login, not working with srun
                timeout=exec_timeout)

            if wait_result:
                # get the shared channel for stdout/stderr/stdin
                channel = stdout.channel

                # we do not need stdin
                stdin.close()
                # indicate that we're not going to write to that channel
                channel.shutdown_write()

                # read stdout/stderr in order to prevent read block hangs
                stdout_chunks = []
                stdout_chunks.append(stdout.channel.recv(
                    len(stdout.channel.in_buffer)))
                # chunked read to prevent stalls
                while (not channel.closed
                       or channel.recv_ready()
                       or channel.recv_stderr_ready()):
                    # Stop if channel was closed prematurely,
                    # and there is no data in the buffers.
                    got_chunk = False
                    readq, _, _ = select.select([stdout.channel],
                                                [],
                                                [],
                                                read_chunk_timeout)
                    for c in readq:
                        if c.recv_ready():
                            stdout_chunks.append(stdout.channel.recv(
                                len(c.in_buffer)))
                            got_chunk = True
                        if c.recv_stderr_ready():
                            # make sure to read stderr to prevent stall
                            stderr.channel.recv_stderr(len(c.in_stderr_buffer))
                            got_chunk = True
                    '''
                    1) make sure that there are at least 2 cycles with no data
                        in the input buffers in order to not exit too early
                        (i.e. cat on a >200k file).
                    2) if no data arrived in the last loop, check if we already
                        received the exit code
                    3) check if input buffers are empty
                    4) exit the loop
                    '''
                    if (not got_chunk
                            and stdout.channel.exit_status_ready()
                            and not stderr.channel.recv_stderr_ready()
                            and not stdout.channel.recv_ready()):
                        # Indicate that we're not going to read from
                        # this channel anymore
                        stdout.channel.shutdown_read()
                        # close the channel
                        stdout.channel.close()
                        # Remote side is finished & our bufferes are empty
                        break

            # close all the pseudofiles
            stdout.close()
            stderr.close()

            if wait_result:
                # exit code is always ready at this point
                exit_code = stdout.channel.recv_exit_status()
                if exit_code is 0:
                    output = ''.join(stdout_chunks)
                else:
                    output = ''.join(stdout_chunks)  # TODO stderr
                return (output, exit_code)
            else:
                return True
        else:
            if wait_result:
                return (None, None)
            else:
                return False
