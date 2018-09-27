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
#
# History:
#
# 2018-02-07   hpcgogol@hlrs.de     added option login_shell
#

"""Wrap of paramiko to send ssh commands

Todo:
    * read stderr and return it
    * control SSH exceptions and return failures
"""
import select
import thread
import logging
import io
import socket

try:
    import SocketServer
except ImportError:
    import socketserver as SocketServer

from paramiko import client, RSAKey, ssh_exception
from hpc_plugin.utilities import shlex_quote

# # @TODO `posixpath` can be used for common pathname manipulations on
# #       remote HPC systems
# import posixpath as cli_path

logging.getLogger("paramiko").setLevel(logging.WARNING)


class SshClient(object):
    """Represents a ssh client"""
    _client = None

    def __init__(self, credentials):
        # Build a tunnel if necessary
        self._tunnel = None
        self._host = credentials['host']
        self._port = int(credentials['port']) if 'port' in credentials else 22
        if 'tunnel' in credentials:
            self._tunnel = SshForward(credentials)
            self._host = "localhost"
            self._port = self._tunnel.port()

        self._client = client.SSHClient()
        self._client.set_missing_host_key_policy(client.AutoAddPolicy())

        # Build the private key if provided
        private_key = None
        if 'private_key' in credentials and credentials['private_key']:
            key_data = credentials['private_key']
            if not isinstance(key_data, unicode):
                key_data = unicode(key_data, "utf-8")
            key_file = io.StringIO()
            key_file.write(key_data)
            key_file.seek(0)
            if 'private_key_password' in credentials and \
                    credentials['private_key_password'] != "":
                private_key_password = credentials['private_key_password']
            else:
                private_key_password = None
            private_key = RSAKey.from_private_key(
                key_file,
                password=private_key_password)

        # This switch allows to execute commands in a login shell.
        # By default commands are executed on the remote host.
        # See discussions in the following threads:
        #   https://superuser.com/questions/306530/run-remote-ssh-command-with-full-login-shell
        #   https://stackoverflow.com/questions/32139904/ssh-via-paramiko-load-bashrc
        # @TODO: think of SSHClient.invoke_shell()
        #        instead of SSHClient.exec_command()
        self._login_shell = False
        if 'login_shell' in credentials:
            self._login_shell = credentials['login_shell']

        retries = 5
        passwd = credentials['password'] if 'password' in credentials else None
        while True:
            try:
                self._client.connect(
                    self._host,
                    port=self._port,
                    username=credentials['user'],
                    pkey=private_key,
                    password=passwd,
                    look_for_keys=False
                )
            except ssh_exception.SSHException as err:
                if retries > 0 and \
                        str(err) == "Error reading SSH protocol banner":
                    retries -= 1
                    logging.getLogger("paramiko").\
                        warning("Retrying SSH connection: "+str(err))
                    continue
                else:
                    raise err
            break

    def get_transport(self):
        """Gets the transport object of the client (paramiko)"""
        return self._client.get_transport()

    def is_open(self):
        """Check if connection is open"""
        return self._client is not None

    def close_connection(self):
        """Closes opened connection"""
        if self._client is not None:
            self._client.close()
        if self._tunnel is not None:
            self._tunnel.close()

    def execute_shell_command(self,
                              cmd,
                              workdir=None,
                              env=None,
                              wait_result=False,
                              detach=False):
        """ Execute the command remotely
        - if workdir is set: in the specific workdir
        - if defined, env is a list of string keypairs to set env variables.
        - if wait_result is set to True: blocks until it gather
          the results
        - if detach is set tu True: let the command running in the background.
          it is incompatible with wait_result=True"""
        if detach:
            wait_result = False
            cmd = "nohup " + cmd + " &"

        call = ""
        if env is not None:
            for key, value in env.iteritems():
                call += "export "+key+"=" + value + " && "

        if not workdir:
            call += cmd
            return self.send_command(call,
                                     wait_result=wait_result)
        else:
            # TODO: set scale variables as well
            call += "export CURRENT_WORKDIR=" + workdir + " && "
            call += "cd " + workdir + " && "
            call += cmd
            return self.send_command(call,
                                     wait_result=wait_result)

    def send_command(self,
                     command,
                     exec_timeout=3000,
                     read_chunk_timeout=500,
                     wait_result=False):
        """Sends a command and returns stdout, stderr and exitcode"""

        # Check if connection is made previously
        if self._client is not None:

            if self._login_shell:
                cmd = "bash -l -c {}".format(shlex_quote(command))
            else:
                cmd = command
            # there is one channel per command
            stdin, stdout, stderr = self._client.exec_command(
                cmd,
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

    @staticmethod
    def check_ssh_client(ssh_client,
                         logger):
        if not isinstance(ssh_client, SshClient) or not ssh_client.is_open():
            logger.error("SSH Client can't be used")
            return False
        return True


class SshForward(object):
    """Represents a ssh port forwarding"""

    def __init__(self, credentials):
        self._client = SshClient(credentials['tunnel'])
        self._remote_port = \
            int(credentials['port']) if 'port' in credentials else 22

        class SubHander(Handler):
            chain_host = credentials['host']
            chain_port = self._remote_port
            ssh_transport = self._client.get_transport()

        self._server = ForwardServer(("", 0), SubHander)
        self._port = self._server.server_address[1]

        thread.start_new_thread(self._server.serve_forever, ())

    def port(self):
        return self._port

    def close(self):
        self._server.shutdown()


# Following code taken from paramiko forward demo in github
# https://github.com/paramiko/paramiko/blob/master/demos/forward.py

class ForwardServer(SocketServer.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler(SocketServer.BaseRequestHandler):

    def handle(self):
        try:
            chan = self.ssh_transport.open_channel(
                "direct-tcpip",
                (self.chain_host, self.chain_port),
                self.request.getpeername(),
            )
        except Exception as e:
            verbose(
                "Incoming request to %s:%d failed: %s"
                % (self.chain_host, self.chain_port, repr(e))
            )
            return
        if chan is None:
            verbose(
                "Incoming request to %s:%d was rejected by the SSH server."
                % (self.chain_host, self.chain_port)
            )
            return

        # verbose(
        #     "Connected!  Tunnel open %r -> %r -> %r"
        #     % (
        #         self.request.getpeername(),
        #         chan.getpeername(),
        #         (self.chain_host, self.chain_port),
        #     )
        # )
        while True:
            r, w, x = select.select([self.request, chan], [], [])
            if self.request in r:
                try:
                    data = self.request.recv(1024)
                except socket.error as err:
                    data = bytes('')
                    logging.getLogger("paramiko").\
                        warning("Waiting for data: "+str(err))
                if len(data) == 0:
                    break
                chan.send(data)
            if chan in r:
                data = chan.recv(1024)
                if len(data) == 0:
                    break
                self.request.send(data)

        # peername = self.request.getpeername()
        chan.close()
        self.request.close()
        # verbose("Tunnel closed from %r" % (peername,))


def verbose(s):
    print(s)


def get_host_port(spec, default_port):
    "parse 'hostname:22' into a host and port, with the port optional"
    args = (spec.split(":", 1) + [default_port])[:2]
    args[1] = int(args[1])
    return args[0], args[1]
