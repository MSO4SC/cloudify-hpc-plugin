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

"""Wrap of paramiko to send ssh commands

Todo:
    * read stderr and return it
    * control SSH exceptions and return failures
"""

from hpc_plugin.cli_client.cli_client import CliClient
from hpc_plugin.utilities  import shlex_quote

class DummyClient(CliClient):
    """Represents a stub for SSH client that prints out SSH calls without executing them.
    It can be used for debug purposes and generating shell scripts."""

    def __init__(self, address, username, password, port=22, use_login_shell = False, **kwargs):
        # print "Connecting to server ", str(address)+":"+str(port)
        self._call_prefix = "ssh {username}@{address}{port}".\
            format(address=address, username=username, password=password, port=":" + str(port) if port != 22 else '')

        # This switch allows to execute commands in a login shell.
        # By default commands are executed on the remote host.
        self._use_login_shell = use_login_shell

    def is_open(self):
        """Check if connection is open"""
        return True

    def close_connection(self):
        pass

    def send_command(self, command, **kwargs):
        """Sends a command and returns stdout, stderr and exitcode"""
        print(r"{0} {1}".format(self._call_prefix, shlex_quote("bash -l -c " + shlex_quote(command) \
            if self._use_login_shell else command)))
        return '', 0


class Logger(object):
    """Represents a stub for logger.
    It can be used for debug purposes in combination with `dummy_client.SshClient`"""
    def info(s):
        sys.stdout.write('# INF ' + s + '\n')

    def error(s):
        sys.stderr.write('# ERR ' + s + '\n')
