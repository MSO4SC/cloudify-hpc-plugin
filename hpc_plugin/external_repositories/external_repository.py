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
""" Holds the external repository common behaviour """


from hpc_plugin.ssh import SshClient


class ExternalRepository(object):

    def factory(publish_item):
        if publish_item['type'] == "CKAN":
            from ckan import Ckan
            return Ckan(publish_item)
        else:
            return None
    factory = staticmethod(factory)

    def __init__(self, publish_item):
        self.er_type = publish_item['type']

    def publish(self,
                ssh_client,
                logger,
                workdir=None):
        """
        Publish the local file in the external repository

        @type ssh_client: SshClient
        @param ssh_client: ssh client connected to an HPC login node
        @rtype string
        @return False if something went wrong
        """
        if not SshClient.check_ssh_client(ssh_client, logger):
            return False

        call = self._build_publish_call(logger)
        if call is None:
            return False

        return ssh_client.execute_shell_command(
            call,
            workdir=workdir,
            wait_result=False)

    def _build_publish_call(self, logger):
        """
        Creates a script to publish the local file

        @rtype string
        @return string with the publish call. None if an error arise.
        """
        raise NotImplementedError("'_build_publish_call' not implemented.")
