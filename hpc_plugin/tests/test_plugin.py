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

""" Holds the unit tests """
from os import path
import unittest

from cloudify.test_utils import workflow_test


class TestPlugin(unittest.TestCase):
    """ Test plugin class """
    @workflow_test(path.join('blueprint', 'blueprint.yaml'),
                   resources_to_copy=[(path.join('blueprint', 'hpc_plugin',
                                                 'test_plugin.yaml'),
                                       'hpc_plugin')],
                   inputs={'ft2_credentials': path.join('hpc_plugin',
                                                        'tests',
                                                        'blueprint',
                                                        'credentials',
                                                        'cesga.json')})
    def test_my_task(self, cfy_local):
        """

        :param cfy_local:
        """
        cfy_local.execute('install', task_retries=0)

        # extract single node instance
        instance = cfy_local.storage.get_node_instances()[0]

        # assert runtime properties is properly set in node instance
        self.assertEqual(instance.runtime_properties['login'],
                         True)

        # assert deployment outputs are ok
        self.assertDictEqual(cfy_local.outputs(),
                             {'job_command': 'touch job.test'})
