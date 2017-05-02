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

""" Holds the Slurm unit tests """
import unittest
from hpc_plugin import slurm


class TestSlurm(unittest.TestCase):
    """ Holds slurm tests """

    def test_bad_type_name(self):
        """ Test sending bad name type """
        call = slurm.get_slurm_call(42, {'command': 'cmd',
                                         'type': 'SBATCH'})
        self.assertIsNone(call)

    def test_bad_type_settings(self):
        """ Test sending bad type settings """
        call = slurm.get_slurm_call('test', 'bad type')
        self.assertIsNone(call)

    def test_bad_settings_command_type(self):
        """ Test sending bad type settings """
        call = slurm.get_slurm_call('test', 'bad type')
        self.assertIsNone(call)

    def test_empty_settings(self):
        """ Test sending empty job settings """
        call = slurm.get_slurm_call('test', {})
        self.assertIsNone(call)

    def test_only_type_settings(self):
        """ Test sending only type as job settings """
        call = slurm.get_slurm_call('test', {'command': 'cmd',
                                             'type': 'BAD'})
        self.assertIsNone(call)

    def test_only_command_settings(self):
        """ Test sending only command as job settings """
        call = slurm.get_slurm_call('test', {'command': 'cmd'})
        self.assertIsNone(call)

    def test_notime_srun_call(self):
        """ Test srun command without max time set """
        call = slurm.get_slurm_call('test', {'command': 'cmd',
                                             'type': 'SRUN'})
        self.assertIsNone(call)

    def test_basic_srun_call(self):
        """ Test basic srun command """
        call = slurm.get_slurm_call('test', {'command': 'cmd',
                                             'type': 'SRUN',
                                             'max_time': '05:00'})
        self.assertEqual(call, "nohup srun -J 'test' -t 05:00 cmd &")

    def test_complete_srun_call(self):
        """ Test complete srun command """
        call = slurm.get_slurm_call('test', {'modules': ['mod1', 'mod2'],
                                             'type': 'SRUN',
                                             'command': 'cmd',
                                             'partition': 'thinnodes',
                                             'nodes': 4,
                                             'tasks': 96,
                                             'tasks_per_node': 24,
                                             'max_time': '05:00'})
        self.assertEqual(call, "module load mod1 mod2; "
                               "nohup srun -J 'test'"
                               " -p thinnodes"
                               " -N 4"
                               " -n 96"
                               " --ntasks-per-node=24"
                               " -t 05:00"
                               " cmd &")

    def test_basic_sbatch_call(self):
        """ Test basic sbatch command """
        call = slurm.get_slurm_call('test', {'command': 'cmd',
                                             'type': 'SBATCH'})
        self.assertEqual(call, "sbatch -J 'test' cmd")

    def test_complete_sbatch_call(self):
        """ Test complete sbatch command """
        call = slurm.get_slurm_call('test', {'modules': ['mod1', 'mod2'],
                                             'type': 'SBATCH',
                                             'command': 'cmd',
                                             'partition': 'thinnodes',
                                             'nodes': 4,
                                             'tasks': 96,
                                             'tasks_per_node': 24,
                                             'max_time': '05:00'})
        self.assertEqual(call, "module load mod1 mod2; "
                               "sbatch -J 'test'"
                               " -p thinnodes"
                               " -N 4"
                               " -n 96"
                               " --ntasks-per-node=24"
                               " -t 05:00"
                               " cmd")
