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
import logging
import unittest
from hpc_plugin.workload_managers.slurm import WorkloadManager


class TestSlurm(unittest.TestCase):
    """ Holds slurm tests """

    def __init__(self, methodName='runTest'):
        super(TestSlurm, self).__init__(methodName)
        self.wm = WorkloadManager.factory("SLURM")
        self.logger = logging.getLogger('TestSlurm')

    def test_bad_type_name(self):
        """ Bad name type """
        response = self.wm._build_job_submission_call(42,
                                                      {'command': 'cmd',
                                                       'type': 'SBATCH'},
                                                      self.logger)
        self.assertIn('error', response)

    def test_bad_type_settings(self):
        """ Bad type settings """
        response = self.wm._build_job_submission_call('test',
                                                      'bad type',
                                                      self.logger)
        self.assertIn('error', response)

    def test_bad_settings_command_type(self):
        """ Bad type settings """
        response = self.wm._build_job_submission_call('test',
                                                      'bad type',
                                                      self.logger)
        self.assertIn('error', response)

    def test_empty_settings(self):
        """ Empty job settings """
        response = self.wm._build_job_submission_call('test',
                                                      {},
                                                      self.logger)
        self.assertIn('error', response)

    def test_only_type_settings(self):
        """ Type only as job settings """
        response = self.wm._build_job_submission_call('test',
                                                      {'command': 'cmd',
                                                       'type': 'BAD'},
                                                      self.logger)
        self.assertIn('error', response)

    def test_only_command_settings(self):
        """ Command only as job settings. """
        response = self.wm._build_job_submission_call('test',
                                                      {'command': 'cmd'},
                                                      self.logger)
        self.assertIn('error', response)

    def test_notime_srun_call(self):
        """ srun command without max time set. """
        response = self.wm._build_job_submission_call('test',
                                                      {'command': 'cmd',
                                                       'type': 'SRUN'},
                                                      self.logger)
        self.assertIn('error', response)

    def test_basic_srun_call(self):
        """ Basic srun command. """
        response = self.wm._build_job_submission_call('test',
                                                      {'command': 'cmd',
                                                       'type': 'SRUN',
                                                       'max_time': '00:05:00'},
                                                      self.logger)
        self.assertNotIn('error', response)
        self.assertIn('call', response)

        call = response['call']
        self.assertEqual(call, "nohup srun -J 'test' -t 00:05:00 cmd &")

    def test_complete_srun_call(self):
        """ Complete srun command. """
        response = self.wm._build_job_submission_call('test',
                                                      {'modules': ['mod1',
                                                                   'mod2'],
                                                       'type': 'SRUN',
                                                       'command': 'cmd',
                                                       'partition':
                                                       'thinnodes',
                                                       'nodes': 4,
                                                       'tasks': 96,
                                                       'tasks_per_node': 24,
                                                       'max_time': '05:00'},
                                                      self.logger)
        self.assertNotIn('error', response)
        self.assertIn('call', response)

        call = response['call']
        self.assertEqual(call, "module load mod1 mod2; "
                               "nohup srun -J 'test'"
                               " -p thinnodes"
                               " -N 4"
                               " -n 96"
                               " --ntasks-per-node=24"
                               " -t 05:00"
                               " cmd &")

    def test_basic_sbatch_call(self):
        """ Basic sbatch command. """
        response = self.wm._build_job_submission_call('test',
                                                      {'command': 'cmd',
                                                       'type': 'SBATCH'},
                                                      self.logger)
        self.assertNotIn('error', response)
        self.assertIn('call', response)

        call = response['call']
        self.assertEqual(call, "sbatch --parsable -J 'test' cmd")

    def test_complete_sbatch_call(self):
        """ Complete sbatch command. """
        response = self.wm._build_job_submission_call('test',
                                                      {'modules': ['mod1',
                                                                   'mod2'],
                                                       'type': 'SBATCH',
                                                       'command': 'cmd',
                                                       'partition':
                                                       'thinnodes',
                                                       'nodes': 4,
                                                       'tasks': 96,
                                                       'tasks_per_node': 24,
                                                       'max_time': '00:05:00'},
                                                      self.logger)
        self.assertNotIn('error', response)
        self.assertIn('call', response)

        call = response['call']
        self.assertEqual(call, "module load mod1 mod2; "
                               "sbatch --parsable -J 'test'"
                               " -p thinnodes"
                               " -N 4"
                               " -n 96"
                               " --ntasks-per-node=24"
                               " -t 00:05:00"
                               " cmd")

    def test_random_name(self):
        """ Random name formation. """
        name = self.wm._get_random_name('base')

        self.assertEqual(11, len(name))
        self.assertEqual('base_', name[:5])

    def test_random_name_uniqueness(self):
        """ Random name uniqueness. """
        names = []
        for _ in range(0, 50):
            names.append(self.wm._get_random_name('base'))

        self.assertEqual(len(names), len(set(names)))

    def test_parse_sacct_jobid(self):
        """ Parse JobID from sacct """
        parsed = self.wm.parse_sacct("   test1 012345\n  test2     "
                                     "123456\n   test3    234567\n")

        self.assertDictEqual(parsed, {'test1': '012345',
                                      'test2': '123456',
                                      'test3': '234567'})

    def test_parse_clean_sacct(self):
        """ Parse no output from sacct """
        parsed = self.wm.parse_sacct("\n")

        self.assertDictEqual(parsed, {})
