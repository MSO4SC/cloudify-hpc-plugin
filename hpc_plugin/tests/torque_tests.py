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

""" Holds the Torque unit tests """
import unittest

import logging
from hpc_plugin.utilities import shlex_quote
from hpc_plugin.workload_managers.workload_manager import WorkloadManager


class TestTorque(unittest.TestCase):
    """ Holds Torque tests. """

    def __init__(self, methodName='runTest'):
        super(TestTorque, self).__init__(methodName)
        self.wm = WorkloadManager.factory("TORQUE")
        self.logger = logging.getLogger('TestTorque')

    def test_bad_type_name(self):
        """ Bad name type. """
        response = self.wm._build_job_submission_call(42,
                                                      {'command': 'cmd',
                                                       'type': 'SBATCH'},
                                                      self.logger)
        self.assertIn('error', response)

    def test_bad_type_settings(self):
        """ Bad type settings. """
        response = self.wm._build_job_submission_call('test',
                                                      'bad type',
                                                      self.logger)
        self.assertIn('error', response)

    def test_bad_settings_command_type(self):
        """ Bad type settings. """
        response = self.wm._build_job_submission_call('test',
                                                      'bad type',
                                                      self.logger)
        self.assertIn('error', response)

    def test_empty_settings(self):
        """ Empty job settings. """
        response = self.wm._build_job_submission_call('test',
                                                      {},
                                                      self.logger)
        self.assertIn('error', response)

    def test_invalid_type_settings(self):
        """ Incomplete job settings with invalid type. """
        response = self.wm._build_job_submission_call('test',
                                                      {'command': 'cmd',
                                                       'type': 'BAD'},
                                                      self.logger)
        self.assertIn('error', response)

    def test_only_command_settings(self):
        """ Incomplete job settings: specify only command. """
        response = self.wm._build_job_submission_call('test',
                                                      {'command': 'cmd'},
                                                      self.logger)
        self.assertIn('error', response)

    def test_basic_batch_call(self):
        """ Basic batch call. """
        response = self.wm._build_job_submission_call('test',
                                                      {'command': 'cmd',
                                                       'type': 'SBATCH'},
                                                      self.logger)
        self.assertNotIn('error', response)
        self.assertIn('call', response)

        call = response['call']
        self.assertEqual(call, "qsub -V -N test cmd")

    def test_complete_batch_call(self):
        """ Complete batch call. """
        response = self.wm._build_job_submission_call(
            'test',
            dict(modules=['mod1', 'mod2'],
                 type='SBATCH',
                 command='cmd',
                 partition='thinnodes',
                 nodes=4,
                 tasks=96,
                 tasks_per_node=24,
                 max_time='00:05:00'),
            self.logger)
        self.assertNotIn('error', response)
        self.assertIn('call', response)

        call = response['call']
        self.assertEqual(call, "module load mod1 mod2; "
                               "qsub -V"
                               " -N test"
                               " -l nodes=4:ppn=24,walltime=00:05:00"
                               " cmd")

    def test_batch_call_with_job_array(self):
        """ Complete batch call. """
        response = self.wm._build_job_submission_call(
            'test',
            dict(modules=['mod1', 'mod2'],
                 type='SBATCH',
                 command='cmd',
                 partition='thinnodes',
                 nodes=4,
                 tasks=96,
                 tasks_per_node=24,
                 max_time='00:05:00',
                 scale=10,
                 scale_max_in_parallel=2),
            self.logger)
        self.assertNotIn('error', response)
        self.assertIn('call', response)

        call = response['call']
        self.assertEqual(call, "module load mod1 mod2; "
                               "qsub -V"
                               " -N test"
                               " -l nodes=4:ppn=24,walltime=00:05:00"
                               " -J 0-9%2"
                               " cmd")
        scale_env_mapping_call = response['scale_env_mapping_call']
        self.assertEqual(scale_env_mapping_call,
                         "sed -i ':a;N;$! ba;s/\\n.*#SBATCH.*\\n/&"
                         "SCALE_INDEX=$PBS_ARRAYID\\n"
                         "SCALE_COUNT=10\\n"
                         "SCALE_MAX=2\\n\\n/' cmd")

    def test_cancellation_call(self):
        """ Jobs cancellation call. """
        response = self.wm._build_job_cancellation_call('test',
                                                        {'type': 'SBATCH'},
                                                        self.logger)
        self.assertEqual(response, "qselect -N test | xargs qdel")

    @unittest.skip("deprecated")
    def test_identifying_job_ids_call(self):
        """ Call for revealing job ids by job names. """
        job_names = ('test_1', 'test 2')

        # @TODO: replace by _get_jobids_by_name() as soon as the dependency on
        #        SSH client is removed.
        from hpc_plugin.utilities import shlex_quote
        response = "qstat -i `echo {} | xargs -n 1 qselect -N` |"\
                   " tail -n+6 | awk '{{ print $4 \" \" $1 }}'".format(
                       shlex_quote(' '.join(map(shlex_quote, job_names))))

        self.assertEqual(response, 'qstat -i '
                         '`echo \'\'"\'"\'test 2\'"\'"\' test_1\' |'
                         ' xargs -n 1 qselect -N` |'
                         ' tail -n+6 | awk \'{ print $4 " " $1 }\'')

    @unittest.skip("deprecated")
    def test_identifying_job_status_call(self):
        """ Call for revealing status of jobs by job ids. """
        job_ids = {'1.some.host', '11.some.host'}

        # @TODO: replace by _get_jobids_by_name() as soon as the dependency on
        #        SSH client is removed.
        response = "qstat -i {} | tail -n+6 | awk '{{ print $1 \" \" $10 }}'".\
            format(' '.join(job_ids))

        self.assertEqual(
            response, 'qstat -i 11.some.host 1.some.host |'
            ' tail -n+6 | awk \'{ print $1 " " $10 }\'')

    def test_get_states(self):
        """ Call for revealing job ids by job names. """
        from hpc_plugin.cli_client.cli_client import ICliClient

        job_names = ('test_1', 'test 2')

        class MockClient(ICliClient):
            def __init__(self, test_case):
                self._test_case = test_case

            def is_open(self):
                return True

            def send_command(self, command, **kwargs):
                expected_cmd = 'qstat -i'\
                               ' `echo {} '\
                               '| xargs -n 1 qselect -N` '\
                               '| tail -n+6 '\
                               '| awk \'{{ print $4 "|" $10 }}\''.format(
                                   shlex_quote(' '.join(
                                       map(shlex_quote, job_names))))
                self._test_case.assertEqual(command, expected_cmd)
                return """   test_1 | S
   test 2   | R\n""", 0

        response = self.wm.get_states_tabular(
            MockClient(self), job_names, self.logger)
        self.assertDictEqual(
            response, {'test_1': 'SUSPENDED', 'test 2': 'RUNNING'})

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

    def test_parse_qstat_job_states(self):
        """ Parse JobID from qstat """
        parsed = self.wm._parse_qstat_tabular("""   test1 | S
   test2   | C
   test3   | R
   test4   | W\n""")

        self.assertDictEqual(parsed, {'test1': 'SUSPENDED',
                                      'test2': 'COMPLETED',
                                      'test3': 'RUNNING',
                                      'test4': 'PENDING'})

    def test_parse_clean_qstat(self):
        """ Parse empty output from qstat. """
        parsed = self.wm._parse_qstat_tabular("\n")

        self.assertDictEqual(parsed, {})


if __name__ == '__main__':
    unittest.main()
