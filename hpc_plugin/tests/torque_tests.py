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

import logging,sys
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
        self.assertEqual(call, "module load mod1 mod2; "\
                               "qsub -V"\
                               " -N test"\
                               " -l nodes=4:ppn=24,walltime=00:05:00"\
                               " cmd")

    def test_cancellation_call(self):
        """ Jobs cancellation call. """
        response = self.wm._build_job_cancellation_call('test',
                                                      {'type': 'SBATCH'},
                                                      self.logger)
        self.assertEqual(response, "qselect -N test | xargs qdel")

    def test_identifying_job_ids_call(self):
        """ Call for revealing job ids by job names. """
        job_names = {'test_1', 'test 2'}

        # @TODO: replace by _get_jobids_by_name() as soon as the dependency on SSH client is removed.
        from hpc_plugin.utilities import shlex_quote
        response = "qstat -i `echo {} | xargs -n 1 qselect -N` | tail -n+6 | awk '{{ print $4 \" \" $1 }}'".\
            format( shlex_quote(' '.join(map(shlex_quote, job_names))) )

        self.assertEqual(response, 'qstat -i `echo \'\'"\'"\'test 2\'"\'"\' test_1\' | xargs -n 1 qselect -N` |'\
            ' tail -n+6 | awk \'{ print $4 " " $1 }\'')

    def test_identifying_job_status_call(self):
        """ Call for revealing status of jobs by job ids. """
        job_ids = {'1.some.host', '11.some.host'}

        # @TODO: replace by _get_jobids_by_name() as soon as the dependency on SSH client is removed.
        response = "qstat -i {} | tail -n+6 | awk '{{ print $1 \" \" $10 }}'".\
            format( ' '.join(job_ids) )

        self.assertEqual(response, 'qstat -i 11.some.host 1.some.host | tail -n+6 | awk \'{ print $1 " " $10 }\'')

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

    def test_parse_qstat_jobids(self):
        """ Parse JobID from qstat """
        parsed = self.wm.parse_qstat("""   test1 012345.some.host
   test2    123456.some.host
   test3    234567.some.host\n""")

        self.assertDictEqual(parsed, {'test1': '012345.some.host',
                                      'test2': '123456.some.host',
                                      'test3': '234567.some.host'})

    def test_parse_clean_qstat(self):
        """ Parse empty output from qstat. """
        parsed = self.wm.parse_qstat("\n")

        self.assertDictEqual(parsed, {})

if __name__ == '__main__':
    unittest.main()
