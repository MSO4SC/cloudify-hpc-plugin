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
import os
import unittest
import logging
import yaml

from cloudify.test_utils import workflow_test


class TestPlugin(unittest.TestCase):
    """ Test workflows class """

    def set_inputs(self, *args, **kwargs):  # pylint: disable=W0613
        """ Parse inputs yaml file """
        # Chech whether a local inputs file is available
        inputs_file = 'blueprint-inputs.yaml'
        if os.path.isfile(os.path.join('hpc_plugin',
                                       'tests',
                                       'inputs',
                                       'local-blueprint-inputs.yaml')):
            inputs_file = 'local-blueprint-inputs.yaml'
        inputs = {}
        with open(os.path.join('hpc_plugin',
                               'tests',
                               'inputs',
                               inputs_file),
                  'r') as stream:
            try:
                inputs = yaml.load(stream)
            except yaml.YAMLError as exc:
                print exc

        return inputs

    @workflow_test(os.path.join('blueprints', 'blueprint_srun.yaml'),
                   copy_plugin_yaml=True,
                   resources_to_copy=[(os.path.join('blueprints',
                                                    'inputs_def.yaml'),
                                       './'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'bootstrap_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'revert_example.sh'),
                                       'scripts')],
                   inputs='set_inputs')
    def test_srun(self, cfy_local):
        """ Single SRUN Job Blueprint """
        cfy_local.execute('install', task_retries=0)
        cfy_local.execute('run_jobs', task_retries=0)
        cfy_local.execute('uninstall', task_retries=0)

        # extract single node instance
        instance = cfy_local.storage.get_node_instances()[0]

        # due to a cfy bug sometimes login keyword is not ready in the tests
        if 'login' in instance.runtime_properties:
            # assert runtime properties is properly set in node instance
            self.assertEqual(instance.runtime_properties['login'],
                             True)
        else:
            logging.warning('[WARNING] Login could not be tested')

    @workflow_test(os.path.join('blueprints', 'blueprint_sbatch.yaml'),
                   copy_plugin_yaml=True,
                   resources_to_copy=[(os.path.join('blueprints',
                                                    'inputs_def.yaml'),
                                       './'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'bootstrap_' +
                                                    'sbatch_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'revert_' +
                                                    'sbatch_example.sh'),
                                       'scripts')],
                   inputs='set_inputs')
    def test_sbatch(self, cfy_local):
        """ Single SBATCH Job Blueprint """
        cfy_local.execute('install', task_retries=0)
        cfy_local.execute('run_jobs', task_retries=0)
        cfy_local.execute('uninstall', task_retries=0)

        # extract single node instance
        instance = cfy_local.storage.get_node_instances()[0]

        # due to a cfy bug sometimes login keyword is not ready in the tests
        if 'login' in instance.runtime_properties:
            # assert runtime properties is properly set in node instance
            self.assertEqual(instance.runtime_properties['login'],
                             True)
        else:
            logging.warning('[WARNING] Login could not be tested')

    @workflow_test(os.path.join('blueprints', 'blueprint_sbatch_output.yaml'),
                   copy_plugin_yaml=True,
                   resources_to_copy=[(os.path.join('blueprints',
                                                    'inputs_def.yaml'),
                                       './'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'bootstrap_' +
                                                    'sbatch_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'revert_' +
                                                    'sbatch_example.sh'),
                                       'scripts')],
                   inputs='set_inputs')
    def test_sbatch_output(self, cfy_local):
        """ Single SBATCH Output Job Blueprint """
        cfy_local.execute('install', task_retries=0)
        cfy_local.execute('run_jobs', task_retries=0)
        cfy_local.execute('uninstall', task_retries=0)

        # extract single node instance
        instance = cfy_local.storage.get_node_instances()[0]

        # due to a cfy bug sometimes login keyword is not ready in the tests
        if 'login' in instance.runtime_properties:
            # assert runtime properties is properly set in node instance
            self.assertEqual(instance.runtime_properties['login'],
                             True)
        else:
            logging.warning('[WARNING] Login could not be tested')

    @workflow_test(os.path.join('blueprints', 'blueprint_sbatch_scale.yaml'),
                   copy_plugin_yaml=True,
                   resources_to_copy=[(os.path.join('blueprints',
                                                    'inputs_def.yaml'),
                                       './'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'bootstrap_' +
                                                    'sbatch_scale_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'revert_' +
                                                    'sbatch_example.sh'),
                                       'scripts')],
                   inputs='set_inputs')
    def test_sbatch_scale(self, cfy_local):
        """ SBATCH Scale Job Blueprint """
        cfy_local.execute('install', task_retries=0)
        cfy_local.execute('run_jobs', task_retries=0)
        cfy_local.execute('uninstall', task_retries=0)

        # extract single node instance
        instance = cfy_local.storage.get_node_instances()[0]

        # due to a cfy bug sometimes login keyword is not ready in the tests
        if 'login' in instance.runtime_properties:
            # assert runtime properties is properly set in node instance
            self.assertEqual(instance.runtime_properties['login'],
                             True)
        else:
            logging.warning('[WARNING] Login could not be tested')

    @workflow_test(os.path.join('blueprints', 'blueprint_singularity.yaml'),
                   copy_plugin_yaml=True,
                   resources_to_copy=[(os.path.join('blueprints',
                                                    'inputs_def.yaml'),
                                       './'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'singularity_' +
                                                    'bootstrap_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'singularity_' +
                                                    'revert_example.sh'),
                                       'scripts')],
                   inputs='set_inputs')
    def test_singularity(self, cfy_local):
        """ Single Singularity Job Blueprint """
        cfy_local.execute('install', task_retries=0)
        cfy_local.execute('run_jobs', task_retries=0)
        cfy_local.execute('uninstall', task_retries=0)

        # extract single node instance
        instance = cfy_local.storage.get_node_instances()[0]

        # due to a cfy bug sometimes login keyword is not ready in the tests
        if 'login' in instance.runtime_properties:
            # assert runtime properties is properly set in node instance
            self.assertEqual(instance.runtime_properties['login'],
                             True)
        else:
            logging.warning('[WARNING] Login could not be tested')

    @workflow_test(os.path.join('blueprints',
                                'blueprint_singularity_scale.yaml'),
                   copy_plugin_yaml=True,
                   resources_to_copy=[(os.path.join('blueprints',
                                                    'inputs_def.yaml'),
                                       './'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'singularity_' +
                                                    'bootstrap_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'singularity_' +
                                                    'revert_example.sh'),
                                       'scripts')],
                   inputs='set_inputs')
    def test_singularity_scale(self, cfy_local):
        """ Single Singularity Sacale Job Blueprint """
        cfy_local.execute('install', task_retries=0)
        cfy_local.execute('run_jobs', task_retries=0)
        cfy_local.execute('uninstall', task_retries=0)

        # extract single node instance
        instance = cfy_local.storage.get_node_instances()[0]

        # due to a cfy bug sometimes login keyword is not ready in the tests
        if 'login' in instance.runtime_properties:
            # assert runtime properties is properly set in node instance
            self.assertEqual(instance.runtime_properties['login'],
                             True)
        else:
            logging.warning('[WARNING] Login could not be tested')

    @workflow_test(os.path.join('blueprints', 'blueprint_four.yaml'),
                   copy_plugin_yaml=True,
                   resources_to_copy=[(os.path.join('blueprints',
                                                    'inputs_def.yaml'),
                                       './'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'bootstrap_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'revert_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'singularity_' +
                                                    'bootstrap_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'singularity_' +
                                                    'revert_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'bootstrap_' +
                                                    'sbatch_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'revert_' +
                                                    'sbatch_example.sh'),
                                       'scripts')],
                   inputs='set_inputs')
    def test_four(self, cfy_local):
        """ Four Jobs Blueprint """
        cfy_local.execute('install', task_retries=0)
        cfy_local.execute('run_jobs', task_retries=0)
        cfy_local.execute('uninstall', task_retries=0)

        # extract single node instance
        instance = cfy_local.storage.get_node_instances()[0]

        # due to a cfy bug sometimes login keyword is not ready in the tests
        if 'login' in instance.runtime_properties:
            # assert runtime properties is properly set in node instance
            self.assertEqual(instance.runtime_properties['login'],
                             True)
        else:
            logging.warning('[WARNING] Login could not be tested')

    @workflow_test(os.path.join('blueprints', 'blueprint_four_scale.yaml'),
                   copy_plugin_yaml=True,
                   resources_to_copy=[(os.path.join('blueprints',
                                                    'inputs_def.yaml'),
                                       './'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'bootstrap_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'revert_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'singularity_' +
                                                    'bootstrap_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'singularity_' +
                                                    'revert_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'bootstrap_' +
                                                    'sbatch_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'revert_' +
                                                    'sbatch_example.sh'),
                                       'scripts')],
                   inputs='set_inputs')
    def test_four_scale(self, cfy_local):
        """ Four Scale Jobs Blueprint """
        cfy_local.execute('install', task_retries=0)
        cfy_local.execute('run_jobs', task_retries=0)
        cfy_local.execute('uninstall', task_retries=0)

        # extract single node instance
        instance = cfy_local.storage.get_node_instances()[0]

        # due to a cfy bug sometimes login keyword is not ready in the tests
        if 'login' in instance.runtime_properties:
            # assert runtime properties is properly set in node instance
            self.assertEqual(instance.runtime_properties['login'],
                             True)
        else:
            logging.warning('[WARNING] Login could not be tested')

    # # It doesn't allow "simulate" property. Code is left for manual testing.
    # @workflow_test(os.path.join('blueprints', 'blueprint_openstack.yaml'),
    #                copy_plugin_yaml=True,
    #                resources_to_copy=[(os.path.join('blueprints',
    #                                                 'inputs_def.yaml'),
    #                                    './'),
    #                                   (os.path.join('blueprints', 'scripts',
    #                                                 'singularity_' +
    #                                                 'bootstrap_example.sh'),
    #                                    'scripts'),
    #                                   (os.path.join('blueprints', 'scripts',
    #                                                 'singularity_' +
    #                                                 'revert_example.sh'),
    #                                    'scripts')],
    #                inputs='set_inputs')
    # def test_openstack(self, cfy_local):
    #     """ Openstack Blueprint """
    #     cfy_local.execute('install', task_retries=5)
    #     cfy_local.execute('run_jobs', task_retries=0)
    #     cfy_local.execute('uninstall', task_retries=0)

    #     # extract single node instance
    #     instance = cfy_local.storage.get_node_instances()[0]

    #     # due to a cfy bug sometimes login keyword is not ready in the tests
    #     if 'login' in instance.runtime_properties:
    #         # assert runtime properties is properly set in node instance
    #         self.assertEqual(instance.runtime_properties['login'],
    #                          True)
    #     else:
    #         logging.warning('[WARNING] Login could not be tested')

    @workflow_test(os.path.join('blueprints', 'blueprint_eosc.yaml'),
                   copy_plugin_yaml=True,
                   resources_to_copy=[(os.path.join('blueprints',
                                                    'inputs_def.yaml'),
                                       './'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'singularity_' +
                                                    'bootstrap_example.sh'),
                                       'scripts'),
                                      (os.path.join('blueprints', 'scripts',
                                                    'singularity_' +
                                                    'revert_example.sh'),
                                       'scripts')],
                   inputs='set_inputs')
    def test_eosc(self, cfy_local):
        """ EOSC Blueprint """
        cfy_local.execute('install', task_retries=0)
        cfy_local.execute('run_jobs', task_retries=0)
        cfy_local.execute('uninstall', task_retries=0)

        # extract single node instance
        instance = cfy_local.storage.get_node_instances()[0]

        # due to a cfy bug sometimes login keyword is not ready in the tests
        if 'login' in instance.runtime_properties:
            # assert runtime properties is properly set in node instance
            self.assertEqual(instance.runtime_properties['login'],
                             True)
        else:
            logging.warning('[WARNING] Login could not be tested')

    @workflow_test(os.path.join('blueprints', 'blueprint_eosc_opm.yaml'),
                   copy_plugin_yaml=True,
                   resources_to_copy=[
                        (os.path.join('blueprints',
                                      'inputs_def.yaml'),
                         './'),
                        (os.path.join('blueprints', 'scripts',
                                      'singularity_' +
                                      'bootstrap_example.sh'),
                         'scripts'),
                        (os.path.join('blueprints', 'scripts',
                                      'singularity_' +
                                      'revert_example.sh'),
                         'scripts'),
                        (os.path.join('blueprints', 'scripts',
                                      'singularity_' +
                                      'bootstrap_run-flow-generic.sh'),
                         'scripts'),
                        (os.path.join('blueprints', 'scripts',
                                      'singularity_' +
                                      'revert_run-flow-generic.sh'),
                         'scripts')],
                   inputs='set_inputs')
    def test_eosc_opm(self, cfy_local):
        """ EOSC & OPM Blueprint """
        cfy_local.execute('install', task_retries=0)
        cfy_local.execute('run_jobs', task_retries=0)
        cfy_local.execute('uninstall', task_retries=0)

        # extract single node instance
        instance = cfy_local.storage.get_node_instances()[0]

        # due to a cfy bug sometimes login keyword is not ready in the tests
        if 'login' in instance.runtime_properties:
            # assert runtime properties is properly set in node instance
            self.assertEqual(instance.runtime_properties['login'],
                             True)
        else:
            logging.warning('[WARNING] Login could not be tested')


if __name__ == '__main__':
    unittest.main()
