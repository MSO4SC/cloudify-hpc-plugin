########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

""" Setuptools module file """
from setuptools import setup

# Replace the place holders with values for your project

setup(

    # Do not use underscores in the plugin name.
    name='hpc',

    version='0.5',
    author='Javier Carnero',
    author_email='javier.carnero@atos.net',
    description='Plugin to use HPC resources in Cloudify',

    # This must correspond to the actual packages in the plugin.
    packages=['hpc_plugin',
              'hpc_plugin.workload_managers'],

    zip_safe=False,
    install_requires=[
        # Necessary dependency for developing plugins, do not remove!
        "cloudify-plugins-common",
        "paramiko",
        "pyyaml"
    ],
    license='LICENSE'
)
