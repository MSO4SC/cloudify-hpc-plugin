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
""" Ckan specific communication to publish data """


from external_repository import ExternalRepository


class Ckan(ExternalRepository):

    def __init__(self, publish_item):
        super(Ckan, self).__init__(publish_item)

        self.entrypoint = publish_item['entrypoint']
        self.api_key = publish_item['api_key']
        self.dataset = publish_item['dataset']
        self.file_path = publish_item['file_path']
        self.name = publish_item['name']
        self.description = publish_item["description"]

    def _build_publish_call(self, logger):
        # detach call??
        operation = "create"
        # TODO: if resource file exists, operation="update"
        call = "curl -H'Authorization: " + self.api_key + "' " + \
            "'" + self.entrypoint + "/api/action/resource_" + \
            operation + "' " + \
            "--form upload=@" + self.file_path + " " + \
            "--form package_id=" + self.dataset + " " + \
            "--form name=" + self.name + " " + \
            "--form description='" + self.description + "'"
        logger.info('Ckan/_build_publish_callJob: ' + call)
        return call
