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
""" Holds the plugin workflows """

import sys
import time
import requests

from cloudify.decorators import workflow
from cloudify.workflows import ctx


BOOTFAIL = 0
CANCELLED = 1
COMPLETED = 2
CONFIGURING = 3
COMPLETING = 4
FAILED = 5
NODEFAIL = 6
PENDING = 7
PREEMPTED = 8
REVOKED = 9
RUNNING = 10
SPECIALEXIT = 11
STOPPED = 12
SUSPENDED = 13
TIMEOUT = 14

JOBSTATES = [
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "CONFIGURING",
    "COMPLETING",
    "FAILED",
    "NODE_FAIL",
    "PENDING",
    "PREEMPTED",
    "REVOKED",
    "RUNNING",
    "SPECIAL_EXIT",
    "STOPPED",
    "SUSPENDED",
    "TIMEOUT",
]


class JobGraphInstance(object):
    """ blah """

    def __init__(self, parent, instance):
        self._status = 'WAITING'
        self.parent_node = parent
        self.winstance = instance

        if parent.is_job:
            self._status = 'WAITING'

            prefix = (instance._node_instance.  # pylint: disable=W0212
                      runtime_properties["job_prefix"])

            self.monitor_url = (instance._node_instance.  # pylint: disable=W0212
                                runtime_properties["monitor_entrypoint"] +
                                instance._node_instance.  # pylint: disable=W0212
                                runtime_properties["monitor_port"])

            self.simulate = (instance._node_instance.  # pylint: disable=W0212
                             runtime_properties["simulate"])

            # build job name
            instance_components = instance.id.split('_')
            self.name = prefix + instance_components[-1]
        else:
            self._status = 'NONE'
            self.name = instance.id
            self.monitor_url = ""

    def queue(self):
        """ blah """
        if not self.parent_node.is_job:
            return

        self.winstance.send_event('Queuing HPC job..')
        result = self.winstance.execute_operation('hpc.interfaces.'
                                                  'lifecycle.queue',
                                                  kwargs={"name": self.name})
        self.winstance.send_event('..HPC job queued')
        result.task.wait_for_terminated()

        self._status = 'PENDING'
        # print result.task.dump()

    def is_finished(self):
        """ blah """
        if not self.parent_node.is_job:
            return True

        return self._status == 'FINISHED'

    def update_status(self, status):
        """ blah """
        if not status == self._status:
            self._status = status
            self.winstance.send_event('State changed to ' + self._status)


class JobGraphNode(object):
    """ blah """

    def __init__(self, node, job_instances_map):
        self.name = node.id
        self.type = node.type
        self.cfy_node = node
        self.is_job = 'hpc.nodes.job' in node.type_hierarchy

        if self.is_job:
            self.status = 'WAITING'
        else:
            self.status = 'NONE'

        self.instances = []
        for instance in node.instances:
            graph_instance = JobGraphInstance(self,
                                              instance)
            self.instances.append(graph_instance)
            if graph_instance.parent_node.is_job:
                job_instances_map[graph_instance.name] = graph_instance

        self.parents = []
        self.children = []
        self.parent_depencencies_left = 0

    def add_parent(self, node):
        """ blah """
        self.parents.append(node)
        self.parent_depencencies_left += 1

    def add_child(self, node):
        """ blah """
        self.children.append(node)

    def queue_all_instances(self):
        """ blah """
        if not self.is_job:
            return

        for job_instance in self.instances:
            job_instance.queue()
        self.status = 'QUEUED'

    def is_ready(self):
        """ blah """
        return self.parent_depencencies_left == 0

    def _remove_children_dependency(self):
        """ blah """
        for child in self.children:
            child.parent_depencencies_left -= 1

    def check_finished(self):
        """
        Check if all instances has finished

        If all of them have finished, changes node status as well
        """
        if not self.status == 'FINISHED':
            if self.status == 'NONE':
                self._remove_children_dependency()
                self.status = 'FINISHED'
            else:
                all_finished = True
                for job_instance in self.instances:
                    if not job_instance.is_finished():
                        all_finished = False
                        break

                if all_finished:
                    # The job node just finished, remove this dependency
                    self.status = 'FINISHED'
                    self._remove_children_dependency()

        return self.status == 'FINISHED'

    def get_children_ready(self):
        """ blah """
        readys = []
        for child in self.children:
            if child.is_ready():
                readys.append(child)
        return readys

    def __str__(self):
        to_print = self.name + '\n'
        for instance in self.instances:
            to_print += '- ' + instance.name + '\n'
        for child in self.children:
            to_print += '    ' + child.name + '\n'
        return to_print


def build_graph(nodes):
    """ blah """

    job_instances_map = {}

    # first create node structure
    nodes_map = {}
    root_nodes = []
    for node in nodes:
        new_node = JobGraphNode(node, job_instances_map)
        nodes_map[node.id] = new_node
        # check if it is root node
        try:
            node.relationships.next()
        except StopIteration:
            root_nodes.append(new_node)

    # then set relationships
    for _, child in nodes_map.iteritems():
        for relationship in child.cfy_node.relationships:
            parent = nodes_map[relationship.target_node.id]
            parent.add_child(child)
            child.add_parent(parent)

    return root_nodes, job_instances_map


class Monitor(object):
    """ blah """

    def __init__(self, job_instances_map):
        self.job_ids = {}
        self._execution_pool = {}
        self.timestamp = 0
        self.job_instances_map = job_instances_map

    def check_status(self):
        """ blah """

        # first get the instances we need to check
        instances_map = {}
        for _, job_node in self.get_executions_iterator():
            if job_node.is_job:
                for job_instance in job_node.instances:
                    if not job_instance.simulate:
                        if job_instance.monitor_url in instances_map:
                            instances_map[job_instance.monitor_url].append(
                                job_instance)
                        else:
                            instances_map[job_instance.monitor_url] = [
                                job_instance]
                    else:
                        job_instance.update_status('FINISHED')

        # nothing to do if we don't have nothing to monitor
        if not instances_map:
            return

        # then look for the status of the instances through its name
        states = self._get_states(instances_map)
        for inst_name, state in states.iteritems():
            # FIXME(emepetres): contemplate failed states
            if state == 'BOOT_FAIL' or \
                    state == 'CANCELLED' or \
                    state == 'COMPLETED' or \
                    state == 'FAILED' or \
                    state == 'PREEMPTED' or \
                    state == 'REVOKED' or \
                    state == 'TIMEOUT':
                state = 'FINISHED'

            self.job_instances_map[inst_name].update_status(state)

    def _get_states(self, instances_map):
        states = {}

        # We wait to not sature the monitors
        seconds_to_wait = 15 - (time.time() - self.timestamp)
        if seconds_to_wait > 0:
            sys.stdout.flush()  # necessary to output work properly wiht sleep
            time.sleep(seconds_to_wait)

        self.timestamp = time.time()

        for url, instances in instances_map:
            if len(instances) == 1:
                query = ('http://' + url + '/api/v1/query?query=job_status'
                         '%7Bjob%3D%22FT2%22%2Cname%3D%22')
            else:
                query = ('http://' + url + '/api/v1/query?query=job_status'
                         '%7Bjob%3D%22FT2%22%2Cname%3D~%22')
            query += '|'.join(instances) + '%22%7D'

            payload = requests.get(query)

            response = payload.json()

            for item in response["data"]["result"]:
                states[item["metric"]["name"]
                       ] = JOBSTATES[int(item["value"][1])]
        return states

    def get_executions_iterator(self):
        """ blah """
        return self._execution_pool.iteritems()

    def add_node(self, node):
        """ blah """
        self._execution_pool[node.name] = node

    def finish_node(self, node_name):
        """ blah """
        del self._execution_pool[node_name]

    def is_something_executing(self):
        """ blah """
        return self._execution_pool


@workflow
def run_jobs(**kwargs):  # pylint: disable=W0613
    """ Workflow to execute long running batch operations """

    root_nodes, job_instances_map = build_graph(ctx.nodes)
    monitor = Monitor(job_instances_map)

    # Execution of first job instances
    for root in root_nodes:
        root.queue_all_instances()
        monitor.add_node(root)

    # Monitoring and next executions loop
    while monitor.is_something_executing():
        # Monitor the infrastructure
        monitor.check_status()
        exec_nodes_finished = []
        new_exec_nodes = []
        for node_name, exec_node in monitor.get_executions_iterator():
            # TODO(emepetres): support different states
            if exec_node.check_finished():
                exec_nodes_finished.append(node_name)
                new_nodes_to_execute = exec_node.get_children_ready()
                for new_node in new_nodes_to_execute:
                    new_exec_nodes.append(new_node)
        # remove finished nodes
        for node_name in exec_nodes_finished:
            monitor.finish_node(node_name)
        # perform new executions
        for new_node in new_exec_nodes:
            new_node.queue_all_instances()
            monitor.add_node(new_node)

    return
