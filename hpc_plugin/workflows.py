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

from cloudify.decorators import workflow
from cloudify.workflows import ctx, api
from hpc_plugin import monitors


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


class JobGraphInstance(object):
    """ Wrap to add job functionalities to node instances """

    def __init__(self, parent, instance):
        self._status = 'WAITING'
        self.parent_node = parent
        self.winstance = instance

        if parent.is_job:
            self._status = 'WAITING'

            prefix = (instance._node_instance.  # pylint: disable=W0212
                      runtime_properties["job_prefix"])

            self.monitor_type = (instance.  # pylint: disable=W0212
                                 _node_instance.runtime_properties[
                                     "monitor_type"])
            self.host = (instance.  # pylint: disable=W0212
                         _node_instance.runtime_properties[
                             "credentials"]["host"])
            self.monitor_url = ('http://' + instance.  # pylint: disable=W0212
                                _node_instance.runtime_properties[
                                    "monitor_entrypoint"] +
                                instance.  # pylint: disable=W0212
                                _node_instance.runtime_properties[
                                    "monitor_port"])

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
        """ Send the instance to the HPC queue if it is a Job """
        if not self.parent_node.is_job:
            return

        self.winstance.send_event('Queuing HPC job..')
        result = self.winstance.execute_operation('hpc.interfaces.'
                                                  'lifecycle.queue',
                                                  kwargs={"name": self.name})
        self.winstance.send_event('..HPC job queued')
        # result.task.wait_for_terminated()

        self._status = 'PENDING'
        # print result.task.dump()
        return result.task

    def is_finished(self):
        """ True if the job is finished or it is not a job """
        if not self.parent_node.is_job:
            return True

        return self._status == 'FINISHED'

    def update_status(self, status):
        """ Update the instance state """
        if not status == self._status:
            self._status = status
            self.winstance.send_event('State changed to ' + self._status)

    def clean(self):
        """ clean aux files if it is a Job """
        if not self.parent_node.is_job:
            return

        self.winstance.send_event('Cleaning HPC job..')
        result = self.winstance.execute_operation('hpc.interfaces.'
                                                  'lifecycle.cleanup',
                                                  kwargs={"name": self.name})
        self.winstance.send_event('..HPC job cleaned')
        # result.task.wait_for_terminated()

        # print result.task.dump()
        return result.task

    def cancel(self):
        """ Cancel the instance of the HPC if it is a Job """
        if not self.parent_node.is_job:
            return

        # First perform clean operation
        self.clean()

        self.winstance.send_event('Cancelling HPC job..')
        result = self.winstance.execute_operation('hpc.interfaces.'
                                                  'lifecycle.cancel',
                                                  kwargs={"name": self.name})
        self.winstance.send_event('..HPC job canceled')
        result.task.wait_for_terminated()

        self._status = 'CANCELLED'


class JobGraphNode(object):
    """ Wrap to add job functionalities to nodes """

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
        """ Adds a parent node """
        self.parents.append(node)
        self.parent_depencencies_left += 1

    def add_child(self, node):
        """ Adds a child node """
        self.children.append(node)

    def queue_all_instances(self):
        """ Send all instances to the HPC queue if it represents a Job """
        if not self.is_job:
            return []

        tasks = []
        for job_instance in self.instances:
            tasks.append(job_instance.queue())

        self.status = 'QUEUED'
        return tasks

    def is_ready(self):
        """ True if it has no more dependencies to satisfy """
        return self.parent_depencencies_left == 0

    def _remove_children_dependency(self):
        """ Removes a dependency of the Node already satisfied """
        for child in self.children:
            child.parent_depencencies_left -= 1

    def check_finished(self):
        """
        Check if all instances has finished

        If all of them have finished, change node status as well
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
        """ Get all children nodes that are ready to start """
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

    def clean_all_instances(self):
        """ Clean all files instances of the HPC if it represents a Job """
        if not self.is_job:
            return

        for job_instance in self.instances:
            job_instance.clean()
        self.status = 'CANCELED'

    def cancel_all_instances(self):
        """ Cancel all instances of the HPC if it represents a Job """
        if not self.is_job:
            return

        for job_instance in self.instances:
            job_instance.cancel()
        self.status = 'CANCELED'


def build_graph(nodes):
    """ Creates a new graph of nodes and instances with the job wrapper """

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
    """ Monitor the instances talking with prometheus """

    def __init__(self, job_instances_map):
        self.job_ids = {}
        self._execution_pool = {}
        self.timestamp = 0
        self.job_instances_map = job_instances_map

    def check_status(self):
        """ Gets all executing instances and check their state """

        # first get the instances we need to check
        url_names_map = {}
        url_host_map = {}
        url_mtype_map = {}
        for _, job_node in self.get_executions_iterator():
            if job_node.is_job:
                for job_instance in job_node.instances:
                    if not job_instance.simulate:
                        if job_instance.monitor_url in url_names_map:
                            url_names_map[job_instance.monitor_url].append(
                                job_instance.name)
                        else:
                            url_names_map[job_instance.monitor_url] = [
                                job_instance.name]
                            url_host_map[job_instance.monitor_url] = \
                                job_instance.host
                            url_mtype_map[job_instance.monitor_url] = \
                                job_instance.monitor_type
                    else:
                        job_instance.update_status('FINISHED')

        # nothing to do if we don't have nothing to monitor
        if not url_names_map:
            return

        # We wait to not sature the monitor
        seconds_to_wait = 15 - (time.time() - self.timestamp)
        if seconds_to_wait > 0:
            sys.stdout.flush()  # necessary to output work properly with sleep
            time.sleep(seconds_to_wait)

        self.timestamp = time.time()

        # then look for the status of the instances through its name
        states = monitors.get_states(
            url_names_map, url_mtype_map, url_host_map)

        # finally set job status
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

    def get_executions_iterator(self):
        """ Executing nodes iterator """
        return self._execution_pool.iteritems()

    def add_node(self, node):
        """ Adds a node to the execution pool """
        self._execution_pool[node.name] = node

    def finish_node(self, node_name):
        """ Delete a node from the execution pool """
        del self._execution_pool[node_name]

    def is_something_executing(self):
        """ True if there are nodes executing """
        return self._execution_pool


@workflow
def run_jobs(**kwargs):  # pylint: disable=W0613
    """ Workflow to execute long running batch operations """

    root_nodes, job_instances_map = build_graph(ctx.nodes)
    monitor = Monitor(job_instances_map)

    # Execution of first job instances
    tasks = []
    for root in root_nodes:
        tasks += root.queue_all_instances()
        monitor.add_node(root)
    wait_tasks_to_finish(tasks)

    # Monitoring and next executions loop
    while monitor.is_something_executing() and not api.has_cancel_request():
        # Monitor the infrastructure
        monitor.check_status()
        exec_nodes_finished = []
        new_exec_nodes = []
        for node_name, exec_node in monitor.get_executions_iterator():
            # TODO(emepetres): support different states
            if exec_node.check_finished():
                exec_node.clean_all_instances()
                exec_nodes_finished.append(node_name)
                new_nodes_to_execute = exec_node.get_children_ready()
                for new_node in new_nodes_to_execute:
                    new_exec_nodes.append(new_node)
        # remove finished nodes
        for node_name in exec_nodes_finished:
            monitor.finish_node(node_name)
        # perform new executions
        tasks = []
        for new_node in new_exec_nodes:
            tasks += new_node.queue_all_instances()
            monitor.add_node(new_node)
        wait_tasks_to_finish(tasks)

    if monitor.is_something_executing():
        for node_name, exec_node in monitor.get_executions_iterator():
            exec_node.cancel_all_instances()
        raise api.ExecutionCancelled()

    ctx.logger.info(
        "------------------Workflow Finished-----------------------")
    return


def wait_tasks_to_finish(tasks):
    """Blocks until all tasks have finished"""
    for task in tasks:
        task.wait_for_terminated()
