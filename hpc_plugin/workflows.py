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
from cloudify.workflows import ctx, api, tasks
from hpc_plugin import monitors

MONITOR_PERIOD = 15


class JobGraphInstance(object):
    """ Wrap to add job functionalities to node instances """

    def __init__(self, parent, instance):
        self._status = 'WAITING'
        self.parent_node = parent
        self.winstance = instance

        self.completed = not self.parent_node.is_job  # True if is not a job
        self.failed = False

        if parent.is_job:
            self._status = 'WAITING'

            # Get runtime properties
            self.simulate = (instance._node_instance.  # pylint: disable=W0212
                             runtime_properties["simulate"])
            credentials = (instance.  # pylint: disable=W0212
                           _node_instance.runtime_properties[
                               "credentials"])
            self.host = credentials["host"]
            workload_manager = (instance.  # pylint: disable=W0212
                                _node_instance.runtime_properties[
                                    "workload_manager"])
            prefix = (instance._node_instance.  # pylint: disable=W0212
                      runtime_properties["job_prefix"])
            external_monitor_type = (instance.  # pylint: disable=W0212
                                     _node_instance.runtime_properties[
                                         "external_monitor_type"])
            external_monitor_entrypoint = (instance.  # pylint: disable=W0212
                                           _node_instance.runtime_properties[
                                               "external_monitor_entrypoint"])
            external_monitor_port = (instance.  # pylint: disable=W0212
                                     _node_instance.runtime_properties[
                                         "external_monitor_port"])

            # Decide how to monitor the job
            if external_monitor_entrypoint:
                self.monitor_type = external_monitor_type
                self.monitor_config = {
                    'url': ('http://' +
                            external_monitor_entrypoint +
                            external_monitor_port)
                }
            else:  # internal monitoring
                self.monitor_type = workload_manager
                self.monitor_config = credentials

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
        result.task.wait_for_terminated()
        if result.task.get_state() == tasks.TASK_FAILED:
            init_state = 'FAILED'
        else:
            self.winstance.send_event('..HPC job queued')
            init_state = 'PENDING'
        self.set_status(init_state)
        # print result.task.dump()
        return result.task

    def publish(self):
        """ Send the instance to the HPC queue if it is a Job """
        if not self.parent_node.is_job:
            return

        self.winstance.send_event('Publishing HPC job..')
        result = self.winstance.execute_operation('hpc.interfaces.'
                                                  'lifecycle.publish',
                                                  kwargs={"name": self.name})
        # TODO: How to do it in non-blocking??
        result.task.wait_for_terminated()
        if result.task.get_state() != tasks.TASK_FAILED:
            self.winstance.send_event('..HPC job published')

        return result.task

    def set_status(self, status):
        """ Update the instance state """
        if not status == self._status:
            self._status = status
            self.winstance.send_event('State changed to ' + self._status)

            self.completed = not self.parent_node.is_job or \
                self._status == 'COMPLETED'

            if not self.parent_node.is_job:
                self.failed = False
            else:
                self.failed = self.parent_node.is_job and \
                    (self._status == 'BOOT_FAIL' or
                     self._status == 'CANCELLED' or
                     self._status == 'FAILED' or
                     self._status == 'REVOKED' or
                     self._status == 'TIMEOUT')

    def clean(self):
        """ clean aux files if it is a Job """
        if not self.parent_node.is_job:
            return

        self.winstance.send_event('Cleaning HPC job..')
        result = self.winstance.execute_operation('hpc.interfaces.'
                                                  'lifecycle.cleanup',
                                                  kwargs={"name": self.name})
        # result.task.wait_for_terminated()
        self.winstance.send_event('..HPC job cleaned')

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

        self.completed = False
        self.failed = False

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

        tasks_list = []
        for job_instance in self.instances:
            tasks_list.append(job_instance.queue())

        self.status = 'QUEUED'
        return tasks_list

    def publish(self):
        """ Send all instances to the HPC queue if it represents a Job """
        if not self.is_job:
            return []

        tasks_list = []
        for job_instance in self.instances:
            tasks_list.append(job_instance.publish())

        return tasks_list

    def is_ready(self):
        """ True if it has no more dependencies to satisfy """
        return self.parent_depencencies_left == 0

    def _remove_children_dependency(self):
        """ Removes a dependency of the Node already satisfied """
        for child in self.children:
            child.parent_depencencies_left -= 1

    def check_status(self):
        """
        Check if all instances status

        If all of them have finished, change node status as well
        Returns True if there is no errors (no job has failed)
        """
        if not self.completed and not self.failed:
            if not self.is_job:
                self._remove_children_dependency()
                self.status = 'COMPLETED'
                self.completed = True
            else:
                completed = True
                failed = False
                for job_instance in self.instances:
                    if job_instance.failed:
                        failed = True
                        break
                    elif not job_instance.completed:
                        completed = False

                if failed:
                    self.status = 'FAILED'
                    self.failed = True
                    self.completed = False
                    return False

                if completed:
                    # The job node just finished, remove this dependency
                    self.status = 'COMPLETED'
                    self._remove_children_dependency()
                    self.completed = True

        return not self.failed

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
    """Monitor the instances"""

    def __init__(self, job_instances_map, logger):
        self.job_ids = {}
        self._execution_pool = {}
        self.timestamp = 0
        self.job_instances_map = job_instances_map
        self.logger = logger

    def update_status(self):
        """Gets all executing instances and update their state"""

        # first get the instances we need to check
        monitor_jobs = {}
        for _, job_node in self.get_executions_iterator():
            if job_node.is_job:
                for job_instance in job_node.instances:
                    if not job_instance.simulate:
                        if job_instance.host in monitor_jobs:
                            monitor_jobs[job_instance.host]['names'].append(
                                job_instance.name)
                        else:
                            monitor_jobs[job_instance.host] = {
                                'config': job_instance.monitor_config,
                                'type': job_instance.monitor_type,
                                'names': [job_instance.name]
                            }
                    else:
                        job_instance.set_status('COMPLETED')

        # nothing to do if we don't have nothing to monitor
        if not monitor_jobs:
            return

        # We wait to not sature the monitor
        seconds_to_wait = MONITOR_PERIOD - (time.time() - self.timestamp)
        if seconds_to_wait > 0:
            sys.stdout.flush()  # necessary to output work properly with sleep
            time.sleep(seconds_to_wait)
        self.logger.debug("Reading job status..")

        self.timestamp = time.time()

        # then look for the status of the instances through its name
        states = monitors.get_states(monitor_jobs, self.logger)

        # finally set job status
        for inst_name, state in states.iteritems():
            self.job_instances_map[inst_name].set_status(state)

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
    monitor = Monitor(job_instances_map, ctx.logger)

    # Execution of first job instances
    tasks_list = []
    for root in root_nodes:
        tasks_list += root.queue_all_instances()
        monitor.add_node(root)
    wait_tasks_to_finish(tasks_list)

    # Monitoring and next executions loop
    while monitor.is_something_executing() and not api.has_cancel_request():
        # Monitor the infrastructure
        monitor.update_status()
        exec_nodes_finished = []
        new_exec_nodes = []
        for node_name, exec_node in monitor.get_executions_iterator():
            if exec_node.check_status():
                if exec_node.completed:
                    exec_node.clean_all_instances()
                    exec_nodes_finished.append(node_name)
                    new_nodes_to_execute = exec_node.get_children_ready()
                    for new_node in new_nodes_to_execute:
                        new_exec_nodes.append(new_node)
            else:
                # Something went wrong in the node, cancel execution
                cancel_all(monitor.get_executions_iterator())
                return

        # remove finished nodes
        for node_name in exec_nodes_finished:
            monitor.finish_node(node_name)
        # perform new executions
        tasks_list = []
        for new_node in new_exec_nodes:
            tasks_list += new_node.queue_all_instances()
            monitor.add_node(new_node)
        wait_tasks_to_finish(tasks_list)

    if monitor.is_something_executing():
        cancel_all(monitor.get_executions_iterator())

    ctx.logger.info(
        "------------------Workflow Finished-----------------------")
    return


def cancel_all(executions):
    """Cancel all pending or running jobs"""
    for _, exec_node in executions:
        exec_node.cancel_all_instances()
    raise api.ExecutionCancelled()


def wait_tasks_to_finish(tasks_list):
    """Blocks until all tasks have finished"""
    for task in tasks_list:
        task.wait_for_terminated()
