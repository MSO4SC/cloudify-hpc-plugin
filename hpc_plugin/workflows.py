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

import time
from cloudify.decorators import workflow
from cloudify.workflows import ctx

from hpc_plugin.ssh import SshClient
from hpc_plugin import slurm


class JobGraphInstance(object):
    """ blah """
    def __init__(self, parent, instance):
        self.name = instance.id
        self._status = 'WAITING'
        self.parent_node = parent
        self.cfy_instance = instance
        self.job_id = -1

        if parent.is_job:
            self._status = 'WAITING'
        else:
            self._status = 'NONE'

    def queue(self):
        """ blah """
        if not self.parent_node.is_job:
            return

        self.cfy_instance.send_event('Queuing HPC job..')
        result = self.cfy_instance.execute_operation('hpc.interfaces.'
                                                     'lifecycle.queue')
        self.cfy_instance.send_event('..HPC job queued')
        result.task.wait_for_terminated()
        self._status = 'PENDING'
        # print result.task.dump()

    def is_finished(self):
        """ blah """
        if not self.parent_node.type == 'hpc.nodes.job':
            return True

        return self._status == 'FINISHED'

    def has_job_id(self):
        """ blah """
        return self.job_id is not -1

    def update_status(self, status):
        """ blah """
        if not status == self._status:
            self._status = status
            self.cfy_instance.send_event('State changed to '+self._status)


class JobGraphNode(object):
    """ blah """
    def __init__(self, node):
        self.name = node.id
        self.type = node.type
        self.cfy_node = node

        if 'hpc.nodes.job' in node.type_hierarchy:
            self.is_job = True
        else:
            self.is_job = False

        if self.is_job:
            self.status = 'WAITING'
        else:
            self.status = 'NONE'

        self.instances = {}
        for instance in node.instances:
            self.instances[instance.id] = JobGraphInstance(self, instance)

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

        for _, job_instance in self.instances.iteritems():
            job_instance.queue()
        self.status = 'RUNNING'

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
        if self.status == 'NONE':
            self._remove_children_dependency()
            return True

        if self.status == 'FINISHED':
            return True

        for _, job_instance in self.instances.iteritems():
            if not job_instance.is_finished():
                return False

        # The job node just finished, remove this dependency
        self._remove_children_dependency()

        return True

    def get_children_ready(self):
        """ blah """
        readys = []
        for child in self.children:
            if child.is_ready():
                readys.append(child)
        return readys

    def get_instances_iter(self):
        """ blah """
        return self.instances.iteritems()

    def __str__(self):
        to_print = self.name + '\n'
        for name, _ in self.instances.iteritems():
            to_print += '- ' + name + '\n'
        for child in self.children:
            to_print += '    ' + child.name + '\n'
        return to_print


class JobGraph(object):
    """ blah """
    def __init__(self, nodes):
        # first create node structure
        self.nodes = {}
        self.root_nodes = []
        for node in nodes:
            new_node = JobGraphNode(node)
            self.nodes[node.id] = new_node
            # check if it is root node
            try:
                node.relationships.next()
            except StopIteration:
                self.root_nodes.append(new_node)

        # then set relationships
        for _, child in self.nodes.iteritems():
            for relationship in child.cfy_node.relationships:
                parent = self.nodes[relationship.target_node.id]
                parent.add_child(child)
                child.add_parent(parent)

    def __str__(self):
        to_print = ''
        for _, node in self.nodes.iteritems():
            to_print += node.__str__() + '\n'
        return to_print


class Monitor(object):
    """ blah """
    def __init__(self, simulated, config):
        self.job_ids = {}
        self.execution_pool = {}
        self.timestamp = 0
        self.simulated = simulated
        if not simulated:
            self.host = config['host']
            self.user = config['user']
            self.passwd = config['passwd']
        else:
            self.host = None
            self.user = None
            self.passwd = None

    def check_status(self):
        """ blah """
        # FIXME(emepetres) Direct Slurm monitoring implemented

        # We wait to not sature slurm
        seconds_to_wait = 30 - (time.time() - self.timestamp)
        if seconds_to_wait > 0:
            time.sleep(seconds_to_wait)

        if not self.simulated:
            # first check what operations need to do
            jobs_to_check_id = []
            instances_nodes_map = {}
            jobs_to_check_status = []
            ids_instances_map = {}
            for node_name, node in self.execution_pool.iteritems():
                if node.is_job:
                    for inst_name, inst in node.instances.iteritems():
                        instances_nodes_map[inst_name] = node_name
                        if not inst.has_job_id():
                            jobs_to_check_id.append(inst_name)
                        else:
                            jobs_to_check_status.append(inst.job_id)
                            ids_instances_map[inst.job_id] = inst_name

            client = SshClient(self.host,
                               self.user,
                               self.passwd)

            # first try to get job ids
            if jobs_to_check_id:
                ids = slurm.get_jobids_by_name(client, jobs_to_check_id)
                for job_name in jobs_to_check_id:
                    if job_name in ids:
                        job_node_name = instances_nodes_map[job_name]
                        self.execution_pool[job_node_name].\
                            instances[job_name].job_id = ids[job_name]
                        jobs_to_check_status.append(ids[job_name])
                        ids_instances_map[ids[job_name]] = job_name

            # then look for the status of the jobs that we have id for
            if jobs_to_check_status:
                states = slurm.get_status(client, jobs_to_check_status)
                for job_id in jobs_to_check_status:
                    if job_id in states:
                        # FIXME(emepetres): contemplate failed states
                        if states[job_id] == 'BOOT_FAIL' or \
                                states[job_id] == 'CANCELLED' or \
                                states[job_id] == 'COMPLETED' or \
                                states[job_id] == 'FAILED' or \
                                states[job_id] == 'PREEMPTED' or \
                                states[job_id] == 'REVOKED' or \
                                states[job_id] == 'TIMEOUT':
                            states[job_id] = 'FINISHED'

                        job_instance_name = ids_instances_map[job_id]
                        job_node_name = instances_nodes_map[job_instance_name]
                        job_node = self.execution_pool[job_node_name]
                        job_instance = job_node.instances[job_instance_name]
                        job_instance.update_status(states[job_id])

            client.close_connection()
            self.timestamp = time.time()
        else:
            for _, job_node in self.execution_pool.iteritems():
                if job_node.is_job:
                    for _, job_instance in job_node.instances.iteritems():
                        job_instance.update_status('FINISHED')

    def get_executions_iterator(self):
        """ blah """
        return self.execution_pool.iteritems()

    def add_node(self, node):
        """ blah """
        self.execution_pool[node.name] = node

    def finish_node(self, node_name):
        """ blah """
        del self.execution_pool[node_name]

    def is_something_executing(self):
        """ blah """
        return self.execution_pool


@workflow
def run_jobs(simulate, monitor_config, **kwargs):  # pylint: disable=W0613
    """ Workflow to execute long running batch operations """

    graph = JobGraph(ctx.nodes)
    monitor = Monitor(simulate, monitor_config)

    # Execution of first job instances
    for root in graph.root_nodes:
        root.queue_all_instances()
        monitor.add_node(root)

    # Monitoring and next executions loop
    while monitor.is_something_executing():
        # Monitor the infrastructure
        monitor.check_status()
        exec_nodes_finished = []
        new_exec_nodes = []
        for node_id, exec_node in monitor.get_executions_iterator():
            # TODO(emepetres): support different states
            if exec_node.check_finished():
                exec_nodes_finished.append(node_id)
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
