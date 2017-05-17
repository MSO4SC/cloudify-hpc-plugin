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

# from time import sleep
from cloudify.decorators import workflow
from cloudify.workflows import ctx

# from hpc_plugin import slurm


class JobGraphInstance(object):
    """ blah """
    def __init__(self, parent, instance):
        self.name = instance.id
        self.status = 'WAITING'
        self.parent_node = parent
        self.cfy_instance = instance

        if parent.type is 'hpc.nodes.job':
            self.status = 'WAITING'
        else:
            self.status = 'NONE'

    def queue(self):
        """ blah """
        if not self.parent_node.type == 'hpc.nodes.job':
            return

        self.cfy_instance.send_event('Queuing HPC job..')
        result = self.cfy_instance.execute_operation('hpc.interfaces.'
                                                     'lifecycle.queue')
        self.cfy_instance.send_event('..HPC job queued')
        result.task.wait_for_terminated()
        self.status = 'RUNNING'
        # print result.task.dump()

    def is_finished(self):
        """ blah """
        if self.parent_node.type is not 'hpc.nodes.job':
            return True

        return self.status is 'FINISHED'


class JobGraphNode(object):
    """ blah """
    def __init__(self, node):
        self.name = node.id
        self.type = node.type
        self.cfy_node = node

        if node.type is 'hpc.nodes.job':
            self.status = 'WAITING'
        else:
            self.status = 'NONE'

        self.instances = {}
        for instance in node.instances:
            self.instances[self.name] = JobGraphInstance(self, instance)

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

    def get_next_jobs_nodes(self):
        """ blah """
        if self.cfy_node is 'hpc.nodes.job':
            return [self]
        else:
            job_nodes = []
            for child in self.children:
                job_nodes += child.get_next_jobs_nodes()
            return job_nodes

    def queue_all_instances(self):
        """ blah """
        if not self.type == 'hpc.nodes.job':
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

    def is_finished(self):
        """ blah """
        if self.status is 'NONE':
            self._remove_children_dependency()
            return True

        if self.status is 'FINISHED':
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

    def __str__(self):
        to_print = self.name + '\n'
        for child in self.children:
            to_print += '\t' + child.name + '\n'
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
        for child_name, child in self.nodes.iteritems():
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
    def __init__(self):
        self.job_ids = {}

    def check_status(self, execution_pool):
        """ blah """
        # FIXME(emepetres) MOCK implemented
        for _, job_node in execution_pool.iteritems():
            if job_node.cfy_node.type is 'hpc.nodes.job':
                for _, job_instance in job_node.instances.itertitems():
                    job_instance.status = 'FINISHED'
                job_node.status = 'FINISHED'

    def getJobId(self, job_name):
        """ blah """
        pass


@workflow
def run_jobs(**kwargs):  # pylint: disable=W0613
    """ Workflow to execute long running batch operations """

    graph = JobGraph(ctx.nodes)
    print graph
    print '------------------------------'

    # Execution of first job instances
    execution_pool = {}
    for root in graph.root_nodes:
        root.queue_all_instances()
        execution_pool[root.name] = root

    # Monitoring and next executions loop
    monitor = Monitor()
    while execution_pool:
        # Monitor the infrastructure
        monitor.check_status(execution_pool)
        exec_nodes_finished = []
        new_exec_nodes = []
        for node_id, exec_node in execution_pool.iteritems():
            # TODO(emepetres): support different states
            if exec_node.is_finished():
                exec_nodes_finished.append(node_id)
                new_nodes_to_execute = exec_node.get_children_ready()
                for new_node in new_nodes_to_execute:
                    new_exec_nodes.append(new_node)
        # remove finished nodes
        for node_id in exec_nodes_finished:
            del execution_pool[node_id]
        # perform new executions
        for new_node in new_exec_nodes:
            new_node.queue_all_instances()
            execution_pool[new_node.name] = new_node

    return
