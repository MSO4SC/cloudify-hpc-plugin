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

""" Holds the functions that requests jobs information """

import time
import requests
from threading import Lock
from workload_managers.workload_manager import (WorkloadManager,
                                                state_int_to_str)


class JobRequester(object):
    """ Safely gets the jobs status when requested """
    class __JobRequester(object):
        _last_time = {}
        _lock = Lock()

        def request(self, monitor_jobs, logger):
            """ Retrieves the status of every job"""
            states = {}

            for host, settings in monitor_jobs.iteritems():
                # Only get info when it is safe
                if host in self._last_time:
                    seconds_to_wait = settings['period'] - \
                        (time.time() - self._last_time[host])
                    if seconds_to_wait > 0:
                        continue

                logger.debug("Reading job status..")
                self._last_time[host] = time.time()

                if settings['type'] == "PROMETHEUS":  # external
                    partial_states = self._get_prometheus(
                        host,
                        settings['config'],
                        settings['names'])
                else:  # internal
                    wm = WorkloadManager.factory(settings['type'])
                    if wm:
                        partial_states = wm.get_states(
                            settings['workdir'],
                            settings['config'],
                            settings['names'],
                            logger
                        )
                    else:
                        partial_states = self._no_states(
                            host,
                            settings['type'],
                            settings['names'],
                            logger)
                states.update(partial_states)

            return states

        def _get_prometheus(self, host, config, names):
            states = {}
            url = config['url']
            if len(names) == 1:
                query = (url + '/api/v1/query?query=job_status'
                         '%7Bjob%3D%22' + host +
                         '%22%2Cname%3D%22')
            else:
                query = (url + '/api/v1/query?query=job_status'
                         '%7Bjob%3D%22' + host +
                         '%22%2Cname%3D~%22')
            query += '|'.join(names) + '%22%7D'

            payload = requests.get(query)

            response = payload.json()

            for item in response["data"]["result"]:
                states[item["metric"]["name"]] = state_int_to_str(
                    item["value"][1])

            return states

        def _no_states(self, host, mtype, names, logger):
            logger.error("Monitor of type " +
                         mtype +
                         " not suppported. " +
                         "Jobs [" +
                         ','.join(names) +
                         "] on host '" + host
                         + "' will be considered FAILED.")
            states = {}
            for name in names:  # TODO cancel those jobs
                states[name] = 'FAILED'
            return states

    instance = None

    def __init__(self):
        if not JobRequester.instance:
            JobRequester.instance = JobRequester.__JobRequester()

    def __getattr__(self, name):
        return getattr(self.instance, name)
