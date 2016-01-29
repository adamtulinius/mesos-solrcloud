#!/usr/bin/env python3

import json
import random
import string

from pprint import pprint
from mesos.Agents import Agent
from mesos.Offers import Offer

import requests

masters = ["mesos-master-t01.dbc.dk:5050",
           "mesos-master-t02.dbc.dk:5050",
           "mesos-master-t03.dbc.dk:5050"]


def parse_resources(resources):
    result = {}
    for resource in resources:
        if resource['name'] in ('cpus', 'mem'):
            result[resource['name']] = resource['scalar']['value']

    return result


def has_required_resources(resources, **kwargs):
    parsed_resources = parse_resources(resources)
    result = []
    for resource_type, resource_requirement in kwargs.items():
        if resource_type not in parsed_resources or parsed_resources[resource_type] < resource_requirement:
            return False
        else:
            result.append({'name': resource_type, 'type': 'SCALAR', 'scalar': {'value': resource_requirement}})

    return result


class MesosFramework:
    def __init__(self, masters):
        self.framework_id = None
        self.framework_user = 'mesos-default'
        self.framework_name = 'mesos-solr'
        self.connection = None
        self.masters = masters
        self.leader = None
        self.heartbeats = 0
        self.shutdown = False
        self.known_agents = []
        self.running_tasks = []

    def __send_message__(self, data, host=None, stream=False):
        if host is None:
            host = self.leader

        r = requests.post('http://' + host + '/api/v1/scheduler',
                          stream=stream,
                          headers={'Content-Type': 'application/json',
                                   'Accept': 'application/json'},
                          data=data)
        print(r.status_code)

        return r

    def message(self, type, **kwargs):
        m = kwargs
        m['type'] = type
        if self.framework_id is not None:
            m['framework_id'] = {'value': self.framework_id}

        return m

    def json_message(self, type, **kwargs):
        res = json.dumps(self.message(type, **kwargs))
        print(res)
        return res

    def is_connected(self):
        return self.connection is not None and self.leader is not None

    def connect(self, force=False):
        payload = self.json_message(
            'SUBSCRIBE',
            subscribe={
                'framework_info': {
                    'name': self.framework_name,
                    'user': self.framework_user,
                    'checkpoint': True
                },
                'force': force
            }
        )

        for master in self.masters:
            r = self.__send_message__(payload, host=master, stream=True)

            if r.status_code == 200:
                print("Connected to '%s'" % master)
                self.connection = r
                self.leader = master
                return True
            else:
                print("Got: %s (%s)" % (r.content, r.status_code))
                return False

    def run(self):
        self.shutdown = False
        if not self.is_connected():
            self.connect()
            # self.reconcile()

        for data in self.connection.iter_content(chunk_size=None):
            if self.shutdown:
                self.connection.close()
                break

            length, response_str = data.decode(encoding='UTF-8').split('\n', 1)
            response = json.loads(response_str)

            print(response['type'].lower())

            handler_name = 'handle_' + response['type'].lower()
            try:
                fn = getattr(self, handler_name)
                fn(response)
            except AttributeError as e:
                print(e)
                print('Missing handler: ', handler_name)
                pprint(response)

    def teardown(self):
        if self.is_connected():
            self.__send_message__(self.json_message(type='TEARDOWN'))
        self.shutdown = True

    def reconcile(self):
        self.__send_message__(self.json_message(type='RECONCILE', reconcile={'tasks': []}))

    def accept_offer(self, offer, resources):
        task_id = 'solrcloud-' + ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(8))

        r = self.__send_message__(
            self.json_message(
                'ACCEPT',
                accept={
                    'offer_ids': [{'value': offer.id}]
                },
                operations=[
                    {'type': 'LAUNCH',
                     'launch': {
                         'task_infos': [
                             {
                                 'name': 'solrcloud',
                                 'task_id': {'value': task_id},
                                 'agent_id': {'value': offer.agent.id},
                                 'resources': resources,
                                 'command': {
                                     'value': 'echo foo && sleep 300',
                                     'user': 'mesos-default'
                                 }
                             }
                         ]
                     }}
                ]
            )
        )

        return 200 <= r.status_code < 300

    def decline_offers(self, offers):
        print("Declining offers: %s" % ','.join([offer.id for offer in offers]))
        r = self.__send_message__(
            self.json_message('DECLINE',
                              decline={
                                  'offer_ids': [{'value': offer.id} for offer in offers]
                              })
        )

        return 200 <= r.status_code < 300

    def handle_subscribed(self, response):
        self.framework_id = response['subscribed']['framework_id']['value']
        print("Updated framework id to '%s'" % self.framework_id)

    def handle_heartbeat(self, response):
        self.heartbeats += 1
        if self.heartbeats > 1:
            pass
            # self.teardown()

    def handle_offers(self, response):
        offers = []
        for offer in response['offers']['offers']:
            agent = Agent(id=offer['agent_id']['value'], hostname=offer['hostname'])
            o = Offer(id=offer['id']['value'], agent=agent, resources=[])
            offers.append(o)

            resources = has_required_resources(offer['resources'], cpus=1.0, mem=1024.0)

            if resources:
                print("Accepting offer: %s" % o)
                ok = self.accept_offer(Offer(id=offer['id']['value'], agent=agent, resources=[]), resources)
                if not ok:
                    print("Failed to accept offer %s" % o)
            else:
                print("Declining offer: %s" % o)
                ok = self.decline_offers([Offer(id=offer['id']['value'], agent=agent, resources=[])])
                if not ok:
                    print("Failed to decline offer %s" % o)


if __name__ == '__main__':
    mf = MesosFramework(masters)
    mf.message("REVIVE", foo='bar')
    mf.run()
