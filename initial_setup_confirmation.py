import argparse
import ipaddress
import logging
import sys
import time
import traceback
from os import popen
import platform
#import pprint
import os

import xmltodict
from slack_log_handler import SlackLogHandler

sys.path.append(os.environ['HOME'] + os.sep + 'netapp-manageability-sdk' + os.sep + 'lib' + os.sep + 'python')
from NetApp import NaElement
from NetApp import NaServer

#sys.path.append("NetApp")
#import NaElement
#from NaServer import *

class EclNetApp(object):

    API_VERSION_MAJOR = 1
    API_VERSION_MINOR = 31
    TRANSPORT = 'HTTPS'
    MAX_RECORDS = (2 ** 32) - 1
    filer = ""
    user = ""
    password = ""
    naserver = None

    timeout = -1
    retry_iteration = 60
    retry_sleep = 10

    def __init__(self, filer="", user="", password="", loglevel=logging.NOTSET):
        self.log = None
        self.log_sh = None
        self.log_syh = None
        self.set_logger(loglevel)

        self.filer = filer
        self.user = user
        self.password = password
        self.init_naserver()

        self.cluster_name = ''
        self.nodes = []
        self.model = ''
        self.shelf_model = ''

        self.futai_step = 1


    def set_logger(self, loglevel):
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.DEBUG)

        # Steam Handler for STDERR logging
        self.log_sh = logging.StreamHandler(sys.stderr)
        self.log_sh.setLevel(loglevel)
        stderr_formatter = logging.Formatter(
            '%(asctime)s %(name)s:%(lineno)d %(levelname)s: %(message)s'
            #'%(asctime)s - %(threadName)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s'
        )
        self.log_sh.setFormatter(stderr_formatter)
        self.log.addHandler(self.log_sh)

        # File handler
        file_handler = logging.FileHandler('/var/tmp/setup-check.log')
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            #'%(asctime)s %(name)s:%(lineno)d %(levelname)s:\n' + '%(message)s' + '\n'
            '%(message)s\n'
        )
        file_handler.setFormatter(file_formatter)
        self.log.addHandler(file_handler)

        # Syslog Handler
        #addr = ''
        #s = platform.system()
        #if s == 'Linux':
        #    addr = '/dev/log'
        #elif s == 'Darwin':
        #    addr = '/var/run/syslog'
        #if addr:
        #    self.log_syh = logging.handlers.SysLogHandler(addr)
        #    self.log_syh.setLevel(logging.ERROR)
        #    syslog_formatter = logging.Formatter('%(name)s[%(process)d]: %(levelname)s - %(message)s')
        #    self.log_syh.setFormatter(syslog_formatter)
        #    self.log.addHandler(self.log_syh)

        # Slack log hander
        webhook_url = 'https://hooks.slack.com/services/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'
        self.slack_handler = SlackLogHandler(webhook_url=webhook_url)
        self.slack_handler.setLevel(logging.INFO)
        self.log.addHandler(self.slack_handler)
        slack_formatter = logging.Formatter(
                '[%(levelname)s] [%(name)s] - %(message)s'
        )
        self.slack_handler.setFormatter(slack_formatter)
        self.log.addHandler(self.slack_handler)

    def init_naserver(self, filer='', user='', password=''):
        if not filer:
            filer = self.filer
        if not user:
            user = self.user
        if not password:
            password = self.password

        self.naserver = NaServer(
            filer,
            self.API_VERSION_MAJOR,
            self.API_VERSION_MINOR
        )
        self.naserver.set_admin_user(self.user, self.password)
        self.naserver.set_transport_type(self.TRANSPORT)
        if self.timeout > 0:
            self.naserver.set_timeout(self.timeout)

    def _invoke(self, request, retry=True):
        for loop in range(self.retry_iteration):
            self.log.debug('-----------------')
            self.log.debug('Request to NetApp')
            self.log.debug('-----------------\n' + request.sprintf())

            response = self.naserver.invoke_elem(request)

            self.log.debug('--------------------')
            self.log.debug('Response from NetApp')
            self.log.debug('--------------------\n' + response.sprintf())

            if not retry:
                break
            elif response.results_status() == 'passed':
                break
            elif response.results_status() == 'failed' and int(response.results_errno()) == 13001:
                reason = str(response.results_reason())
                if 'Operation timed out' in reason:
                    self.log.info('Operation timed out. Will retry after %s secs' % (self.retry_sleep))
                    time.sleep(self.retry_sleep)
                    continue
                elif 'Connection refused' in reason:
                    self.log.info('Connection refused. Will retry after %s secs' % (self.retry_sleep))
                    time.sleep(self.retry_sleep)
                    continue
                elif 'Connection reset by peer' in reason:
                    self.log.info('Connection reset. Will retry after %s secs' % (self.retry_sleep))
                    time.sleep(self.retry_sleep)
                    continue
                elif 'Connection timed out' in reason:
                    self.log.info('Connection timed out. Will retry after %s secs' % (self.retry_sleep))
                    time.sleep(self.retry_sleep)
                    continue
                elif 'EOF occurred in violation of protocol' in reason:
                    self.log.info('ONTAPI not responding. Rebooting node? Will retry after %s sec' % (self.retry_sleep))
                    time.sleep(self.retry_sleep)
                    continue
                elif 'RPC: Port mapper failure' in reason:
                    self.log.info('RPC failed. Changed LIF or port settings? Will retry after %s sec' % (self.retry_sleep))
                    time.sleep(self.retry_sleep)
                    continue
                elif 'RPC: Unable to receive' in reason:
                    self.log.info('RPC failed. Changed LIF or port settings? Will retry after %s sec' % (self.retry_sleep))
                    time.sleep(self.retry_sleep)
                    continue
            break

        # Cleaning vfiler if set
        # This is required when a Cluster-level API is called after a Vserver-level API called
        if self.naserver.vfiler:
            self.naserver.vfiler = ""

        return response

    def get_node_names(self):
        """
        Get list of node names of cluster.
        :return:  list of node names (sorted).
        """
        if self.nodes:
            return self.nodes

        api = "system-node-get-iter"
        request = NaElement(api)

        desired_attributes = NaElement("desired-attributes")
        node_details_info = NaElement("node-details-info")
        node = NaElement("node")
        node_details_info.child_add(node)
        desired_attributes.child_add(node_details_info)
        request.child_add(desired_attributes)

        response = self._invoke(request)

        if response.results_status() != 'passed':
            raise Exception('status != passed. Errno=%s, Reason=%s' % (response.results_errno(), response.results_reason()))

        xmldict = xmltodict.parse(response.sprintf())

        # num records should be 2
        num_records = int(xmldict['results']['num-records'])
        if num_records != 2:
            Exception("num_records != 2. Something wrong")

        nodes = []
        for node in xmldict['results']['attributes-list']['node-details-info']:
            nodes.append(node['node'])

        nodes.sort()
        self.nodes = nodes
        return nodes

    def system_cli(self, command, priv=''):
        """
        Excecute ONTAP CLI command via system-cli API
        :param command:  command string
        :param priv:  privilege to execute command. priv can be 'adv' or 'diag'
        :return: True if command is successfully executed else False.
        """
        api = 'system-cli'
        request = NaElement(api)

        a = NaElement('args')

        if priv and priv.startswith('diag'):
            a.child_add_string('arg', 'set -privilege diagnostic -confirmations off;')
        elif priv and priv.startswith('adv'):
            a.child_add_string('arg', 'set -privilege advanced -confirmations off;')

        for arg in command.split():
            a.child_add_string('arg', arg)

        request.child_add(a)

        response = self._invoke(request)

        if response.results_status() != 'passed':
            raise Exception('status != passed. Errno=%s, Reason=%s' % (response.results_errno(), response.results_reason()))

        xmldict = xmltodict.parse(response.sprintf())

        # original slack formatter
        self.log.info('===== system_cli(command: %s) =====' % command)
        orig_formatter = self.slack_handler.formatter
        self.slack_handler.setFormatter(
        logging.Formatter(
                '[%(levelname)s] [%(name)s] - ```%(message)s```'
        )
        )

        self.log.info(xmldict['results']['cli-output'])
        self.slack_handler.setFormatter(orig_formatter)

        if xmldict['results']['cli-result-value'] == '1':
            return True
        return False

    def initial_setup_check_commands(self):

        nodes = self.get_node_names()

        command_list = [
            'cluster identity show',
            'system node show',
            'system node image show',
            'system node run -node %s -command sasadmin shelf' % nodes[0],
            'system node run -node %s -command sasadmin shelf' % nodes[1],
            'system node run -node %s -command sasadmin adapter_state' % nodes[0],
            'system node run -node %s -command sasadmin adapter_state' % nodes[1],
            'storage shelf acp show',
            'disk show -broken',
            'system chassis fru show',
            'network interface show -lif cluster_mgmt -home-node %s -home-port a0c' % nodes[0],
            'route show',
            'timezone',
            'dns show',
            'network interface show -role node-mgmt -home-node %s -home-port e0M' % nodes[0],
            'network interface show -role node-mgmt -home-node %s -home-port e0M' % nodes[1],
            'system service-processor network show -fields ip-address ,netmask ,gateway ,status -address-family IPv4',
            'ifgrp show -fields node, ifgrp, ports, mode, mac, distr-func, activeports',
            'broadcast-domain show',
            'network port show',
            'storage aggregate show -root true -node %s -fields size,state,raidstatus' % nodes[0],
            'storage aggregate show -root true -node %s -fields size,state,raidstatus' % nodes[1],
            'system license show'
        ]
        for command in command_list:
            #time.sleep(1)
            self.system_cli(command)
        return True
