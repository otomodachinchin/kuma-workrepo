import argparse
import ipaddress
import logging
import sys
import time
import traceback
from os import popen
import platform
import pprint
import os

import xmltodict
from slack_log_handler import SlackLogHandler

#sys.path.append(os.environ['HOME'] + os.sep + 'netapp-manageability-sdk' + os.sep + 'lib' + os.sep + 'python')
#from NetApp import NaElement

sys.path.append("NetApp")
import NaElement
from NaServer import *


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

    region_sites = {
        'lab1ec': ['lb1'], # for testing in Lab1
        'lab3ec': ['lb3'],
        'lab4ec': ['lb4'],
        'jp1': ['kw1'],
        'uk1': ['lo8', 'hh3'],
        'sg1': ['sg2'],
        'us1': ['va1'],
        'jp2': ['os5', 'os1'],
        'au1': ['sy1'],
        'hk1': ['fd2'],
        'de1': ['ff1', 'ff6'],
        #'us2': ['ca1'],
        'jp3': ['ku1'],
        'jp5': ['tk2'],
        'jp4': ['mt1'],
        'fr1': ['pr1'],
        'jp6': ['kj1'],
    }

    valid_node_models = ['AFF8060', 'AFF8080', 'AFF-A300', 'FAS8060', 'FAS8080'] # Added FAS8060/8080 since ONTAP8 says so even if they are actually AFF8060/8080


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


    def get_cluster_name(self):
        """
        Get cluster name
        :return: cluster name string
        """
        if self.cluster_name:
            return self.cluster_name

        api = 'cluster-identity-get'
        request = NaElement(api)

        response = self._invoke(request)

        if response.results_status() != 'passed':
            raise Exception('status != passed. Errno=%s, Reason=%s' % (response.results_errno(), response.results_reason()))

        xmldict = xmltodict.parse(response.sprintf())

        orig_formatter = self.slack_handler.formatter
        self.slack_handler.setFormatter(
        logging.Formatter(
                '[%(levelname)s] [%(name)s] - ```%(message)s```'
        )
        )

        self.log.info(xmldict['results']['attributes']['cluster-identity-info']['cluster-name'])
        self.slack_handler.setFormatter(orig_formatter)


        cluster_name = xmldict['results']['attributes']['cluster-identity-info']['cluster-name']
        self.cluster_name = cluster_name
        return cluster_name



