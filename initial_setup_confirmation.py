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

#import pymysql
#from slack_log_handler import SlackLogHandler

#sys.path.append(os.environ['HOME'] + os.sep + 'netapp-manageability-sdk' + os.sep + 'lib' + os.sep + 'python')
#from NetApp import NaElement

sys.path.append("NetApp")
import NaElement
from NaServer import *


class EclNetApp(object):



