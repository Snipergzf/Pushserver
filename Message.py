#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2015 fffonion
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either e._pr()ess or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# The Message Defination

import os
import binascii
import time
import random
try:
    import ujson as json
except ImportError:
    import json
from config import *

class MessageObj(object):
    '''
        The Message to be sent
    '''
    STATUS_SUCCESS = 0
    STATUS_ERROR = 1

    def __init__(self, payload_callback = None, tags = [], msgid = None):
        self.msgid = msgid
        self._payload = payload_callback
        self.birth = time.time()

    # @property
    # def msgid_h(self):
    #     '''Get human readable Msg ID'''
    #     return binascii.hexlify(self.msgid)

    @property
    def payload(self):
        return json.dumps(self._payload())

class BundledMessage(object):
    '''
        The Bundle that wrapps message and other runtime-attrs to specific user
    '''
    def __init__(self, msg_ref, usr_ref, callback):
        # be sure not to change it
        self._msg = msg_ref
        #target user
        self._user = usr_ref
        #retry count, 0 means not tried before
        self.try_count = 0
        self.callback = lambda result:callback(msg_ref, result)
        #TODO bundle uuid = msgid + user+id

    @property
    def msg(self):
        return self._msg

    @property
    def user(self):
        return self._user

    def get_try_count(self):
        return self.try_count

if __name__ == '__main__':
    class d():pass
    usr = d()
    msg = d()
    print(usr)
    print(msg)
    _ = BundledMessage(msg, usr)
    print(_.user)
    print(_.msg)

