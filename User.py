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
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# The user module

import uuid
import time
from Message import BundledMessage
import alg

class UserObj(object):

    def __init__(self, uid, gw_id, _match_func = None):
        self.uid = uid
        self.guid = gw_id
        self.heard_before = 0#the newest message user has been send
        self.leaked_msg = {}#msgid of failed message, prior to heard_before. {msgid:try_count,...}
        self.match_func = _match_func or self._match_func
        self.birth_time = time.time()

    def gen_bundle(self, msg):
        '''
            Return BundledMessage if msg should be sent
            else return None
        '''
        if msg.msgid in self.leaked_msg:#previously leaked
            print('User: previously leaked')
            b = BundledMessage(msg, self, self._send_callback)
            _ = self.leaked_msg[msg.msgid] + 1
            b.try_count = _
            return b
        if msg.birth > self.heard_before and self.match_func(msg):# not sent before, and, match interest
            print('User: not sent before, and, match interest')
            b = BundledMessage(msg, self, self._send_callback)
            return b
        return None

    def _match_func(self, msg):
        return alg.match([], [])
        #for test
        #return abs(int(str(self.guid)[-1], 16) - ord(msg.msgid[-1]) ^ 0xF) <= 2 #4/16 = 0.25

    def _send_callback(self, msg, send_result):
        if send_result > 0:#SEND_FAILED
            if msg.msgid not in self.leaked_msg:
                self.leaked_msg[msg.msgid] = 0
            else:
                self.leaked_msg[msg.msgid] += 1
        if msg.birth > self.heard_before:#raise last sent time to newest
            # print('msg.id '+str(msg.msgid))
            # print('msg.birth'+str(msg.birth))
            # print('raise last sent time to newest'+str(int(time.time())))
            self.heard_before = msg.birth

    @property
    def pr(self):
        # for testing, newer the bigger pr
        return self.birth_time

    # def set_tags(self, era, score=1):
    #     self._for_test_era.append(era)

