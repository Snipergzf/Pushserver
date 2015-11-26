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

# The Message Queue Handling Module

from __future__ import print_function
import time
import random
import gevent
from gevent.lock import RLock
from gevent.queue import Queue, Empty

import Logger
from config import *
from Message import MessageObj
from DataMgr import DataMgr


class QueueHandler(object):

    def __init__(self, logger, pending_online_users, make_func, send_func):
        self.alive = True
        self.last_idx = None
        self.logger = logger
        self.pending_online_users = pending_online_users
        self._pause_lock = RLock()
        self._make_func = make_func
        self._send_func = send_func#self._send_func
        #self.daemon = True
        #self.start()

    def sort(self):
        pass

    def put_bundle(self, msg):
        self.bundle_queue.bundle_queue_put(msg)

    def shutdown(self):
        self.alive = False
        #put None to notify running thread
        gevent.killall(self.greenlets)

    def run(self):
        self.greenlets = [
            gevent.spawn(self.main_loop),
            gevent.spawn(self.online_loop)
        ]

    def pause(self):
        self._pause_lock.acquire()

    def resume(self):
        self._pause_lock.release()

    @property
    def qsize(self):
        return self.bundle_queue.qsize()

    def main_loop(self):
        while True:
            self._pause_lock.acquire()
            # call DataMgr.make_bundle to make bundle of full m*n map
            # and pass function _send_func(GatewayMgr.send_push) as argument
            self._make_func(self._send_func)
            self._pause_lock.release()
            #TODO sleep longer
            #gevent.sleep(random.random())
            gevent.sleep(MSG_CHECK_INTERV)

    def online_loop(self):
        while True:
            u = self.pending_online_users.get()
            # call DataMgr.make_bundle to make bundle of full m*1 map for specific user
            self._make_func(self._send_func, user_keys = [u])
            # context switch
            gevent.sleep(0)


