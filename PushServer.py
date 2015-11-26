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
 
import atexit
import signal
import gevent
from gevent.queue import Queue, Empty
from gevent import monkey
#monkey.patch_all()

import Logger
from config import *
from User import UserObj
from DataMgr import DataMgr
from Message import MessageObj
from GatewayMgr import GatewayMgr
from QueueHandler import QueueHandler
import alg

logger = Logger.Logging('logging')

import os

class PushServer(object):
    def __init__(self, logger):
        self.has_shutdown = False
        self.logger = logger
        self.greenlets = []
        self.dm = DataMgr(self.logger)
        self.gm = GatewayMgr(self.logger, self.dm.send_queue, self.dm.set_user_online, self.dm.set_user_offline)
        self.qh = QueueHandler(self.logger, self.dm.pending_online_users, self.dm.make_bundle, self.gm.send_push)
        # hook on exit
        atexit.register(self.shutdown)
        logger.info('Server started. (PID=%d)' % os.getpid())

    def demo(self):
        import random
        import string
        from User import UserObj
        from Message import MessageObj
        #u = UserObj('u-a', '12')
        #self.dm.users_add(u)


    def run(self):
        self.dm.reset()
        self.qh.run()
        self.greenlets = [
            # gevent.spawn(self.main_loop),
            # gevent.spawn(self.online_loop),
        ]

    def shutdown(self):
        if not self.has_shutdown:
            # shutdown all greenlets and clean up
            gevent.killall(self.greenlets)
            self.qh.shutdown()
            try:
                self.gm.shutdown()
            except KeyboardInterrupt:
                pass
            self.dm.shutdown()
            self.has_shutdown = True
            logger.info('Server exit nicely.')

    def reload_alg(self, n = 0, e = 0):
        try:
            reload(alg)
        except Exception as ex:
            logger.error('Failed to import algorithm module. Will use old module. \n%s' % traceback.format_exc())
        else:
            logger.info('reloading new algorithm module(version=%s)' % alg.__version__)
            

def main():
    try:
        logger.info('Initializing...')
        ps = PushServer(logger)
        ps.run()
        if hasattr(signal, 'SIGHUP'):
            # register on SIGHUP to reload alg module
            signal.signal(signal.SIGHUP, ps.reload_alg)
        else:
            logger.warning('will never reload alg module on win32 platform')
        ps.demo()
        while True:
            # ready for context switching
            gevent.sleep(0)
    except KeyboardInterrupt:
        return


if __name__ == '__main__':
    main()
