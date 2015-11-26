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

# TestSuite

import traceback

from QueueHandler import QueueHandler
from Message import MessageObj
from DataMgr import DataMgr
from User import UserObj
from config import *
import Logger
import atexit

logger = Logger.Logging('logging')

def wait_for_sent_finnished(qh):
    import time
    while qh.qsize > 0:
        time.sleep(1)

def test_suite1():
    logger.info('***START OF TEST***')
    try:
        import random
        import time
        import string
        dm = DataMgr()
        atexit.register(dm.shutdown)
        dm.reset()
        qh = QueueHandler(dm.msg_queue)
        logger.debug('remaining msg queue size:%d' % qh.qsize)
        assert(qh.qsize == 0)
        for i in range(100):
            u = UserObj(i)
            dm.users_add(u)
        for i in range(8):
            m = MessageObj(''.join([random.choice(string.letters) for i in range(10)]))
            dm.msg_add(m)
        time.sleep(MSG_CHECK_INTERV)
        dm.make_bundle()

        wait_for_sent_finnished(qh)
        logger.debug('remaining msg queue size:%d' % qh.qsize)
        assert(qh.qsize == 0)

        qh.pause()
        logger.info('***TESTING callback and re-add feature***')
        dm.make_bundle()
        logger.debug('remaining msg queue size:%d' % qh.qsize)
        qh.resume()
        wait_for_sent_finnished(qh)
        assert(qh.qsize == 0)

        logger.info('***TESTING jump-in-queue feature***')
        _new = []
        for i in range(5):
            u = UserObj(i)
            _new.append(str(u.guid))
            dm.users_add(u)
        _new = sorted(_new)
        logger.debug('add %s' % ','.join(_new))
        dm.make_bundle()
        wait_for_sent_finnished(qh)

        qh.shutdown()
    except:
        logger.error('***TEST FAILED***\n%s' % traceback.format_exc())
    logger.info('***END OF TEST1***\n')




if __name__ == '__main__':
    #TODO: fix tags for test 1
    test_suite1()
    #test_suite2()
