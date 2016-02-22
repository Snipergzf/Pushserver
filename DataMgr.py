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

import os
import os.path as opath
import gevent
from gevent import Greenlet
from gevent import monkey
from gevent.lock import RLock
from gevent.queue import Queue, Empty
try:
    import ujson as json
except ImportError:
    import json
import pickle
import atexit
import time
import Logger
from Message import MessageObj
from User import UserObj
from db_helper import mongo

from config import *

class InconsistentError(Exception):
    pass

class DataMgr(Greenlet):
    pickle_names = ['_msgs', '_users', 'send_queue', 'pending_online_users']
    data_version = 1000

    def __init__(self, logger):
        Greenlet.__init__(self)
        self.logger = logger
        self._users_lock = RLock()
        self._msgs = {}
        self._users = {}
        self.send_queue = Queue()
        self.pending_online_users = Queue()
        self.bootstrap()
        self._dying = False
        self.start()

    def bootstrap(self):
        """Restore data from disk"""
        _ = opath.join(DATA_DIR, DM_PKL_NAME)
        if opath.exists(_):
            _ = pickle.load(file(_, 'rb'))
            if '_version' not in _ or _['_version'] != DataMgr.data_version:
                raise Exception(" pkl file mismatch:program(%d) file(%d)" % (DataMgr.data_version, None if '_version' not in _ else _['_version']))
            self.__dict__.update(_)

    def shutdown(self):
        """Save data to disk"""
        self._dying = True
        self.logger.debug('[DM] saving data to disk...')
        self._save_cache()

    def reset(self):
        """reset in-memory data and disk data"""
        self.send_queue = Queue()
        self.pending_online_users = Queue()
        _ = opath.join(DATA_DIR, DM_PKL_NAME)
        if opath.exists(_):
            os.remove(_)

    def _save_cache(self):
        # fixme: save to external database not implemented
        _ = {'_version':DataMgr.data_version}
        for k in DataMgr.pickle_names:
            if k in self.__dict__:
                _[k] = self.__dict__[k]
        #pickle.dump(_, file(opath.join(DATA_DIR, DM_PKL_NAME), 'wb'), pickle.HIGHEST_PROTOCOL)

    def msg_add(self, msg):
        """add message to msg_queue
            :param msg: msg to add
            :type msg: MessageObj
        """
        if not isinstance(msg, MessageObj):
            raise ValueError(" argument is not a MessageObj")
        if msg.msgid not in self._msgs:
            self._msgs[msg.msgid] = msg

    def msg_get(self, msgid):
        """get message by msgid

            :param msgid: message id
            :type msgid: int
        """
        if msgid not in self._msgs:
            raise IndexError(" msgid %s not in queue" % idx)
        return self._msgs[msgid]

    def msg_del(self, msgid):
        """del message by msgid

            :param msgid: message id
            :type msgid: int
        """
        if msgid not in self._msgs:
            raise IndexError(" msgid %s not in queue" % idx)
        del self._msgs[msgid]
        # self._msgs.pop(msgid)

    def msg_set(self, msgid, msg):
        self._msgs[msgid] = msg

    @property
    def msg_count(self):
        """get message queue length
        """
        return len(self._msgs)
    
    def set_user_online(self, guid):
        """set user to online

        this will generate a UserObj instance

            :param guid: user guid
            :type guid: int
        """
        #TODO get userid from rid
        uid = "u" + guid
        u = UserObj(uid, guid)
        self.users_add(u)
        self.pending_online_users.put(guid)

    def set_user_offline(self, guid):
        """set user to offline

            :param guid: user guid
        """
        #TODO get userid from rid
        self.users_del(guid)

    def users_add(self, u):
        """add a user instance to user queue

            :param u: user instance
            :type u: UserObj
        """
        if not isinstance(u, UserObj):
            raise ValueError(" argument is not a UserObj")
        self._users_lock.acquire()
        self._users[u.guid] = u
        self._users_lock.release()

    def users_get(self, guid):
        """get user by guid

            :param guid: user guid
        """
        if guid not in self._users:
            raise InconsistentError(" guid %s not in users list" % guid)
        return self._users[guid]

    def users_del(self, guid):
        """del user by guid

            :param guid: user guid
        """
        if '-' in guid:  # convert to bytes
            guid = binascii.unhexlify(guid)
        if guid not in self._users:
            raise InconsistentError(" guid %s not in users list" % guid)
        self._users_lock.acquire()
        del self._users[guid]
        self._users_lock.release()

    @property
    def users_count(self):
        """get user queue length
        """
        return len(self._users)

    def make_bundle(self, send_func, user_keys = None):
        """make bundle and call send_func

            :param send_func: the function to call on generated bundles
            :type send_func: lambda, function, instancemethod
            :param user_keys: user guid list to do the match func
            :type send_func: list

        """
        self.mongo_instance = mongo()
        user_keys = user_keys or self._users.keys()
        self.logger.debug('[DM] begin mapping of %du * %dm' % (len(user_keys), self.msg_count))
        cnt = 0
        user_keys = sorted(user_keys, key = lambda x:self._users[x].pr, reverse = True)
        # monkey.patch_all()
        for k in user_keys:
            u = self._users[k]
            for _k, m in self._msgs.iteritems():
                event_dict = self.mongo_instance.get_pushed_event(u.guid)
                try:
                    event_list = event_dict["pushed_event"]
                except Exception, e:
                    event_list = []
                if m.msgid not in event_list:
                    _ = u.gen_bundle(m)
                    if _:
                        cnt += 1
                        send_func(_)
                        self.mongo_instance.insert_pushed_event(u.guid,m.msgid)
        if cnt:
            self.logger.debug('[DM] queued %d new bundles' % cnt)
        return cnt


    def run(self):
        """the background thread that automatically do n*m mapping
        """
        self.mongo_instance = mongo()
        while not self._dying:
            msgids,msg_off_ids = self.mongo_instance.event_get_id(int(time.time()))
            # msgids,msg_off_ids = self.mongo_instance.event_get_id(0)
            for i in msg_off_ids:
                x = i
                self.msg_del(x)
            for i in msgids:
                #generate new MessageObj instance
                x = i
                if x not in self._msgs:
                    m = MessageObj(
                        payload_callback = lambda d = i:self.mongo_instance.event_get_single_info(d),
                        msgid = i
                    )
                self.msg_add(m)
            gevent.sleep(60)
            self._save_cache()