#!/usr/bin/python
# -*- coding: utf-8 -*-
# import pymongo

import MySQLdb
import time
import random
import string
from gevent import monkey
monkey.patch_all()
from _mysql_exceptions import IntegrityError
from pymongo import MongoClient

from config import *

class mysql(object):

    """docstring for connector_MySQL"""
    login_timeout = 1800

    def get_token(self, uid):
        retry_count = 2
        db = MySQLdb.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWD, "Register")
        for i in range(retry_count):
            try:
                cur = db.cursor()
                _token = ''.join(
                    random.sample(string.digits + string.ascii_letters, 32))
                str_1 = "SELECT * FROM Token WHERE uid = %d " % (uid)
                if cur.execute(str_1) == 0:
                    str_2 = "INSERT INTO Token (uid,timeout,token) VALUES (%d, %d, \"%s\" )" % (
                        uid,
                        int(time.time() + login_timeout),
                        _token
                    )
                    cur.execute(str_2)
                    cur.close
                    return True
                else:
                    str_2 = "UPDATE Token SET timeout = %d,token = \"%s\" WHERE uid = %d " % (
                        int(time.time() + login_timeout), _token, uid)
                    cur.execute(str_2)
                    cur.close
                    return True
            except IntegrityError as e:
                if e[0] == 1062:
                    return e[1]
                else:
                    return False
            except Exception, e:
                return False

    def find_uid(self, _token):
        db = MySQLdb.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWD, "Register")
        cur = db.cursor()
        str_1 = "SELECT uid FROM Token WHERE token = \"%s\" " % (_token)
        if cur.execute(str_1) == 0:
            return False
        else:
            _uid = cur.fetchone()[0]
            str_2 = "SELECT timeout FROM Token WHERE token = \"%s\" " % (
                _token)
            cur.execute(str_2)
            _timeout = cur.fetchone()[0]
            # print _timeout
            # print int(time.time())
            if ((_timeout - login_timeout) <= int(time.time())) and (int(time.time()) <= _timeout):
                str_3 = "UPDATE Token SET timeout = %d WHERE token = \"%s\" " % (
                    int(time.time()), _token)
                cur.execute(str_3)
                return _uid
            else:
                return False

class mongo(object):
#     """Summary of class here.

#     this class is used for Event_server to get the very data of Events and Users. 
#     """

    # conn = pymongo.Connection("127.0.0.1",12345)
    def __init__(self):
        conn = MongoClient(MONGO_HOST, 12345)
        self.db = conn.admin
        self.db.authenticate(MONGO_USER, MONGO_PASSWD)
        self.db = conn.core

    #插入指定的用户标签(重复插入返回False，第一次插入返回True)
    def tag_insert(self, cUser_id, tag):
        """Summary of function here.

        this is the function to insert tags of user 
        cUser_id is the id of user
        tag is the tag to be inserted"""
        if tag.decode('utf-8') in self.db.cUser.find_one({"_id":cUser_id})['cUser_tag']:
            return False
        else:
            self.db.cUser.update({"_id":cUser_id},{"$push":{"cUser_tag":tag}})
            return True

    #删除指定的用户标签
    def tag_remove(self,cUser_id,tag):
        self.db.cUser.update({"_id":cUser_id},{"$pull":{"cUser_tag":tag}})

    #加好友（重复插入返回False，第一次插入返回True）
    def friend_insert(self, cUser_id,f_id):
        if f_id in self.db.cUser.find_one({"_id":cUser_id})['cUser_friend']:
            return False
        else:
            self.db.cUser.update({"_id":cUser_id},{"$push":{"cUser_friend":f_id}})
            return True

    #删除好友
    def friend_remove(self,cUser_id,f_id):
        self.db.cUser.update({"_id":cUser_id},{"$pull":{"cUser_friend":f_id}})

    #获取用户信息，返回对应键的值，返回类型根据输入参数的不同而定，
    #name和email返回字符串
    #rank返回int
    #tag返回字符串数组
    #major返回Json对象
    def user_info_1(self, cUser_id, key):
        try:
            return self.db.cUser.find_one({"_id":cUser_id},{key:1})[key]
        except:
            return False

    #获取用户信息，返回类型为Json对象
    def user_info_2(self,cUser_id,key_1,key_2):
        try:
            return self.db.cUser.find_one({"_id":cUser_id},{key_1:1,key_2:1})
        except:
            return False

    #获取用户信息，返回类型为Json对象
    def user_info_3(self,cUser_id,key_1,key_2,key_3):    
        try:
            return self.db.cUser.find_one({"_id":cUser_id},{key_1:1,key_2:1,key_3:1})
        except:
            return False

    #获取用户信息，返回类型为Json对象
    def user_info_3(self,cUser_id,key_1,key_2,key_3,key_4):    
        try:
            return self.db.cUser.find_one({"_id":cUser_id},{key_1:1,key_2:1,key_3:1,key_4:1})
        except:
            return False    

    #获取用户信息（有用信息），返回类型为Json对象
    def user_info(self, cUser_id):
        try:
            return self.db.cUser.find_one({"_id":cUser_id},{"cUser_name":1,"cUser_tag":1,"cUser_rank":1,"cUser_major":1})
        except:
            return False
            
    #获取有效活动ID(返回活动截止时间大于查询时间的所有活动id，并且把过期的活动转移到cEvent_off集合中)
    def event_get_id(self, push_time):
        try:
            event_id = []
            for i in self.db.cEvent.find():
                _len = len(i['cEvent_time'])
                if i['cEvent_time'][_len-1]>push_time:
                    event_id.append(i['_id'])
                else:
                    self.db.cEvent_off.insert(i)
                    self.db.cEvent.remove({"_id":i['_id']})
            return event_id
        except:
            return []

    #获取有效活动信息，输入参数为数组，返回类型为Json对象数组
    def event_get_single_info(self, cEvent_id):
        return self.db.cEvent.find_one({"_id":cEvent_id},
            {"cEvent_name":1,"cEvent_time":1,"cEvent_content":1,
            "cEvent_theme":1,"cEvent_place":1,"cEvent_provider":1})
            
    #获取有效活动信息，输入参数为数组，返回类型为Json对象数组
    def event_get_info(self, cEvent_id):
        content = []
        try:
            for i in cEvent_id:
                content.append(self.db.cEvent.find_one({"_id":i},
                    {"cEvent_name":1,"cEvent_time":1,"cEvent_content":1,
                    "cEvent_theme":1,"cEvent_place":1,"cEvent_provider":1}))
            return content
        except:
            return []

    #查询过期活动信息，返回类型为Json对象
    def event_off_info(self, cEvent_id):
        try:
            return self.db.cEvent_off.find_one({"_id":cEvent_id})
        except:
            return False

    #查询活动评论，返回类型为Json对象数组
    def comments_get(self, cEvent_id):
        try:
            return self.db.cEvent.find_one({"_id":cEvent_id},{"cEvent_comment":1})["cEvent_comment"]
        except:
            return False

    #插入一条评论
    def comment_insert(self, cEvent_id, cUser_id, content):
        icon = self.db.cUser.find_one({"_id":cUser_id},{"cUser_icon":1})["cUser_icon"]
        self.db.cEvent.update({"_id":cEvent_id},{"$push":
            {"cEvent_comment":{"speaker_id":cUser_id,"speaker_icon":icon,"content":content}}})

