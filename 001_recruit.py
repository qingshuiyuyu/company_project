#! /user/bin/env python
# -*- coding=UTF-8 -*-

import pymysql
import uuid

#定义数据库类,完成各种crud
class Databases(object):
    #初始化数据库
    def __init__(self,host,port,db,user,passwd,charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor ):
            self.__host = host
            self.__port = port
            self.__db = db
            self.__user = user
            self.__passwd = passwd
            self.__charset = charset
            self.__cursorclass=cursorclass

    # 建立连接并获得cursor对象
    def __open(self):
        self.__conn = pymysql.connect(
            host=self.__host,
            port=self.__port,
            db=self.__db,
            user=self.__user,
            passwd=self.__passwd,
            charset=self.__charset,
            cursorclass = self.__cursorclass
            )
        self.__cursor = self.__conn.cursor()

    # 关闭cursor及conn对象
    def __close(self):
        self.__cursor.close()
        self.__conn.close()

    #查询
    def select(self,sql,params):
        try:
            self.__open()
            self.__cursor.execute(sql,params)
            result = self.__cursor.fetchone()
            return result
        except Exception,e:
            print e
        finally:
            self.__close()
    #增删改
    def insert(self,sql,params):
        try:
            self.__open()
            self.__cursor.execute(sql,params)
            self.__conn.commit()
            return 'ok'
        except Exception,e:
            print e
        finally:
            self.__close()

#新抓取数据查询
def new_company_sql():
    sql = "SELECT %s FROM `zp_lagou_company_full` WHERE `status`=0"
    params = ('zj')
    result = {}
    result['sql'] = sql
    result['params'] = params
    return result
#改变数据新抓取数据库状态码
def change_company_sql(zj):
    sql = "update zp_lagou_company_full set status=1 where zj=%s"
    params = (zj)
    result = {}
    result['sql'] = sql
    result['params'] = params
    return result
#查询是否已经挂接
def rel_recruit(relationid,resource):
    sql = "select %s from `rel_recruit_pre` WHERE relationid=%s"
    params = (relationid,resource)
    result = {}
    result['sql'] = sql
    result['params'] = params
    return result
#插入已经挂接数据
def insert_data():
    pass






def main():
    database = Databases('localhost',3306,'scrapy','root','mysql')



if __name__ == '__main__':
    pass