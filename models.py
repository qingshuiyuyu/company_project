#! /user/bin/env python
# -*- coding=UTF-8 -*-

import pymysql
import uuid
import datetime
import time

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

    def selects(self, sql, params):
        try:
            self.__open()
            self.__cursor.execute(sql, params)
            result = self.__cursor.fetchall()
            return result
        except Exception, e:
            print e
        finally:
            self.__close()
    #增删改
    def excute(self,sql,params):
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
    sql = "SELECT * FROM `zp_lagou_company_full` WHERE `status`=%s"
    params = ('0')
    result = {}
    result['sql'] = sql
    result['params'] = params
    return result

#构建查询新抓取职位的查询信息
def new_jobs_sql(companyzj):
    sql = "select * from `zp_lagou_job_source` where companyzj=%s"
    params = (companyzj)
    result = {}
    result['sql'] = sql
    result['params'] = params
    return result


#改变数据新抓取数据库状态码
def change_company_sql(zj,status):
    sql = "update zp_lagou_company_full set status=%s where zj=%s"
    params = (status,zj)
    result = {}
    result['sql'] = sql
    result['params'] = params
    return result

#查询是否已经挂接
def rel_recruit(resource):
    sql = "select companyid from `rel_recruit_pre` WHERE relationid=%s"
    params = (resource)
    result = {}
    result['sql'] = sql
    result['params'] = params
    return result

#插入已经挂接数据
def insert_data(item,corp_id,companyinfo):
    sql = """
    insert into t_recruit (
            id,
            corpid,
            position,
            provinceid,
            provincename,
            address,
            salary,
            publishtime,
            education,
            experience,
            language,
            age,
            department,
            major,
            report,
            subordinates,
            jobdescription,
            corpintroduction,
            createtime,
            creatorid,
            updatetime,
            updatorid
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    params = (
        item['uuid'].upper(),#uuid
        corp_id,#挂接后的公司id
        item['name'],#职位名
        None,#省份id
        item['city'],#工作城市
        item['address'],#工作地点
        item['salary'],#薪资待遇
        item['date_str']+ " 00:00:00",#发布时间
        item['degree'],#教育水平
        item['exp'],#经验要求
        None,#语言
        None,#年龄
        None,#所属部门
        None,#专业要求
        None,#汇报对象
        None,#招聘人数
        item['job_bt'],#职位描述
        companyinfo['company_memo'],#企业介绍
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),#创建时间
        None,#创建人
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), #修改时间
        None #修改人
    )
    result = {}
    result['sql'] = sql
    result['params'] = params
    return result