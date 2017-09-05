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
def insert_data(item,corp_id):
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
        None,#企业介绍
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),#创建时间
        None,#创建人
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), #修改时间
        None #修改人
    )
    result = {}
    result['sql'] = sql
    result['params'] = params
    return result





def Lagou():
    database = Databases('localhost',3306,'scrapy','root','mysql')
    while 1:
        new_company_data = new_company_sql()
        new_company_result = database.select(new_company_data['sql'],new_company_data['params'])
        if new_company_result != None:
            # print new_company_result
            pisitionid = new_company_result['zj']
            print pisitionid
            #挂接查询数据
            rel_recruit_data = rel_recruit(pisitionid)
            print rel_recruit_data
            is_guajie = database.select(rel_recruit_data['sql'],str(rel_recruit_data['params']))

            # 更改新抓取数据状态status
            change_status_data = change_company_sql(rel_recruit_data['params'],'1')

            # 如果没有挂接则不处理！
            if is_guajie == None:
                change_status_data = change_company_sql(rel_recruit_data['params'], '2')
                status_result = database.excute(change_status_data['sql'], change_status_data['params'])
                print '未挂接'
                if status_result == 'ok':
                    print '状态已经改变！'
                continue
            status_result = database.excute(change_status_data['sql'],change_status_data['params'])

            if status_result == 'ok':
                print '状态已经改变！'

            companyid = is_guajie['companyid']

            #查询工作职位
            new_jobs_data = new_jobs_sql(pisitionid)
            # print new_jobs_data
            #新的工作职位
            new_jobs = database.selects(new_jobs_data['sql'],new_jobs_data['params'])
            # print new_jobs,'11111111'
            # print len(new_jobs)


            #插入t_recruit
            for each in new_jobs:
                insert_datas = insert_data(each,companyid)
                is_insert = database.excute(insert_datas['sql'],insert_datas['params'])
                if is_insert == 'ok':
                    print '新插入t_recruit成功'

            time.sleep(0.3)
        else:
            print '无查询结果!插入完毕!'
            break


def Jobs51():
    pass


if __name__ == '__main__':
    Lagou()