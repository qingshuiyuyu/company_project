#! /user/bin/env python
# -*- coding=UTF-8 -*-

import pymysql
import uuid
import datetime
import time
from models import *


def new_51company():
    sql = """select * from `zp_51job_company_full` where status = %s"""
    params = ('0')
    result = {}
    result['sql'] = sql
    result['params'] = params
    return result


def rel_51recruit(resource):
    sql = "select companyid from `rel_recruit_pre` WHERE relationid=%s"
    params = (resource)
    result = {}
    result['sql'] = sql
    result['params'] = params
    return result

def is_inlagou(companyname):
    sql = """select company_name from `zp_lagou_company_full` where company_name = %s"""
    params = {companyname}
    result = {}
    result['sql'] = sql
    result['params'] = params
    return result
#构建改变51jobs状态status的状态语句
def change_51status(companyid,status):
    sql = "update zp_51job_company_full set status=%s where id=%s"
    params = (status, companyid)
    result = {}
    result['sql'] = sql
    result['params'] = params
    return result

def new_51jobs(companyid):
    sql = "select * from `zp_51job_job_full` where companyid=%s"
    params = (companyid)
    result = {}
    result['sql'] = sql
    result['params'] = params
    return result

def insert_51jobs(item,crop_id,companyinfo):
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
        str(uuid.uuid1()).replace('-','').upper(),#职位的uuid
        crop_id,#挂接的名字
        item['jobname'],#职位名字
        None,
        item['jobaddr'],#工作城市
        companyinfo['addr'],#工作地点
        item['salary'],#薪资待遇
        item['publishtime'],#发布时间
        item['edu'],#教育水平
        item['year'],#工作经验
        item['laung'],#语言
        None,#年龄
        None,#所属部门
        item['domain'],#专业要求
        None,#回报对象
        item['neednum'],#需要人数
        item['jobdescription'],#职位描述
        companyinfo['memo'],#企业介绍
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # 创建时间
        None,#创建人
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # 修改时间
        None#修改人
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
                insert_datas = insert_data(each,companyid,new_company_result)
                is_insert = database.excute(insert_datas['sql'],insert_datas['params'])
                if is_insert == 'ok':
                    print '新插入t_recruit成功'

            time.sleep(0.3)
        else:
            print '无查询结果!插入完毕!'
            break


def Jobs51():
    database = Databases('localhost',3306,'scrapy','root','mysql')
    while 1:
        new_51company_data = new_51company()
        new_51company_result = database.select(new_51company_data['sql'],new_51company_data['params'])
        if new_51company_result != None:
            positionid = new_51company_result.get('id')
            companyname = new_51company_result.get("name")

            rel_51recruit_data =  rel_51recruit(positionid)#构建查询挂接数据
            is_inlagou_data = is_inlagou(companyname)#构建是否在拉勾查询

            rel_51recruit_result = database.select(rel_51recruit_data['sql'],rel_51recruit_data['params'])#是否挂接
            is_inlagou_result = database.select(is_inlagou_data['sql'],is_inlagou_data['params'])#是否在拉勾里

            # 更改新抓取数据状态status
            change_51status_data = change_51status(positionid,'1')

            if rel_51recruit_result == None:
                print '该公司未挂接!'
                change_51status_data = change_51status(positionid, '2')
                change_51status_result = database.excute(change_51status_data['sql'],change_51status_data['params'])
                if change_51status_result == 'ok':
                    print '公司状态已经改为2'
                    continue
            elif rel_51recruit_result != None and is_inlagou_result != None:
                print '该公司已经有拉勾关联'
                change_51status_data = change_51status(positionid, '1')
                change_51status_result = database.excute(change_51status_data['sql'], change_51status_data['params'])
                if change_51status_result == 'ok':
                    print '公司状态已经改为1'
                    continue
            print '该公司已经关联'
            change_51status_data = change_51status(positionid, '1')
            change_51status_result = database.excute(change_51status_data['sql'], change_51status_data['params'])
            if change_51status_result == 'ok':
                print '公司状态已经改为1'


            #查询51jobs中几经关联公司的职位
            new_51jobs_data = new_51jobs(positionid)
            new_51jobs_result = database.selects(new_51jobs_data['sql'],new_51jobs_data['params'])

            companyid = rel_51recruit_result.get("companyid")
            print companyid

            if new_51jobs_result != None:
                for each in new_51jobs_result:
                    insert_51jobs_data = insert_51jobs(each,companyid,new_51company_result)
                    insert_51jobs_result = database.excute(insert_51jobs_data['sql'],insert_51jobs_data['params'])
                    if insert_51jobs_result == 'ok':
                        print '新插入一条！'
        else:
            print '已经处理完毕,数据库中无未处理数据'



if __name__ == '__main__':
    Lagou()
    # Jobs51()