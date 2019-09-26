# !/usr/bin/env python
# _*_coding: utf-8 _*_
# @Time: 2019/6/4 9:04
from datetime import datetime

import bson
import os
import sys
import traceback

import pymongo
from pymongo import MongoClient

"""
    数据修复脚本模板：
    1、提供断点续读（从断点处的_id 往后继 续读，避免重复操作）
    2、规范日志文件名称，方便查询
    3、提供统一的取值框架，减少 reviewer 工作量 
"""


def logger(fl_name, msg):  # 只需传入文件名称，自动生成以当前脚本名称为前缀的 .txt 文件，保存到相对路径下，作为日志文件。
    logger_file_name = os.path.basename(sys.argv[0])[:-3] + fl_name + ".txt"
    with open(logger_file_name, "a") as f:
        f.write(msg)
        f.write("\n")


def get_last_id(file_name):  # 获取文件最后一个合法的 _id，用于断点续读
    with open(file_name, 'r') as f:

        index = -1
        while True:
            _id = f.readlines()[index].strip()
            if len(_id) == 24:
                return _id

            logger("_LoggerMsg", f"Get the {-index} th _id error; Current _id: {_id}")
            index -= 1


def find_documents(conn, data_base, collection, query, projection, sort_key="_id", sort_value=pymongo.ASCENDING, limits=0):
    # 默认根据_id 升序排列，不限制返回的结果数量
    _docs = conn[data_base][collection].find(query, projection).sort(sort_key, sort_value).limit(limits)
    docs = [item for item in _docs]  # 将结果集放到一个 list 中，方便计数
    return docs


def main(uri, more_filter, starts='', ends='', limits=10000):
    """


    :param uri:
    :param more_filter:  除 _id 外的过滤条件， 如不需要，可以不用传这个参数；若传递的话，应该是个 Map。
    :param starts: 自定义起始的 ObjectID . 不传的话，则会去表中查第一条 ObjectID，或者从文件中读取最后一条 ObjectID
    :param ends:   自定义结束的 ObjectID . 不传的话，则会去表中查最后一条的 ObjectID
    :param limits:  限制每次返回的数据量.
    :return:
    """

    start_id = ''
    current_id = ''
    exception_count = 0
    has_query_count = 0
    has_read_id_count = 0
    conn = MongoClient(host=uri)
    current_file_name = os.path.basename(sys.argv[0])[:-3]

    if os.path.exists(f"{current_file_name}_HasReadIds.txt"):  # 读取本地文件中最后一个 _id
        try:
            start_id = get_last_id(f"{current_file_name}_HasReadIds.txt")
        except Exception as e:
            msg = str(e) + ",trace:" + traceback.format_exc()
            logger("_LoggerMsg", f"Failed to get last id, exit! Error Msg {msg}.")
            sys.exit()

    if not start_id:  # 读取数据库中的第一条 _id
        one_doc = conn.DataAnalysis.GeoCityError.find({}, projection={"_id": 1}).sort("_id", pymongo.ASCENDING)  # 修改库名和表名
        start_id = one_doc[0]["_id"]
        logger('_HasReadIds', str(start_id))

    if not ends:  # 不传入终点 _id, 去库中取最后一条的 _id。
        ends = conn.DataAnalysis.GeoCityError.find({}, projection={"_id": 1}).sort("_id", pymongo.DESCENDING).limit(1)[0]["_id"]  # 修改库名和表名

    end_id = bson.ObjectId(ends)
    query = {"_id": {"$gte": bson.ObjectId(start_id)}}

    if starts:  # 传入起点，则以传入的 objectId 为起点，否则以库中查询的第一条或者读取本地文件。
        query = {"_id": {"$gte": bson.ObjectId(starts)}}

    if more_filter:
        query.update(more_filter)

    while exception_count < 20:  # 捕获异常20 次，则退出检查

        has_query_count += 1
        docs = find_documents(conn, "DataBase", "Collection", query, None, "_id", pymongo.ASCENDING, limits)  # 修改库名和表名
        logger("_LoggerMsg", f"******************** Has queried {has_query_count}*{limits}={has_query_count * limits}  documents. Time: {datetime.today()}. ********************")

        try:
            if not docs:  # 查询结果为空，直接退出
                logger("_LoggerMsg", f"Empty docs, exits! Time:{datetime.today()}, last _id is: {current_id}.")
                return

            for doc in docs:

                _id = doc.get("_id")
                current_id = _id

                if current_id > end_id:  # 程序退出条件
                    logger("_LoggerMsg", f"Get end point, and mission is over! Time:{datetime.today()}, last _id is: {current_id}.")
                    return

                has_read_id_count += 1
                if not has_read_id_count % 1000:  # 每一千个_id ，保存一次
                    logger("_HasReadIds", str(current_id))

                # 核心处理逻辑
                # 核心处理逻辑
                # 核心处理逻辑
                # 核心处理逻辑

                query["_id"] = {"$gt": current_id}  # 新的query

        except Exception as e:
            query["_id"] = {"$gt": current_id}  # 新的query
            logger("_LoggerMsg", f'Get error, exception msg is {str(e) + ",trace:" + traceback.format_exc()}, Time: {datetime.today()}, current _id is: {current_id}.')
            exception_count += 1

    logger("_LoggerMsg", f"Catch exception 20 times, mission is over. Ending time:{datetime.today()}, Last _id is: {current_id}.")


def sandbox():
    uri = ""
    start = ""
    end = ""
    more_filter = {}
    limit = 5
    main(uri, more_filter, start, end, limit)


def online_few(uri, limit=100):
    online_start = ""
    online_end = ""
    more_filter = {}
    main(uri, more_filter, online_start, online_end, limit)


def online_all(uri, limit=1000):
    more_filter = {}
    main(uri, more_filter, "", "", limit)


if __name__ == '__main__':
    # sandbox()
    pass
