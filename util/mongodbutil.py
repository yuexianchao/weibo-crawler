#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
 * 项目名称：weibo-crawler
 * 类 名 称：.mongodb
 * 类 描 述：TODO
 * 开 发 人：yuexianchao
 * 开发时间：2024/7/2 15:13
 * 测 试 人：
 * 对接业务人：
 * 线上脚本：
 * 线下脚本：
 * 技术方案文档：
 * 版    本: 1.0
 * 更 新 人: 
 * 更新时间: 2024/7/2 15:13
"""
import copy
import sys

from util.logutil import logger

def info_to_mongodb(self, collection, info_list):
    try:
        import pymongo
    except ImportError:
        logger.warning("系统中可能没有安装pymongo库，请先运行 pip install pymongo ，再运行程序")
        sys.exit()
    try:
        from pymongo import MongoClient

        client = MongoClient(self.mongodb_URI)
        db = client["weibo"]
        collection = db[collection]
        if len(self.write_mode) > 1:
            new_info_list = copy.deepcopy(info_list)
        else:
            new_info_list = info_list
        for info in new_info_list:
            if not collection.find_one({"id": info["id"]}):
                collection.insert_one(info)
            else:
                collection.update_one({"id": info["id"]}, {"$set": info})
    except pymongo.errors.ServerSelectionTimeoutError:
        logger.warning("系统中可能没有安装或启动MongoDB数据库，请先根据系统环境安装或启动MongoDB，再运行程序")
        sys.exit()
