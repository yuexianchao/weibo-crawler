#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
 * 项目名称：weibo-crawler
 * 类 名 称：.logutil.py
 * 类 描 述：TODO
 * 开 发 人：yuexianchao
 * 开发时间：2024/7/2 15:10
 * 测 试 人：
 * 对接业务人：
 * 线上脚本：
 * 线下脚本：
 * 技术方案文档：
 * 版    本: 1.0
 * 更 新 人: 
 * 更新时间: 2024/7/2 15:10
"""
import os
import warnings
import logging.config

warnings.filterwarnings("ignore")
dir_project = os.path.split(os.path.realpath(__file__))[0]
dir_project = dir_project.split("/util")[0] + os.sep
# 如果日志文件夹不存在，则创建
if not os.path.isdir(dir_project):
    os.makedirs(dir_project)
logging_path = dir_project + "logging.conf"
logging.config.fileConfig(logging_path)
logger = logging.getLogger("weibo")



# # 如果日志文件夹不存在，则创建
# if not os.path.isdir("log/"):
#     os.makedirs("log/")
# logging_path = os.path.split(os.path.realpath(__file__))[0] + os.sep + "logging.conf"
# logging.config.fileConfig(logging_path)
# logger = logging.getLogger("weibo")
