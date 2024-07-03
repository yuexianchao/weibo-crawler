#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
 * 项目名称：weibo-crawler
 * 类 名 称：.mysqlutil.py
 * 类 描 述：TODO
 * 开 发 人：yuexianchao
 * 开发时间：2024/7/2 15:06
 * 测 试 人：
 * 对接业务人：
 * 线上脚本：
 * 线下脚本：
 * 技术方案文档：
 * 版    本: 1.0
 * 更 新 人: 
 * 更新时间: 2024/7/2 15:06
"""
import sys
from util.logutil import logger

"""创建MySQL数据库或表"""


def mysql_create(connection, sql):
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
    except Exception as e:
        logger.warning("执行sql异常" + sql)
    finally:
        connection.close()


"""创建MySQL数据库"""


def mysql_create_database(self, sql):
    try:
        import pymysql
    except ImportError:
        logger.warning("系统中可能没有安装pymysql库，请先运行 pip install pymysql ，再运行程序")
        sys.exit()
    try:
        connection = pymysql.connect(**self.mysql_config)
        mysql_create(connection, sql)
    except pymysql.OperationalError:
        logger.warning("系统中可能没有安装或正确配置MySQL数据库，请先根据系统环境安装或配置MySQL，再运行程序")
        sys.exit()


"""创建MySQL表"""


def mysql_create_table(self, sql):
    import pymysql
    connection = pymysql.connect(**self.mysql_config)
    mysql_create(connection, sql)


def mysql_insert(self, table, data_list):
    import pymysql
    if len(data_list) > 0:
        keys = ", ".join(data_list[0].keys())
        values = ", ".join(["%s"] * len(data_list[0]))
        # mysql_config["db"] = "weibo"
        connection = pymysql.connect(**self.mysql_config)
        cursor = connection.cursor()
        sql = """INSERT INTO {table}({keys}) VALUES ({values}) ON
                 DUPLICATE KEY UPDATE""".format(
            table=table, keys=keys, values=values
        )
        update = ",".join(
            [" {key} = values({key})".format(key=key) for key in data_list[0]]
        )
        sql += update
        try:
            cursor.executemany(sql, [tuple(data.values()) for data in data_list])
            connection.commit()
        except Exception as e:
            connection.rollback()
            logger.exception(e)
        finally:
            connection.close()


def init_mysql(self):
    # 创建'weibo'数据库
    create_database = """CREATE DATABASE IF NOT EXISTS """ + self.mysql_config.get("database") + """ DEFAULT
                     CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"""

    mysql_create_database(self, create_database)
    # 创建'weibo_user'表
    mysql_create_table(self, """
             CREATE TABLE IF NOT EXISTS weibo_user (
                id varchar(20) NOT NULL,
                screen_name varchar(30),
                gender varchar(10),
                statuses_count INT,
                followers_count INT,
                follow_count INT,
                registration_time varchar(20),
                sunshine varchar(20),
                birthday varchar(40),
                location varchar(200),
                education varchar(200),
                company varchar(200),
                description varchar(400),
                profile_url varchar(200),
                profile_image_url varchar(200),
                avatar_hd varchar(200),
                urank INT,
                mbrank INT,
                verified BOOLEAN DEFAULT 0,
                verified_type INT,
                verified_reason varchar(140),
                PRIMARY KEY (id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
    # 创建'weibo_weibo'表
    mysql_create_table(self, """
             CREATE TABLE IF NOT EXISTS weibo (
                id varchar(20) NOT NULL
                ,bid varchar(12) NOT NULL
                ,user_id varchar(20)
                ,screen_name varchar(30)
                ,text varchar(20000)
                ,article_url varchar(100)
                ,topics varchar(200)
                ,at_users varchar(1000)
                ,pics varchar(3000)
                ,video_url varchar(1000)
                ,location varchar(100)
                ,created_at DATETIME
                ,source varchar(30)
                ,attitudes_count INT
                ,comments_count INT
                ,reposts_count INT
                ,retweet_id varchar(20)
                ,PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
    # 创建'weibo_bins'表

    mysql_create_table(self, """
             CREATE TABLE IF NOT EXISTS weibo_bins (
                id integer PRIMARY KEY AUTO_INCREMENT
                ,ext varchar(10) NOT NULL /*file extension*/
                ,data blob NOT NULL
                ,weibo_id varchar(20)
                ,comment_id varchar(20)
                ,path text
                ,url text
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
    # 创建'weibo_comments'表

    mysql_create_table(self, """
             CREATE TABLE IF NOT EXISTS weibo_comments (
                id varchar(20) NOT NULL
                ,bid varchar(20) NOT NULL
                ,weibo_id varchar(32) NOT NULL
                ,root_id varchar(20)
                ,user_id varchar(20) NOT NULL
                ,created_at varchar(20)
                ,user_screen_name varchar(64) NOT NULL
                ,user_avatar_url text
                ,text varchar(1000)
                ,pic_url text
                ,like_count integer
                ,PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
    # 创建'weibo_reposts'表

    mysql_create_table(self, """
             CREATE TABLE IF NOT EXISTS weibo_reposts (
                id varchar(20) NOT NULL
                ,bid varchar(20) NOT NULL
                ,weibo_id varchar(32) NOT NULL
                ,user_id varchar(20) NOT NULL
                ,created_at varchar(20)
                ,user_screen_name varchar(64) NOT NULL
                ,user_avatar_url text
                ,text varchar(1000)
                ,like_count integer
                ,PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
