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
    id varchar(20) NOT NULL COMMENT '用户ID',
    screen_name varchar(30) COMMENT '用户名',
    gender varchar(10) COMMENT '性别',
    statuses_count INT COMMENT '微博数量',
    followers_count INT COMMENT '关注者数量',
    follow_count INT COMMENT '关注数量',
    registration_time varchar(20) COMMENT '注册时间',
    sunshine varchar(20) COMMENT '阳光信用',
    birthday varchar(40) COMMENT '生日',
    location varchar(200) COMMENT '所在地',
    education varchar(200) COMMENT '教育背景',
    company varchar(200) COMMENT '公司',
    description varchar(400) COMMENT '个人简介',
    profile_url varchar(200) COMMENT '个人主页链接',
    profile_image_url varchar(200) COMMENT '个人头像链接',
    avatar_hd varchar(200) COMMENT 'HD头像链接',
    urank INT COMMENT '用户等级',
    mbrank INT COMMENT '微博等级',
    verified BOOLEAN DEFAULT 0 COMMENT '是否认证',
    verified_type INT COMMENT '认证类型',
    verified_reason varchar(140) COMMENT '认证原因',
                    PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '微博用户表';  
    """)
    # 创建'weibo_weibo'表
    mysql_create_table(self, """
    CREATE TABLE IF NOT EXISTS weibo (
    id varchar(20) NOT NULL COMMENT '微博ID',
    bid varchar(12) NOT NULL COMMENT '微博bid',
    user_id varchar(20) COMMENT '用户ID',
    screen_name varchar(30) COMMENT '用户名',
    text varchar(20000) COMMENT '微博内容',
    article_url varchar(100) COMMENT '文章链接',
    topics varchar(200) COMMENT '话题',
    at_users varchar(1000) COMMENT '@的用户',
    pics varchar(3000) COMMENT '图片链接',
    video_url varchar(1000) COMMENT '视频链接',
    location varchar(100) COMMENT '发布位置',
    created_at DATETIME COMMENT '发布时间',
    source varchar(30) COMMENT '发布来源',
    attitudes_count INT COMMENT '点赞数',
    comments_count INT COMMENT '评论数',
    reposts_count INT COMMENT '转发数',
    retweet_id varchar(20) COMMENT '转发的微博ID',
                    PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '微博表';  
            """)
    # 创建'weibo_bins'表
    mysql_create_table(self, """
    CREATE TABLE IF NOT EXISTS weibo_bins (
    id integer PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    ext varchar(10) NOT NULL /*file extension*/ COMMENT '文件扩展名',
    data blob NOT NULL COMMENT '二进制数据',
    weibo_id varchar(20) COMMENT '微博ID',
    comment_id varchar(20) COMMENT '评论ID',
    path text COMMENT '路径',
    url text COMMENT 'URL'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '微博用户评论绑定关系'; 
            """)
    # 创建'weibo_comments'表

    mysql_create_table(self, """
    CREATE TABLE IF NOT EXISTS weibo_comments (
    id varchar(20) NOT NULL COMMENT '评论ID',
    bid varchar(20) NOT NULL COMMENT '微博bid',
    weibo_id varchar(32) NOT NULL COMMENT '微博ID',
    root_id varchar(20) COMMENT '根评论ID',
    user_id varchar(20) NOT NULL COMMENT '用户ID',
    created_at varchar(20) COMMENT '创建时间',
    user_screen_name varchar(64) NOT NULL COMMENT '用户名',
    user_avatar_url text COMMENT '用户头像链接',
    text varchar(1000) COMMENT '评论内容',
    pic_url text COMMENT '评论图片链接',
    like_count integer COMMENT '点赞数',
                    PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '微博评论';
            """)
    # 创建'weibo_reposts'表

    mysql_create_table(self, """
    CREATE TABLE IF NOT EXISTS weibo_reposts (
    id varchar(20) NOT NULL COMMENT '转发ID',
    bid varchar(20) NOT NULL COMMENT '微博bid',
    weibo_id varchar(32) NOT NULL COMMENT '微博ID',
    user_id varchar(20) NOT NULL COMMENT '用户ID',
    created_at varchar(20) COMMENT '创建时间',
    user_screen_name varchar(64) NOT NULL COMMENT '用户名',
    user_avatar_url text COMMENT '用户头像链接',
    text varchar(1000) COMMENT '转发内容',
    like_count integer COMMENT '点赞数',
                    PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '微博转发';
            """)
