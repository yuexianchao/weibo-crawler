#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
 * 项目名称：weibo-crawler
 * 类 名 称：.sqliteutil.py
 * 类 描 述：TODO
 * 开 发 人：yuexianchao
 * 开发时间：2024/7/2 15:24
 * 测 试 人：
 * 对接业务人：
 * 线上脚本：
 * 线下脚本：
 * 技术方案文档：
 * 版    本: 1.0
 * 更 新 人: 
 * 更新时间: 2024/7/2 15:24
"""
import os
import sqlite3

from util.logutil import logger


def get_sqlte_path():
    return "./weibo/weibodata.db"

def create_sqlite_table(connection: sqlite3.Connection):
    sql = get_sql_create_sql()
    cur = connection.cursor()
    cur.executescript(sql)
    connection.commit()

def get_sqlite_connection():
    path = get_sqlte_path()
    create = False
    if not os.path.exists(path):
        create = True

    con = sqlite3.connect(path)

    if create == True:
        create_sqlite_table(connection=con)

    return con


def sqlite_insert(con: sqlite3.Connection, data: dict, table: str):
    if not data:
        return
    cur = con.cursor()
    keys = ",".join(data.keys())
    values = ",".join(["?"] * len(data))
    sql = """INSERT OR REPLACE INTO {table}({keys}) VALUES({values})
            """.format(
        table=table, keys=keys, values=values
    )
    cur.execute(sql, list(data.values()))
    con.commit()

def get_sql_create_sql():
    """CREATE TABLE IF NOT EXISTS user (
                id varchar(64) NOT NULL
                ,nick_name varchar(64) NOT NULL
                ,gender varchar(6)
                ,follower_count integer
                ,follow_count integer
                ,birthday varchar(10)
                ,location varchar(32)
                ,edu varchar(32)
                ,company varchar(32)
                ,reg_date DATETIME
                ,main_page_url text
                ,avatar_url text
                ,bio text
                ,PRIMARY KEY (id)
            )  ;"""
    create_sql = """
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
                );
                
            CREATE TABLE IF NOT EXISTS weibo (
                id varchar(20) NOT NULL
                ,bid varchar(12) NOT NULL
                ,user_id varchar(20)
                ,screen_name varchar(30)
                ,text varchar(2000)
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
            )  ;
            CREATE TABLE IF NOT EXISTS weibo_bins (
                id integer PRIMARY KEY AUTOINCREMENT
                ,ext varchar(10) NOT NULL /*file extension*/
                ,data blob NOT NULL
                ,weibo_id varchar(20)
                ,comment_id varchar(20)
                ,path text
                ,url text
            )  ;
            CREATE TABLE IF NOT EXISTS weibo_comments (
                id varchar(20) NOT NULL
                ,bid varchar(20) NOT NULL
                ,weibo_id varchar(32) NOT NULL
                ,root_id varchar(20) 
                ,user_id varchar(20) NOT NULL
                ,created_at DATETIME
                ,user_screen_name varchar(64) NOT NULL
                ,user_avatar_url text
                ,text varchar(1000)
                ,pic_url text
                ,like_count integer
                ,PRIMARY KEY (id)
            ) ;

            CREATE TABLE IF NOT EXISTS weibo_reposts (
                id varchar(20) NOT NULL
                ,bid varchar(20) NOT NULL
                ,weibo_id varchar(32) NOT NULL
                ,user_id varchar(20) NOT NULL
                ,created_at DATETIME
                ,user_screen_name varchar(64) NOT NULL
                ,user_avatar_url text
                ,text varchar(1000)
                ,like_count integer
                ,PRIMARY KEY (id)
            )  
            """
    return create_sql