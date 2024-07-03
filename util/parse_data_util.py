#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
 * 项目名称：weibo-crawler
 * 类 名 称：.parse_data_util.py
 * 类 描 述：TODO
 * 开 发 人：yuexianchao
 * 开发时间：2024/7/2 15:34
 * 测 试 人：
 * 对接业务人：
 * 线上脚本：
 * 线下脚本：
 * 技术方案文档：
 * 版    本: 1.0
 * 更 新 人: 
 * 更新时间: 2024/7/2 15:34
"""
import sys
from collections import OrderedDict
import re
from datetime import datetime, timedelta
from util.logutil import logger

# 评论数据解析
from lxml import etree

def parse_sql_comment(self, comment, weibo):
    if not comment:
        return
    sql_comment = OrderedDict()
    sql_comment["id"] = comment["id"]

    _try_get_value("bid", "bid", sql_comment, comment)
    _try_get_value("root_id", "rootid", sql_comment, comment)
    _try_get_value("created_at", "created_at", sql_comment, comment)
    logger.info('处理前：' + sql_comment["created_at"])
    from datetime import datetime, timedelta
    try:
        if ('小时前' in sql_comment["created_at"]):
            hour_str = sql_comment["created_at"].split('小时前')[0]
            # 获取当前时间
            now = datetime.now()
            # 计算小时前的时间
            seven_hours_ago = now - timedelta(hours=int(hour_str))
            # 格式化时间为 '%Y-%m-%d %H:%M:%S' 格式
            sql_comment["created_at"] = seven_hours_ago.strftime('%Y-%m-%d %H:00:00')
        elif ('分钟前' in sql_comment["created_at"]):
            minute_str = sql_comment["created_at"].split('分钟前')[0]
            # 获取当前时间
            now = datetime.now()
            # 计算小时前的时间
            seven_hours_ago = now - timedelta(minutes=int(minute_str))
            # 格式化时间为 '%Y-%m-%d %H:%M:%S' 格式
            sql_comment["created_at"] = seven_hours_ago.strftime('%Y-%m-%d %H:%M:00')
        elif ('天前' in sql_comment["created_at"]):
            day_str = sql_comment["created_at"].split('天前')[0]
            # 获取当前时间
            now = datetime.now()
            # 计算小时前的时间
            seven_hours_ago = now - timedelta(days=int(day_str))
            # 格式化时间为 '%Y-%m-%d %H:%M:%S' 格式
            sql_comment["created_at"] = seven_hours_ago.strftime('%Y-%m-%d')
        elif (len(sql_comment["created_at"].split('-')) == 2):
            sql_comment["created_at"] = datetime.now().year.__str__() + '-' + sql_comment["created_at"]
        elif "昨天" in sql_comment["created_at"]:
            aa, sql_comment["created_at"] = standardize_date(sql_comment["created_at"])
        else:
            date_format = '%a %b %d %H:%M:%S %z %Y'
            date_object = datetime.strptime(sql_comment["created_at"], date_format)
            formatted_date = date_object.strftime('%Y-%m-%d %H:%M:%S')
            sql_comment["created_at"] = formatted_date
    except Exception as e:
        logger.error(e)
        logger.error(sql_comment["created_at"])
    logger.info('处理后：' + sql_comment["created_at"])

    sql_comment["weibo_id"] = weibo["id"]

    sql_comment["user_id"] = comment["user"]["id"]
    sql_comment["user_screen_name"] = comment["user"]["screen_name"]
    _try_get_value(
        "user_avatar_url", "avatar_hd", sql_comment, comment["user"]
    )
    if self.remove_html_tag:
        sql_comment["text"] = re.sub('<[^<]+?>', '', comment["text"]).replace('\n', '').strip()
    else:
        sql_comment["text"] = comment["text"]

    sql_comment["pic_url"] = ""
    if comment.get("pic"):
        sql_comment["pic_url"] = comment["pic"]["large"]["url"]
    _try_get_value("like_count", "like_count", sql_comment, comment)
    if sql_comment["like_count"] == '':
        sql_comment["like_count"] = 0
    return sql_comment


# 转发微博解析
def parse_sqlite_repost(self, repost, weibo):
    if not repost:
        return
    sqlite_repost = OrderedDict()
    sqlite_repost["id"] = repost["id"]

    _try_get_value("bid", "bid", sqlite_repost, repost)
    _try_get_value("created_at", "created_at", sqlite_repost, repost)

    sqlite_repost["weibo_id"] = weibo["id"]

    sqlite_repost["user_id"] = repost["user"]["id"]
    sqlite_repost["user_screen_name"] = repost["user"]["screen_name"]
    _try_get_value(
        "user_avatar_url", "profile_image_url", sqlite_repost, repost["user"]
    )
    text = repost.get("raw_text")
    if text:
        text = text.split("//", 1)[0]
    if text is None or text == "" or text == "Repost":
        text = "转发微博"
    sqlite_repost["text"] = text
    _try_get_value("like_count", "attitudes_count", sqlite_repost, repost)
    return sqlite_repost


# 微博数据解析
def parse_sqlite_weibo(weibo):
    if not weibo:
        return
    sqlite_weibo = OrderedDict()
    sqlite_weibo["user_id"] = weibo["user_id"]
    sqlite_weibo["id"] = weibo["id"]
    sqlite_weibo["bid"] = weibo["bid"]
    sqlite_weibo["screen_name"] = weibo["screen_name"]
    sqlite_weibo["text"] = weibo["text"]
    sqlite_weibo["article_url"] = weibo["article_url"]
    sqlite_weibo["topics"] = weibo["topics"]
    sqlite_weibo["pics"] = weibo["pics"]
    sqlite_weibo["video_url"] = weibo["video_url"]
    sqlite_weibo["location"] = weibo["location"]
    sqlite_weibo["created_at"] = weibo["full_created_at"]
    sqlite_weibo["source"] = weibo["source"]
    sqlite_weibo["attitudes_count"] = weibo["attitudes_count"]
    sqlite_weibo["comments_count"] = weibo["comments_count"]
    sqlite_weibo["reposts_count"] = weibo["reposts_count"]
    sqlite_weibo["retweet_id"] = weibo["retweet_id"]
    sqlite_weibo["at_users"] = weibo["at_users"]
    return sqlite_weibo


# user数据解析
def parse_sqlite_user(user):
    if not user:
        return
    sqlite_user = OrderedDict()
    sqlite_user["id"] = user["id"]
    sqlite_user["nick_name"] = user["screen_name"]
    sqlite_user["gender"] = user["gender"]
    sqlite_user["follower_count"] = user["followers_count"]
    sqlite_user["follow_count"] = user["follow_count"]
    sqlite_user["birthday"] = user["birthday"]
    sqlite_user["location"] = user["location"]
    sqlite_user["edu"] = user["education"]
    sqlite_user["company"] = user["company"]
    sqlite_user["reg_date"] = user["registration_time"]
    sqlite_user["main_page_url"] = user["profile_url"]
    sqlite_user["avatar_url"] = user["avatar_hd"]
    sqlite_user["bio"] = user["description"]
    return sqlite_user


def parse_weibo(self, weibo_info):
    weibo = OrderedDict()
    if weibo_info["user"]:
        weibo["user_id"] = weibo_info["user"]["id"]
        weibo["screen_name"] = weibo_info["user"]["screen_name"]
    else:
        weibo["user_id"] = ""
        weibo["screen_name"] = ""
    weibo["id"] = int(weibo_info["id"])
    if weibo['id'] == '5044764804907569':
        logger.info(f"{weibo_info['id']}")
    weibo["bid"] = weibo_info["bid"]
    text_body = weibo_info["text"]
    encoding = 'utf-8'
    # detected_encoding = chardet.detect(text_body)['encoding']
    # print(f"Detected encoding: {detected_encoding}")
    selector = etree.HTML(f"{text_body}<hr>" if text_body.isspace() else text_body)
    if self.remove_html_tag:
        text_list = selector.xpath("//text()")
        # 若text_list中的某个字符串元素以 @ 或 # 开始，则将该元素与前后元素合并为新元素，否则会带来没有必要的换行
        text_list_modified = []
        for ele in range(len(text_list)):
            if ele > 0 and (text_list[ele - 1].startswith(('@', '#')) or text_list[ele].startswith(('@', '#'))):
                text_list_modified[-1] += text_list[ele]
            else:
                text_list_modified.append(text_list[ele])
        weibo["text"] = "\n".join(text_list_modified)
    else:
        weibo["text"] = text_body

    logger.info("微博id  %s 微博内容：%s ", weibo["id"], weibo["text"])
    weibo["article_url"] = self.get_article_url(selector)
    weibo["pics"] = self.get_pics(weibo_info)
    weibo["video_url"] = self.get_video_url(weibo_info)
    weibo["location"] = self.get_location(selector)
    weibo["created_at"] = weibo_info["created_at"]
    weibo["source"] = weibo_info["source"]
    weibo["attitudes_count"] = self.string_to_int(
        weibo_info.get("attitudes_count", 0)
    )
    weibo["comments_count"] = self.string_to_int(
        weibo_info.get("comments_count", 0)
    )
    weibo["reposts_count"] = self.string_to_int(weibo_info.get("reposts_count", 0))
    weibo["topics"] = self.get_topics(selector)
    weibo["at_users"] = self.get_at_users(selector)
    weibo["text"] = remove_html_tags(weibo["text"])
    logger.info("去除html后微博内容：%s", weibo["text"])
    return standardize_info(weibo)


def print_user_info(self):
    """打印用户信息"""
    logger.info("+" * 100)
    logger.info("用户信息")
    logger.info("用户id：%s", self.user["id"])
    logger.info("用户昵称：%s", self.user["screen_name"])
    gender = "女" if self.user["gender"] == "f" else "男"
    logger.info("性别：%s", gender)
    logger.info("生日：%s", self.user["birthday"])
    logger.info("所在地：%s", self.user["location"])
    logger.info("教育经历：%s", self.user["education"])
    logger.info("公司：%s", self.user["company"])
    logger.info("阳光信用：%s", self.user["sunshine"])
    logger.info("注册时间：%s", self.user["registration_time"])
    logger.info("微博数：%d", self.user["statuses_count"])
    logger.info("粉丝数：%d", self.user["followers_count"])
    logger.info("关注数：%d", self.user["follow_count"])
    logger.info("url：https://m.weibo.cn/profile/%s", self.user["id"])
    if self.user.get("verified_reason"):
        logger.info(self.user["verified_reason"])
    logger.info(self.user["description"])
    logger.info("+" * 100)


def print_one_weibo(weibo):
    """打印一条微博"""
    try:
        logger.info("微博id：%d", weibo["id"])
        logger.info("微博正文：%s", weibo["text"])
        logger.info("原始图片url：%s", weibo["pics"])
        logger.info("微博位置：%s", weibo["location"])
        logger.info("发布时间：%s", weibo["created_at"])
        logger.info("发布工具：%s", weibo["source"])
        logger.info("点赞数：%d", weibo["attitudes_count"])
        logger.info("评论数：%d", weibo["comments_count"])
        logger.info("转发数：%d", weibo["reposts_count"])
        logger.info("话题：%s", weibo["topics"])
        logger.info("@用户：%s", weibo["at_users"])
        logger.info("url：https://m.weibo.cn/detail/%d", weibo["id"])
    except OSError:
        pass


def remove_html_tags(text):
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)


def _try_get_value(source_name, target_name, dict, json):
    dict[source_name] = ""
    value = json.get(target_name)
    if value:
        dict[source_name] = value

DTFORMAT = "%Y-%m-%dT%H:%M:%S"

def standardize_date(created_at):
    if "刚刚" in created_at:
        ts = datetime.now()
    elif "分钟" in created_at:
        minute = created_at[: created_at.find("分钟")]
        minute = timedelta(minutes=int(minute))
        ts = datetime.now() - minute
    elif "小时" in created_at:
        hour = created_at[: created_at.find("小时")]
        hour = timedelta(hours=int(hour))
        ts = datetime.now() - hour
    elif "昨天" in created_at:
        day = timedelta(days=1)
        ts = datetime.now() - day
    else:
        created_at = created_at.replace("+0800 ", "")
        ts = datetime.strptime(created_at, "%c")

    created_at = ts.strftime(DTFORMAT)
    full_created_at = ts.strftime("%Y-%m-%d %H:%M:%S")
    return created_at, full_created_at

"""标准化信息，去除乱码"""

def standardize_info(weibo):
    for k, v in weibo.items():
        if (
                "bool" not in str(type(v))
                and "int" not in str(type(v))
                and "list" not in str(type(v))
                and "long" not in str(type(v))
        ):
            weibo[k] = (
                v.replace("\u200b", "")
                .encode(sys.stdout.encoding, "ignore")
                .decode(sys.stdout.encoding)
            )
    return weibo
