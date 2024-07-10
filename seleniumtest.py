#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""测试selenium框架
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# 初始化浏览器驱动，这里以Chrome为例
# '/path/to/chromedriver'
driver = webdriver.Chrome()  # 替换为你的ChromeDriver路径

# 访问网站
driver.get('https://www.xiaohongshu.com/explore/665d4d3e0000000005004ae0?source=yamcha_homepage')  # 替换为你要爬取的网站URL

# 等待页面加载完成（可选，但推荐）
wait = WebDriverWait(driver, 10)  # 设置最长等待时间为10秒

# 查找页面元素（这里以ID为'example-id'的元素为例）
element = wait.until(EC.presence_of_element_located((By.ID, 'example-id')))

# 获取元素内容（这里假设是文本内容）
content = element.text
print(content)

# 关闭浏览器（可选，但在结束时应该关闭以避免资源泄漏）
driver.quit()
