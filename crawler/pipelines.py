# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from crawler.items import CrawlerItem
import MySQLdb

class CrawlerPipeline(object):
	
	def __init__(self):
		self.conn = MySQLdb.connect(host='localhost',user='root',passwd='hansen',db='popu')
		self.cursor = conn.cursor()
	
    def process_item(self, item, spider):
        if not isinstance(item,CrawlerItem):
			return item
		else:
			if item['imdb_url'].strip().__len__() <=0:
				raise DropItem()
			
			#check category
			category = item['category'].replace('Movies/','')
			sql_str = 'select * from category where name="%s"'%(category)
			iret = self.cursor.execute(sql_str)
			if iret == 0:
				#insert into category
				sql_str = 'insert into category(name) values("%s")'%(category)
				self.cursor.execute(sql_str)
				self.conn.commit()
			
			#search whether already have resource
			sql_str = 'select * from resource where imdb_url="%"'%()item['imdb_url'])
			iret = self.cursor.execute(sql_str)
			if iret == 0:
				#insert resource