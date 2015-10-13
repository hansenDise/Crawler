# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from crawler.items import CrawlerItem
from scrapy.exceptions import DropItem

from scrapy.pipelines.files import FilesPipeline

import MySQLdb
import logging

class CrawlerPipeline(object):

	def process_item(self, item, spider):
		if not isinstance(item,CrawlerItem):
			raise DropItem("invalid item %s",item)
		
		print '---------------------------------------------------'
		print item.get('image_urls')
		print '----------------------------------------------------'
		print item.get('file_urls')
		print '----------------------------------------------------'
		
		#if import field missed ,then drop the item extracted
		if item['movietitle'].__len__() ==0 or item['torrenturl'].__len__() == 0 or item['imdburl'].__len__() == 0 :
			raise DropItem("invalid item %s",item)
		
		for imdblink in item['imdburl']:
			if imdblink.strip().__len__() <=0:
				raise DropItem("invalid item %s",item)
		
		conn = MySQLdb.connect(host='localhost',user='root',passwd='hansen',db='popu')
		cursor = conn.cursor()
		
		#category 
		category = item['category'][0].strip().replace('Movies/','').lower()
		erow = cursor.execute('select categoryid from category where categoryname="%s"'%category)
		categoryid = 0
		
		if erow == 0:
			cursor.execute('insert into category(categoryname) values ("%s")'%category)
			conn.commit()
			cursor.execute('select last_insert_id()')
			categoryid = cursor.fetchone()[0]
			
			logging.log(logging.INFO,"last_insert id="+str(categoryid))
		else:
			categoryid = cursor.fetchone()[0]

		#movie
		imdburl = item['imdburl'][0].lower()
		movieid = 0
		erow = cursor.execute('select movieid from movie where imdburl="%s"'%imdburl)

		if erow > 1:
			raise DropItem("data mess %s",item)
		elif erow == 1:
			movieid = cursor.fetchone()[0]
			
		else:
			
			print '++++++++++++++++++++++++insert into movie(categoryid,title,year,imdburl,posterurl,runtime,plot) values(%d,"%s",%d,"%s","%s",%d,"%s")'%(categoryid,item.get('movietitle','empty')[0],int(item.get('year','0')[0]),item.get('imdburl','empty')[0],item.get('posterurl','empty')[0],int(item.get('runtime','0')[0]),item.get('plot','empty')[0].replace('"',' '))
			
			cursor.execute('insert into movie(categoryid,title,year,imdburl,posterurl,runtime,plot) values(%d,"%s",%d,"%s","%s",%d,"%s")'%(categoryid,item.get('movietitle','empty')[0],int(item.get('year','0')[0]),item.get('imdburl','empty')[0],item.get('posterurl','empty')[0],int(item.get('runtime','0')[0]),item.get('plot','empty')[0].replace('"',' ')) )
			conn.commit()
			cursor.execute('select last_insert_id()')
			movieid = cursor.fetchone()[0]
		
		#genres
		genreslist = item['genres']
		[it.strip().lower() for it in genreslist]
		
		for it in genreslist:
			genresid = 0
			erow = cursor.execute('select genresid from genres where genresname="%s"'%it)
			if erow > 0:
				genresid = cursor.fetchone()[0]
			else:
				cursor.execute('insert into genres(genresname) values("%s")'%it)
				conn.commit()
				cursor.execute('select last_insert_id()')
				genresid = cursor.fetchone()[0]
			
			#movie_genres
			cursor.execute('insert into movie_genres(movieid,genresid) values(%d,%d)'%(movieid,genresid))
			conn.commit()
			

		#torrent
		torrentid = 0
		erows = cursor.execute('select torrentid from torrent where torrenturl="%s"'%item['torrenturl'][0])
		if erows == 0:
			cursor.execute('insert into torrent(movieid,name,torrenturl,magneturl,filesize,addedtime,seeds,downloadcount) values(%d,"%s","%s","%s","%s",now(),0,0)'%(movieid,item['torrentname'][0],item['torrenturl'][0], item['magneturl'][0], item['filesize'][0] ) )
			conn.commit()
			
			cursor.execute('select last_insert_id()')
			torrentid = cursor.fetchone()[0]
		else:
			torrentid = cursor.fetchone()[0]

		#screenshot
		for it in item['scrshoturl']:
			erow = cursor.execute('select * from screenshot where picurl = "%s"'%it)
			if erow == 0:
				cursor.execute('insert into screenshot(movieid ,picurl) values(%d,"%s")'%(torrentid,it))
				conn.commit()
							
		#movie_torrent
		cursor.execute('insert into movie_torrent(movieid,torrentid) values(%d,%d)'%(movieid,torrentid))
		conn.commit()
		
		#people
		for actor in item['actorname']:
			names = actor.split(' ')
			temp = []
			for na in names:
				if len(na.strip())>0:
					temp.append(na.strip().lower())
				else:
					continue
			names = temp
			
			if names.__len__() == 1:
				ret = cursor.execute('select peopleid from people where firstname_en="%s"'%names[0])
				if ret == 0:
					cursor.execute('insert into people(firstname_en,middlename_en,lastname_en,occupationid,borndate,summary) values("%s","","",%d,now(),"")'%(names[0],2))
					conn.commit()
					
					cursor.execute('select last_insert_id()')
					peopleid = cursor.fetchone()[0]
					
					cursor.execute('insert into movie_people(movieid,peopleid)values(%d,%d)'%(movieid,peopleid))
					conn.commit()
					
			elif names.__len__() == 2:
				ret = cursor.execute('select peopleid from people where firstname_en="%s" and lastname_en="%s"'%(names[0],names[1]))
				if ret == 0:
					cursor.execute('insert into people(firstname_en,middlename_en,lastname_en,occupationid,borndate,summary) values("%s","","%s",%d,now(),"")'%(names[0],names[1],2))
					conn.commit()
					
					cursor.execute('select last_insert_id()')
					peopleid = cursor.fetchone()[0]

					cursor.execute('insert into movie_people(movieid,peopleid)values(%d,%d)'%(movieid,peopleid))
					conn.commit()
					
			elif names.__len__() == 3:
				ret = cursor.execute('select peopleid from people where firstname_en="%s" and middlename_en="%s" and lastname_en="%s"'%(names[0],names[1],names[2]))
				if ret == 0:
					cursor.execute('insert into people(firstname_en,middlename_en,lastname_en,occupationid,borndate,summary) values("%s","%s","%s",%d,now(),"")'%(names[0],names[1],names[2],2))
					conn.commit()
					
					cursor.execute('select last_insert_id()')
					peopleid = cursor.fetchone()[0]
					
					cursor.execute('insert into movie_people(movieid,peopleid)values(%d,%d)'%(movieid,peopleid))
					conn.commit()
		
		#close database connection
		conn.close()