# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from crawler.items import CrawlerItem
from scrapy.exceptions import DropItem
import MySQLdb
import logging

class CrawlerPipeline(object):

	def process_item(self, item, spider):
		print '********************** in process_item'
		if not isinstance(item,CrawlerItem):
			print " *********** is not a instant of CrawlerItem"
			raise DropItem("invalid item %s",item)
		
		for imdblink in item['imdburl']:
			if imdblink.strip().__len__() <=0:
				print "*********** invalid item"
				raise DropItem("invalid item %s",item)
		
		conn = MySQLdb.connect(host='localhost',user='root',passwd='hansen',db='popu')
		cursor = conn.cursor()
		
		#category 
		category = item['category'][0].strip().replace('Movies/','').lower()
		print '+++++++++++++++select categoryid from category where categoryname="%s"'%category
		erow = cursor.execute('select categoryid from category where categoryname="%s"'%category)
		categoryid = 0
		
		if erow == 0:
			print "*********** [category] erow=0 , category=" + category
			
			print '+++++++++++++++insert into category(categoryname) values ("%s")'%category
			
			cursor.execute('insert into category(categoryname) values ("%s")'%category)
			conn.commit()
			cursor.execute('select last_insert_id()')
			categoryid = cursor.fetchone()[0]
			
			logging.log(logging.INFO,"last_insert id="+str(categoryid))
		else:
			categoryid = cursor.fetchone()[0]
			logging.log(logging.INFO,"-- [category] categoryid="+str(categoryid))
		#movie
		imdburl = item['imdburl'][0].lower()
		movieid = 0
		print '+++++++++++++++select movieid from movie where imdburl="%s"'%imdburl
		erow = cursor.execute('select movieid from movie where imdburl="%s"'%imdburl)
		if erow > 1:
			raise DropItem("data mess %s",item)
		elif erow == 1:
			movieid = cursor.fetchone()[0]
		else:
			print '+++++++++++++++insert into movie(categoryid,title,year,imdburl,posterurl,runtime,plot) values(%d,"%s",%d,"%s","%s",%d,"%s")'%(categoryid,item['movietitle'],int(item['year'][0]),item['imdburl'],item['posterurl'],int(item['runtime'][0]),item['plot'] )
			cursor.execute('insert into movie(categoryid,title,year,imdburl,posterurl,runtime,plot) values(%d,"%s",%d,"%s","%s",%d,"%s")'%(categoryid,item['movietitle'][0],int(item['year'][0]),item['imdburl'][0],item['posterurl'][0],int(item['runtime'][0]),item['plot'][0] ) )
			conn.commit()
			cursor.execute('select last_insert_id()')
			movieid = cursor.fetchone()[0]
		
		#genres
		genreslist = item['genres']
		[it.strip().lower() for it in genreslist]
		
		for it in genreslist:
			genresid = 0
			print '+++++++++++++++select genresid from genres where genresname="%s"'%it
			erow = cursor.execute('select genresid from genres where genresname="%s"'%it)
			if erow > 0:
				genresid = cursor.fetchone()[0]
			else:
				print '+++++++++++++++++insert into genres(genresname) values("%s")'%it 
				cursor.execute('insert into genres(genresname) values("%s")'%it)
				conn.commit()
				cursor.execute('select last_insert_id()')
				genresid = cursor.fetchone()[0]
			
			#movie_genres
			print '+++++++++++++++insert into movie_genres(movieid,genresid) values(%d,%d)'%(movieid,genresid)
			cursor.execute('insert into movie_genres(movieid,genresid) values(%d,%d)'%(movieid,genresid))
			conn.commit()
			
		#screenshot
		for it in item['scrshoturl']:
			print '+++++++++++++++select * from screenshot where picurl = "%s"'%it
			erow = cursor.execute('select * from screenshot where picurl = "%s"'%it)
			if erow == 0:
				cursor.execute('insert into screenshot(movieid ,picurl) values(%d,"%s")'%(movieid,it))
				conn.commit()
				
		#torrent
		print '+++++++++++++++select * from torrent where torrenturl="%s"'%item['torrenturl']
		erows = cursor.execute('select * from torrent where torrenturl="%s"'%item['torrenturl'][0])
		if erows == 0:
			print '+++++++++++++++insert into torrent(movieid,torrentname,torrenturl,magneturl,filesize,addtime) values(%d,"%s","%s","%s","%s",now())'%(movieid,item['torrentname'],item['torrenturl'], item['magneturl'], item['filesize'] )
			cursor.execute('insert into torrent(movieid,name,torrenturl,magneturl,filesize,addedtime) values(%d,"%s","%s","%s","%s",now())'%(movieid,item['torrentname'][0],item['torrenturl'][0], item['magneturl'][0], item['filesize'][0] ) )
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
				print '++++++++++++++++select peopleid from people where firstname_en="%s"'%names[0]
				ret = cursor.execute('select peopleid from people where firstname_en="%s"'%names[0])
				if ret == 0:
					print '++++++++++++++++++insert into people(firstname_en) values("%s")'%names[0]
					cursor.execute('insert into people(firstname_en,occupationid) values("%s",%d)'%(names[0],2))
					conn.commit()
					
					cursor.execute('select last_insert_id()')
					peopleid = cursor.fetchone()[0]
					
					print '++++++++++++++++++++++insert into movie_people(movieid,peopleid)values(%d,%d)'%(movieid,peopleid)
					
					cursor.execute('insert into movie_people(movieid,peopleid)values(%d,%d)'%(movieid,peopleid))
					conn.commit()
					
			elif names.__len__() == 2:
				print '+++++++++++++++++++++++++select peopleid from people where firstname_en="%s" and lastname_en="%s"'%(names[0],names[1])
				ret = cursor.execute('select peopleid from people where firstname_en="%s" and lastname_en="%s"'%(names[0],names[1]))
				if ret == 0:
					print '++++++++++++++++++++++++++++++++insert into people(firstname_en,lastname_en) values("%s","%s")'%(names[0],names[1])
					cursor.execute('insert into people(firstname_en,lastname_en,occupationid) values("%s","%s",%d)'%(names[0],names[1],2))
					conn.commit()
					
					cursor.execute('select last_insert_id()')
					peopleid = cursor.fetchone()[0]
					
					print '++++++++++++++++++++++++++++insert into movie_people(movieid,peopleid)values(%d,%d)'%(movieid,peopleid)
					
					cursor.execute('insert into movie_people(movieid,peopleid)values(%d,%d)'%(movieid,peopleid))
					conn.commit()
					
			elif names.__len__() == 3:
				print '++++++++++++++++++select peopleid from people where firstname_en="%s" and middlename_en="%s" and lastname_en="%s"'%(names[0],names[1],names[2])
				ret = cursor.execute('select peopleid from people where firstname_en="%s" and middlename_en="%s" and lastname_en="%s"'%(names[0],names[1],names[2]))
				if ret == 0:
					print '++++++++++++insert into people(firstname_en,middlename_en,lastname_en) values("%s","%s")'%(names[0],names[1],names[2])
					cursor.execute('insert into people(firstname_en,middlename_en,lastname_en,occupationid) values("%s","%s",%d)'%(names[0],names[1],names[2],2))
					conn.commit()
					
					cursor.execute('select last_insert_id()')
					peopleid = cursor.fetchone()[0]
					
					cursor.execute('insert into movie_people(movieid,peopleid)values(%d,%d)'%(movieid,peopleid))
					conn.commit()