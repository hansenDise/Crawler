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
		
		for imdblink in item['imdburl']:
			if imdblink.strip().__len__() <=0:
				print "*********** invalid item"
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
			logging.log(logging.INFO,"-- [category] categoryid="+str(categoryid))
		#movie
		imdburl = item['imdburl'][0].lower()
		movieid = 0
		erow = cursor.execute('select movieid from movie where imdburl="%s"'%imdburl)

		if erow > 1:
			raise DropItem("data mess %s",item)
		elif erow == 1:
			movieid = cursor.fetchone()[0]
		else:
			cursor.execute('insert into movie(categoryid,title,year,imdburl,posterurl,runtime,plot) values(%d,"%s",%d,"%s","%s",%d,"%s")'%(categoryid,item['movietitle'][0],int(item['year'][0]),item['imdburl'][0],item['posterurl'][0],int(item['runtime'][0]),item['plot'][0] ) )
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
			
		#screenshot
		for it in item['scrshoturl']:
			erow = cursor.execute('select * from screenshot where picurl = "%s"'%it)
			if erow == 0:
				cursor.execute('insert into screenshot(movieid ,picurl) values(%d,"%s")'%(movieid,it))
				conn.commit()
				
		#torrent
		erows = cursor.execute('select * from torrent where torrenturl="%s"'%item['torrenturl'][0])
		if erows == 0:
			cursor.execute('insert into torrent(movieid,name,torrenturl,magneturl,filesize,addedtime,seeds,downloadcount) values(%d,"%s","%s","%s","%s",now(),0,0)'%(movieid,item['torrentname'][0],item['torrenturl'][0], item['magneturl'][0], item['filesize'][0] ) )
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
					cursor.execute('insert into people(firstname_en,middlename_en,lastname_en,occupationid,borndate,summary) values("%s","%s","",%d,now(),"")'%(names[0],names[1],2))
					conn.commit()
					
					cursor.execute('select last_insert_id()')
					peopleid = cursor.fetchone()[0]

					cursor.execute('insert into movie_people(movieid,peopleid)values(%d,%d)'%(movieid,peopleid))
					conn.commit()
					
			elif names.__len__() == 3:
				ret = cursor.execute('select peopleid from people where firstname_en="%s" and middlename_en="%s" and lastname_en="%s"'%(names[0],names[1],names[2]))
				if ret == 0:
					cursor.execute('insert into people(firstname_en,middlename_en,lastname_en,occupationid,borndate,summary) values("%s","%s",%d,now(),"")'%(names[0],names[1],names[2],2))
					conn.commit()
					
					cursor.execute('select last_insert_id()')
					peopleid = cursor.fetchone()[0]
					
					cursor.execute('insert into movie_people(movieid,peopleid)values(%d,%d)'%(movieid,peopleid))
					conn.commit()

					
					

					
class MyFilePipeLine(FilesPipeline):
	
	def file_path(self, request, response=None, info=None):
		## start of deprecation warning block (can be removed in the future)
		def _warn():
			from scrapy.exceptions import ScrapyDeprecationWarning
			import warnings
			warnings.warn('FilesPipeline.file_key(url) method is deprecated, please use '
						'file_path(request, response=None, info=None) instead',
							category=ScrapyDeprecationWarning, stacklevel=1)

		# check if called from file_key with url as first argument
		if not isinstance(request, Request):
			_warn()
			url = request
		else:
			url = request.url

		# detect if file_key() method has been overridden
		if not hasattr(self.file_key, '_base'):
			_warn()
			return self.file_key(url)
		## end of deprecation warning block

		media_guid = hashlib.sha1(url).hexdigest()  # change to request.url after deprecation
		media_ext = os.path.splitext(url)[1]  # change to request.url after deprecation
		return 'full/%s%s' % (media_guid, media_ext)


