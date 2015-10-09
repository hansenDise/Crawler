﻿# -*- coding: utf-8 -*-
import scrapy
from scrapy.shell import inspect_response
from scrapy.loader import ItemLoader
from crawler.items import CrawlerItem

class RarbgSpider(scrapy.Spider):
	name="rarbgmovie"
	allowed_domains = ["rarbg.to"]
	url_prefix = "https://rarbg.to"
	start_urls = (
	"https://rarbg.to/torrents.php?category=movies",)
	
	def parse(self,response):
		urllist = response.xpath('//tr[@class="lista2"]/td[2]/a[1]/@href').extract()
		
		[self.url_prefix + item for item in urllist]
		
		for url in urllist[1:2]:
			url = self.url_prefix + url
			#yield scrapy.Request(url=url,callback=self.moiveparse,priority=1)
			yield scrapy.Request(url=url,callback=self.moiveparse)

	def moviecateparse(self,response):
		urllist = response.xpath('//tr[@class="lista2"]/td[2]/a[1]/@href').extract()
		
		[self.url_prefix + item for item in urllist]
		
		for url in urllist:
			url = self.url_prefix + url
			yield scrapy.Request(url=url,callback=self.moiveparse,priority=1)

	
	def moiveparse(self,response):
		trlist = response.xpath('//table[@class="lista-rounded"]/tr[2]/td/div/table/tr')
		
		item = self.extractData(trlist)
		print item
		return item
	
	def extractData(self,trlist):
		itemloader = ItemLoader(item=CrawlerItem())
		
		extractIndex = []
		index = -1
		for tr in trlist:
			#imdb url
			imdburl = tr.xpath('./td[2]/a[contains(@href,"http://imdb.com")]/@href').extract()
			if imdburl:
				itemloader.add_value('imdburl',imdburl)

			index = index + 1
			trhead = tr.xpath('./td[1]/text()').extract()
			
			for row in trhead:
				strrow = row.strip().replace(':','').lower()
				strrow = strrow.strip()
				
				if strrow == u'torrent':
					#torrent name
					torrentname = tr.xpath('./td[2]/a[1]/text()').extract().replace('-RARBG','')
					itemloader.add_value('torrentname',torrentname)
					
					#torrent url
					torrenturl = tr.xpath('./td[2]/a[1]/@href').extract()
					if torrenturl:
						turl = self.url_prefix + torrenturl[0]
						print turl
						itemloader.add_value('torrenturl',turl.replace('[','%5B').replace(']','%5D'))
					
						#torrent files
						itemloader.add_value('file_urls',turl)
					
					#magnet url
					magneturl = tr.xpath('./td[2]/a[2]/@href').extract()
					if magneturl:
						itemloader.add_value('magneturl',magneturl[0])
					
					extractIndex.append(index)
				elif strrow == u'poster':
					#poster url 
					posterurl = tr.xpath('./td[2]/img/@src').extract()
					
					for purl in posterurl:
						tempurl = 'http:' + purl
						itemloader.add_value('posterurl',tempurl)

						#images
						itemloader.add_value('image_urls',tempurl)
						
					extractIndex.append(index)
				elif strrow == u'description':
					#screen shot picture url
					screenshoturl = tr.xpath('./td[2]/a/img/@src').extract()
					itemloader.add_value('scrshoturl',screenshoturl)
					extractIndex.append(index)
					
					# images 
					for it in screenshoturl:
						itemloader.add_value('image_urls',it)
					
				elif strrow == u'category':
					#category 
					category = tr.xpath('./td[2]/a/text()').extract()
					itemloader.add_value('category',category)
					extractIndex.append(index)
				elif strrow == u'size':
					#file size
					filesize = tr.xpath('./td[2]/text()').extract()
					itemloader.add_value('filesize',filesize)
					extractIndex.append(index)
				elif strrow == u'title':
					#movie title
					movietitle = tr.xpath('./td[2]/span/text()').extract()
					itemloader.add_value('movietitle',movietitle)
					extractIndex.append(index)
				elif strrow == u'genres':
					#genres
					genres = tr.xpath('./td[2]/span/a/text()').extract()
					itemloader.add_value('genres',genres)
					extractIndex.append(index)
				elif strrow == u'actors':
					#actors
					actors = tr.xpath('./td[2]/span/a/text()').extract()
					itemloader.add_value('actorname',actors)
					extractIndex.append(index)
				elif strrow == u'director':
					#director
					director = tr.xpath('./td[2]/span/a/text()').extract()
					itemloader.add_value('diectorname',director)
					extractIndex.append(index)
				elif strrow == u'runtime':
					#runtime
					runtime = tr.xpath('./td[2]/text()').extract()
					itemloader.add_value('runtime',runtime)
					extractIndex.append(index)
				elif strrow == u'year':
					#year
					year = tr.xpath('./td[2]/text()').extract()
					itemloader.add_value('year',year)
					extractIndex.append(index)
				elif strrow == u'plot':
					#plot
					plot = tr.xpath('./td[2]/span/text()').extract()
					itemloader.add_value('plot',plot)
					extractIndex.append(index)
		return itemloader.load_item()