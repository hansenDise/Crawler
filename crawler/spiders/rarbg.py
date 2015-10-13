# -*- coding: utf-8 -*-
import scrapy
from scrapy.shell import inspect_response
from scrapy.loader import ItemLoader
from crawler.items import CrawlerItem

class RarbgSpider(scrapy.Spider):
	name="rarbgmovie"
	#allowed_domains = ["*"]
	url_prefix = "https://rarbg.to"
	start_urls = (
	"https://rarbg.to/torrents.php?category=movies",)
	
	def parse(self,response):
		if response.status != 200:
			closed("catched!")
			
		urllist = response.xpath('//tr[@class="lista2"]/td[2]/a[1]/@href').extract()
		
		[self.url_prefix + item for item in urllist]
		
		for url in urllist:
			url = self.url_prefix + url
			yield scrapy.Request(url=url,callback=self.moiveparse,priority=-5)  #Negative values are allowed in order to indicate relatively low-priority.
			#yield scrapy.Request(url=url,callback=self.moiveparse)

#	def moviecateparse(self,response):
#		if response.status != 200:
#			closed("catched!")
#
#		urllist = response.xpath('//tr[@class="lista2"]/td[2]/a[1]/@href').extract()
#		
#		[self.url_prefix + item for item in urllist]
#		
#		for url in urllist:
#			url = self.url_prefix + url
#			yield scrapy.Request(url=url,callback=self.moiveparse,priority=1)

	
	def moiveparse(self,response):
		if response.status != 200:
			closed("catched!")
		
		trlist = response.xpath('//table[@class="lista-rounded"]/tr[2]/td/div/table/tr')
		
		item = self.extractData(trlist)
		return item
	
	def extractData(self,trlist):
		itemloader = ItemLoader(item=CrawlerItem())
		
		for tr in trlist:
			#imdb url
			imdburl = tr.xpath('./td[2]/a[contains(@href,"http://imdb.com")]/@href').extract()
			if imdburl:
				itemloader.add_value('imdburl',imdburl)

			
			trhead = tr.xpath('./td[1]/text()').extract()
			
			for row in trhead:
				strrow = row.strip().replace(':','').lower()
				strrow = strrow.strip()
				
				if strrow == u'torrent':
					#torrent name
					torrentname = tr.xpath('./td[2]/a[1]/text()').extract()[0].replace('-RARBG','')
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

				elif strrow == u'poster':
					#poster url 
					posterurl = tr.xpath('./td[2]/img/@src').extract()
					
					for purl in posterurl:
						tempurl = 'http:' + purl
						itemloader.add_value('posterurl',tempurl)

						#images
						itemloader.add_value('image_urls',tempurl)
				elif strrow == u'description':
					#screen shot picture url
					screenshoturl = tr.xpath('./td[2]/a/img/@src').extract()
					itemloader.add_value('scrshoturl',screenshoturl)
					
					# images 
					for it in screenshoturl:
						itemloader.add_value('image_urls',it)
					
				elif strrow == u'category':
					#category 
					category = tr.xpath('./td[2]/a/text()').extract()
					itemloader.add_value('category',category)
					
				elif strrow == u'size':
					#file size
					filesize = tr.xpath('./td[2]/text()').extract()
					itemloader.add_value('filesize',filesize)
					
				elif strrow == u'title':
					#movie title
					movietitle = tr.xpath('./td[2]/span/text()').extract()
					itemloader.add_value('movietitle',movietitle)
					
				elif strrow == u'genres':
					#genres
					genres = tr.xpath('./td[2]/span/a/text()').extract()
					itemloader.add_value('genres',genres)
					
				elif strrow == u'actors':
					#actors
					actors = tr.xpath('./td[2]/span/a/text()').extract()
					itemloader.add_value('actorname',actors)
					
				elif strrow == u'director':
					#director
					director = tr.xpath('./td[2]/span/a/text()').extract()
					itemloader.add_value('diectorname',director)
					
				elif strrow == u'runtime':
					#runtime
					runtime = tr.xpath('./td[2]/text()').extract()
					itemloader.add_value('runtime',runtime)
					
				elif strrow == u'year':
					#year
					year = tr.xpath('./td[2]/text()').extract()
					itemloader.add_value('year',year)
					
				elif strrow == u'plot':
					#plot
					plot = tr.xpath('./td[2]/span/text()').extract()
					itemloader.add_value('plot',plot)
					
		return itemloader.load_item()