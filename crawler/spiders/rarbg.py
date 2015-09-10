# -*- coding: utf-8 -*-
import scrapy
from scrapy.shell import inspect_response
from scrapy.loader import ItemLoader
from crawler.items import CrawlerItem

class RarbgSpider(scrapy.Spider):
	name="rarbgmovie"
	allowed_domains = ["rarbg.to"]
	url_prefix = "https://rarbg.to"
	start_urls = ()
	totalpage = 2	#total pages number
	
	
	def start_requests(self):
		for i in range(1,self.totalpage):
			url = "https://rarbg.to/torrents.php?category=14%3B17%3B42%3B44%3B45%3B46%3B47%3B48&page=" + str(i)
			yield scrapy.Request(url,self.parse)
			
			
	def parse(self,response):
		movielist = response.xpath('//tr[@class="lista2"]/td[2]/a[1]/@href').extract()
		
		#for item in movielist:
		#	url = self.url_prefix + item
		#	yield scrapy.Request(url,callback=self.moviepase)
		url = self.url_prefix + movielist[0]
		return scrapy.Request(url,callback=self.moviepase)
		
	def moviepase(self,response):
		#inspect_response(response,self)
		trlist = response.xpath('//table[@class="lista-rounded"]/tr[2]/td/div/table/tr')
		
		itemloader = ItemLoader(item=CrawlerItem())
		
		#torrent name
		torrentname = trlist[0].xpath('./td[2]/a[1]/text()').extract()
		itemloader.add_value('torrentname',torrentname)
		
		#torrent url
		torrenturl = trlist[0].xpath('./td[2]/a[1]/@href').extract()
		torrenturl = self.url_prefix + str(torrenturl)
		itemloader.add_value('torrenturl',torrenturl)
		
		#magnet url
		magneturl = trlist[0].xpath('./td[2]/a[2]/@href').extract()
		itemloader.add_value('magneturl',magneturl)
		
		#poster url 
		posterurl = trlist[2].xpath('./td[2]/img/@src').extract()
		itemloader.add_value('posterurl',posterurl)
		
		#screen shot picture url
		screenshoturl = trlist[4].xpath('./td[2]/a/img/@src').extract()
		itemloader.add_value('scrshoturl',screenshoturl)
		
		#trailer url
		#trailerurl = trlist[5].xpath('./td[2]/a/@href').extract()
		#itemloader.add_value('trailerurl',trailerurl)
		
		#imdb url
		imdburl = trlist[6].xpath('./td[2]/a/@href').extract()
		itemloader.add_value('imdburl',imdburl)
		
		#category 
		category = trlist[8].xpath('./td[2]/a/text()').extract()
		itemloader.add_value('category',category)
		
		#file size
		filesize = trlist[9].xpath('./td[2]/text()').extract()
		itemloader.add_value('filesize',filesize)
		
		#movie title
		movietitle = trlist[12].xpath('./td[2]/span/text()').extract()
		itemloader.add_value('movietitle',movietitle)
		
		#genres
		genres = trlist[14].xpath('./td[2]/span/a/text()').extract()
		itemloader.add_value('genres',genres)
		
		#actors
		peoplename = trlist[15].xpath('./td[2]/span/a/text()').extract()
		itemloader.add_value('peoplename',peoplename)
		
		#director
		director = trlist[16].xpath('./td[2]/span/a/text()').extract()
		itemloader.add_value('peoplename',director)
		
		#runtime
		runtime = trlist[17].xpath('./td[2]/text()').extract()
		itemloader.add_value('runtime',runtime)
		
		#year
		year = trlist[18].xpath('./td[2]/text()').extract()
		itemloader.add_value('year',year)
		
		#plot
		plot = trlist[19].xpath('./td[2]/span/text()').extract()
		itemloader.add_value('plot',plot)
		
		return itemloader.load_item()
		
		
	