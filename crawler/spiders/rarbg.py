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
		#inspect_response(response,self)
		movielist = response.xpath('//tr[@class="lista2"]/td[2]/a[1]/@href').extract()
		
		for item in movielist:
			url = self.url_prefix + item
			yield scrapy.Request(url,callback=self.moviepase)
	
	def moviepase(self,response):
		inspect_response(response,self)
		
		itemloader = ItemLoader(item=CrawlerItem())
		#获取包含真个电影的 table
		movie_table = response.xpath('//table[@class="lista-rounded"]/tr[2]/td/div//table')
		
		#get poster url
		poster_url = movie_table.xpath('./tr[3]/td[2]/img/@src').extract()
		itemloader.add_value('poster_url',poster_url)
		
		#get screen shot image url
		shot_urls = movie_table.xpath('./tr[5]/td[2]/a/img/@src').extract()
		itemloader.add_value('screenshot_url',shot_urls)
		
		#get imdb url
		imdb_url = movie_table.xpath('/tr[6]/td[2]/a/@href').extract()
		itemloader.add_value('imdb_url',imdb_url)
		
		#get category
		category = movie_table.xpath('./tr[8]/td[2]/a/text()').extract()
		itemloader.add_value('category',category)
		
		#get file size
		filesize = movie_table.xpath('./tr[9]/td[2]/text()').extract()
		itemloader.add_value('filesize',filesize)
		
		#get title
		title = movie_table.xpath('./tr[12]/td[2]/span/text()').extract()
		itemloader.add_value('title',title)
		
		#get genres
		genres = movie_table.xpath('./tr[13]/td[2]/span/a/text()').extract()
		itemloader.add_value('genres',genres)
		
		#get year
		year = moive_table.xpath('./tr[15]/td[2]/text()').extract()
		itemloader.add_value('year',year)
		
		yield itemloader.load_item()
		
		
	