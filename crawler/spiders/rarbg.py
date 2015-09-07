# -*- coding: utf-8 -*-
import scrapy
from scrapy.shell import inspect_response


class RarbgSpider(scrapy.Spider):
	name="rarbgmovie"
	allowed_domains = ["rarbg.to"]
	url_prefix = "https://rarbg.to"
	start_urls = ()
	totalpage = 10	#total pages number
	
	
	def start_requests(self):
		for i in range(1,self.totalpage):
			url = "https://rarbg.to/torrents.php?category=14%3B17%3B42%3B44%3B45%3B46%3B47%3B48&page=" + str(i)
			yield scrapy.Request(url,self.parse)
			
			
	def parse(self,response):
		pass
		
	
	def moviepase(self,response):
		pass
		
	