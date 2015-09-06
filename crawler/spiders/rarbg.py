# -*- coding: utf-8 -*-
import scrapy
from scrapy.shell import inspect_response


class RarbgSpider(scrapy.Spider):
	name="rarbg"
	allowed_domains = ["rarbg.to"]
	url_prefix = "https://rarbg.to"
	start_urls = (
	"https://rarbg.to/torrents.php?page=1",)
	
	def parse(self,response):
		inspect_response(response,self)