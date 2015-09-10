# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlerItem(scrapy.Item):
	category = scrapy.Field()
	peoplename = scrapy.Field()
	genres = scrapy.Field()
	movietitle = scrapy.Field()
	year = scrapy.Field()
	posterurl = scrapy.Field()
	runtime =scrapy.Field()
	plot = scrapy.Field()
	imdburl = scrapy.Field()
	torrentname = scrapy.Field()
	torrenturl = scrapy.Field()
	magneturl = scrapy.Field()
	filesize = scrapy.Field()
	scrshoturl = scrapy.Field()
	trailerurl = scrapy.Field()
	subtitleurl = scrapy.Field()
	
