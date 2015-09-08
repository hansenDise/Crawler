# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlerItem(scrapy.Item):
	resouceType = scrapy.Field()
	title = scrapy.Field()
	release_year = scrapy.Field()
	runtime = scrapy.Field()
	year = scrapy.Field()
	poster_url = scrapy.Field()
	screenshot_url = scrapy.Field()
	imdb_url = scrapy.Field()
	category = scrapy.Field()
	actors = scrapy.Field()
	directors = scrapy.Field()
	torrent_url  = scrapy.Field()
	magnet_url = scrapy.Field()
	filesize = scrapy.Field()
	genres = scrapy.Field()
	
