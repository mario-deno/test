from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from time import sleep
import csv
from random import *

driver = webdriver.Chrome('/home/dap/chromedriver')
driver.get('https://google.com')

#searches=['site:it.linkedin.com/in AND "big data" AND "Milan Area"','site:it.linkedin.com/in AND "hadoop" AND "Milan Area"']

searches=['site:it.linkedin.com/in AND "hadoop" AND "Milan Area"']


for y in range(0, len(searches)):

	search_query = driver.find_element_by_name('q')
	search_query.send_keys(searches[y])
	search_query.send_keys(Keys.RETURN)
	output = []

	x = input('how many pages?')

	for x in range(1, int(x)):
		web_element_urls = driver.find_elements_by_class_name('LC20lb')
		output = output + [url.text.encode('utf-8') for url in web_element_urls]
		driver.find_element_by_id('pnnext').click()
		sleep(randint(11,17))



	with open(str(y)+'_output.txt', 'w') as f:
		for item in output:
			f.write("%s\n" % item)


	search_query = driver.find_element_by_name('q')
	search_query.clear()
	sleep(5)


driver.quit()
