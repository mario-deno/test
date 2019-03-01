entrare con il driver di test



modalita tablet
andare su linkedin e fare la ricerca
caricare tutte le persone
lanciare questo script


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from time import sleep
import csv
from random import *
driver = webdriver.Chrome('/home/dap/chromedriver')
list = driver.find_elements_by_css_selector('div.search-result__info.pt3.pb4.pr4')
with open('linkedin_machine_learning.txt', 'w') as f:      
 for item in list:
  f.write("%s\n" % item.text.replace('\n','::').encode('utf-8'))
