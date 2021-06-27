import requests
import shutil
import argparse
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import time
from selenium.webdriver.support.select import Select
import datetime
import urllib.request 
import configparser
import traceback
import logging
import sys
import subprocess
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d \
:: %(message)s', level = logging.INFO)

config = configparser.ConfigParser()
config.read('config.cfg')

DEBUG = False
chromeDriver_Path="chromedriver.exe"
year_codes={}
class ParkingOccupancyIngest:
    """
    This class performs transformation operations on the dataset.
    Transform timestamp format, clean text part, remove extra spaces etc
    """        
    def __init__(self):
        
        self.historic_years = config['PAID_PARKING']['historic_years']
        self.recent_years = config['PAID_PARKING']['recent_years']
        self.seattle_open_data_url = config['URLS']['seattle_open_data_url']
        self.search_dataByYear = config['XPATH']['search_dataByYear']
        self.parking_Occpn_Option = config['XPATH']['parking_Occpn_Option']
        self.parking_Occpn_Option_ByYear=config['XPATH']['parking_Occpn_Option_ByYear']
        self.url1 = config['URLS']['url1']
        self.url2 = config['URLS']['url2']
        self.arch_url1 = config['URLS']['arch_url1']
        self.arch_url2 = config['URLS']['arch_url2']
        self.blocface_url = config['URLS']['blockface_url']

        # Set environment variables
        os.environ['AZURE_TENANT_ID'] = config['KEY_VAULT']['azure_tenant_id']
        os.environ['AZURE_CLIENT_ID'] = config['KEY_VAULT']['azure_client_id']
        os.environ['AZURE_CLIENT_SECRET'] = config['KEY_VAULT']['azure_client_secret']

        keyVaultName = config['KEY_VAULT']['key_vault_name']
        KVUri = f"https://{keyVaultName}.vault.azure.net"

        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=KVUri, credential=credential)

        qs_user_name = client.get_secret('fileshareusername').value

        qs_pass = client.get_secret('filesharepassword').value

        fileshare_mount=r'net use Z: \\seattledatalake16.file.core.windows.net\qsfileshare {} {}'.format(qs_user_name,qs_pass)


        subprocess.call(r'net use * /del', shell=True)
        # Connect to shared drive, use drive letter M
        subprocess.call(fileshare_mount, shell=True) 


    # function to take care of downloading file
    def get_parkingOccupancy_file_code(self, year):
        """
        This function gets the dynamic parking occupancy file code as per year

        Used Selenium Python Library and Headless chrome to perform series of operations on Seattle Open Data UI to get the code

        Parameters:
                year (Integer): Year

        Returns:
                Code: The alpha-numeric file code for that patricular year

        """

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--verbose')

        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-software-rasterizer')

        print('Year is ',year)

        # initialize driver object and change the <path_to_chrome_driver> depending on your directory where your chromedriver should be
        driver = webdriver.Chrome(chrome_options=chrome_options, executable_path=chromeDriver_Path)

        # get request to target the site selenium is active on
        driver.get(self.seattle_open_data_url)
        time.sleep(2)

        # Enter the search '{Year} Paid Parking' in the search bar 
        search_data = driver.find_element_by_xpath(self.search_dataByYear)
        if year !=current_year:
            search_data.send_keys("{} Paid Parking".format(year))
        else:
            search_data.send_keys("Paid Parking Last 30 days")
        time.sleep(4)

        # Click on the search '{Year} Paid Parking' in the dropdown 
        print(driver.find_element_by_xpath(self.parking_Occpn_Option).text)
        driver.find_element_by_xpath(self.parking_Occpn_Option).click()

        time.sleep(10)

        # Get the URL of Parking Occupancy Data by Year
        url=driver.find_element_by_xpath(self.parking_Occpn_Option_ByYear).get_attribute("href")
        global url_type, file_extn

        urls=url.split("/")

        if "Archive" in url:
            url_type ="Archive"
            file_extn =".zip" 
        else:
            url_type = "Latest"
            file_extn = ".csv"
        
        code=urls[5]

        print('Code is {}'.format(code))

        return code

    # This function returns a concatenated string: base_url+year.csv"
    def _get_parkingOcc_url_by_year(self, code):
        """
        This function gets the Parking Occupancy URL as per the code

        Parameters:
                code (String): File code as per year

        Returns:
                URL (String): URL for downloading the Seattle Parking Occupancy data (Year to Date)

        """
        # Retrieves the correct URL depending on whether the file is archived or latest (i.e. <= 2 yrs old)
        if url_type=="Latest":
            return "{}{}{}".format(self.url1,code,self.url2)
        else:
            return "{}{}{}".format(self.arch_url1,code,self.arch_url2)



    # Retrieves data from list of urls
    def _process_url(self, url, filename):
        """
        This function downloads the Parking Occupancy Data as per year

        """

        logging.info("Starting URL processing for {} and {}".format(url,filename))
        
        r = requests.get(url, verify=False,stream=True)
        if r.status_code!=200:
            logging.error('Error occured for URL {}'.format(url))
            exit()
        else:
            r.raw.decode_content = True
            with open(filename, 'wb') as f:
                #shutil.unpack_archive(filename, "Z:\\")
                shutil.copyfileobj(r.raw, f)
            logging.info("{} downloaded successfully".format(filename))
    



    def sync_files(self, year,code):
        url = self._get_parkingOcc_url_by_year(code)
        file="Z:\{}_Paid_Parking{}".format(year,file_extn)
        self._process_url(url, file)
    

    def sync_sec_file(self, url):
        file="Z:\BlockFace.csv"
        self._process_url(url, file)
    

    def _line_separator():
        return "=" * 50


    def _banner():
        return "%s\nWhole Data Overview\n%s" % ((_line_separator(),) * 2)


    def _greeting(self, day, month, year):
        return f"Starting program for {month}-{year}-{day}\n" + _line_separator()


    def run(self):
        today = datetime.datetime.now()
        global current_year
        current_day, current_month, current_year = today.day, today.month, today.year
       
        while(True):
            print("Please enter year to be processed")
            year = input()
            if year == "":
                print("User provided no input")
                print("Please enter any one option H-> historic_years(2012-2017), R->recent_years(2018-(cur_year-1)), C->current_year or X-> exit")
                data_type = str(input())
                if data_type == 'H':
                    years = list(self.historic_years.split(","))
                    print(years)
                    for year in years:
                        print("Processing for the year {}".format(year))
                        code = self.get_parkingOccupancy_file_code(year)
                        self.sync_files(year,code)
                    break
                elif data_type == 'R':
                    years = list(self.recent_years.split(","))
                    print(years)
                    for year in years:
                        print("Processing for the year {}".format(year))
                        code= self.get_parkingOccupancy_file_code(year)
                        self.sync_files(year,code)
                    break
                elif data_type == 'C':
                    year = current_year
                    code = self.get_parkingOccupancy_file_code(year)
                    self.sync_files(year,code)
                    break
                else:
                    break

            else:
                year = int(year)
                code = self.get_parkingOccupancy_file_code(year)
                self.sync_files(year,code)
                break


        # Download Blockface data
        self.sync_sec_file(self.blocface_url)


        

# Entry point for the pipeline
if __name__ == "__main__":
    parkingIngest = ParkingOccupancyIngest()
    parkingIngest.run()

    
        
