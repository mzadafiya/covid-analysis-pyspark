###################
# Data Extract
# Make a request to https://rapidapi.com/ShubhGupta/api/covid19-data/ for atleast 20 countries using '/GetByCountryName' API, fill up the csv file
###################

import os
import json
import requests
import pandas as pd

import configparser

from setup_logger import create_logger


class Extract():

    def setup(self):
        self.logger.info('Reading config parameters x_rapid_api_key, countries, api_host, api_url and src_file')

        config = configparser.ConfigParser()
        config.read('./config/config.toml')

        api_host = config['api']['host']
        x_rapid_api_key = config['api']['secret_key']

        self.api_url = "https://{}".format(api_host)
        self.headers = {
            "X-RapidAPI-Host": api_host,
            "X-RapidAPI-Key": x_rapid_api_key
        }
        self.countries = config['api']['countries'].split(',')
        self.filename = config['data']['src_file']

    def is_api_available(self):
        """ Check if COVID API is available """

        error = None
        try:
            requests.request('GET', self.api_url, headers=self.headers, params={"country": 'dummy'})
        except requests.exceptions.HTTPError as errh:
            error = "Http Error: %s" % errh
        except requests.exceptions.ConnectionError as errc:
            error = "Error Connecting: %s" % errc
        except requests.exceptions.Timeout as errt:
            error = "Timeout Error: %s" % errt
        except requests.exceptions.RequestException as err:
            error = "Unknown Error" % err

        if error:
            self.logger.error("API is not available. %s" % error)
            self.logger.error("Task failed")
            raise Exception("API is not available. %s" % error)

    def get_covid_data_by_country(self, country):
        """
        Make request to covid API and return response and error if any 
        Args:
            country (string): country name
        Returns:
            data: response data
            status_code == 200, return response
            status_code != 200, log error message
        """

        # Query string
        querystring = {"country": country}

        data = None

        self.logger.info('Request for %s: Fetching started', country)
        response = requests.request(
            'GET', self.api_url, headers=self.headers, params=querystring)
        if response.status_code == 200:
            data = json.loads(response.text)
            self.logger.info('Request for %s: Request successful', country)
        else:
            self.logger.error('Request for %s: Request failed. Invalid status code %s. Error: %s' %
                              (country, response.status_code, response.text))

        return data

    def get_covid_data(self):
        response_data = []

        self.logger.info('Retrieve latest covid data: Started')
        self.logger.info('Retrieve latest covid data: Making requests for given countries')

        # Make request for each countries
        for country in self.countries:
            data = self.get_covid_data_by_country(country)
            if data:
                response_data.append(data)

        if not response_data:
            self.logger.error('Retrieve latest covid data: Covid endpoint didn\'t return any data for given countries')
            self.logger.error("Task failed")
            raise Exception('Retrieve latest covid data: Covid endpoint didn\'t return any data for given countries')

        self.logger.info('Retrieve latest covid data: Completed')
        return response_data

    def response_to_csv(self, response_data):

        os.makedirs(os.path.dirname(self.filename), exist_ok=True)

        self.logger.info('Creating panda dataframe from collected data')
        df = pd.read_json(json.dumps(response_data))

        self.logger.info('Creating or overwriting file from panda dataframe')
        df.to_csv(self.filename, index=False)

        if not os.path.exists(self.filename):
            self.logger.error('Collect data to csv file failed. A file does not exist')
            self.logger.error("Task failed")
            raise Exception('Collect data to csv file failed. A file does not exist')

        self.logger.info('Collect data to csv file is successful')

    def run(self):

        self.logger = create_logger(task='Data Extract')
        self.logger.info('Starting task run...')

        self.setup()

        self.is_api_available()

        response_data = self.get_covid_data()
        self.response_to_csv(response_data)

        self.logger.info('Task run successful...')


if __name__ == "__main__":
    obj = Extract()
    obj.run()
