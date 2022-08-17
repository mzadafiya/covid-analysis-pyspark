###################
# Data Load
# Combine all results into csv file.
###################

import os
import json
from pyspark.sql import SparkSession

import pandas as pd
import shutil

import configparser
from setup_logger import create_logger


class Load():

    def setup(self):
        self.logger.info('Reading config parameters load_file name')

        config = configparser.ConfigParser()
        config.read('./config/config.toml')

        self.load_filename = config['data']['load_file']

        self.spark = SparkSession \
            .builder \
            .master("local[1]") \
            .appName("SparkByExamples.com") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')

    def combine_results(self):
        self.logger.info('Combine results: Starting')

        response_data = []
        response_files = './data/temp/'
        if os.path.isdir(response_files):

            for res_file in os.listdir(response_files):
                self.logger.info('Combine results: Reading %s file' % res_file)

                f = open(os.path.join(response_files, res_file))
                try:
                    response_data.append(json.load(f))
                except ValueError as verr:
                    self.logger.error('Combine results: Decoding JSON has failed')
                f.close()
        else:
            self.logger.error("Temp directory with results does not exist. Dir: %s" % response_files)
            self.logger.error("Task failed")
            raise Exception("Temp directory with results does not exist. Dir: %s" % response_files)

        if not response_data:
            self.logger.error("Combine results: Failed. no valid results found.")
            self.logger.error("Task failed")
            raise Exception("Combine results: Failed. no valid results found.")

        self.logger.info('Combine results: Completed')

        return response_data

    def response_to_csv(self, response_data):

        shutil.rmtree('./data/temp/')

        # create directories if not exist
        os.makedirs(os.path.dirname(self.load_filename), exist_ok=True)

        self.logger.info('Creating panda dataframe from collected response')

        df = pd.read_json(json.dumps(response_data))
        df.to_csv(self.load_filename, index=False)

        self.logger.info('Creating or overwriting file from panda dataframe')

        if not os.path.exists(self.load_filename):
            self.logger.error('Combine all results to file is failed. A file does not exist')
            self.logger.error("Task failed")
            raise Exception('Combine all results to file is failed. A file does not exist')

        self.logger.info('Combine all results to file is successful')

    def run(self):

        self.logger = create_logger(task='Data Load')
        self.logger.info('Starting task run...')

        self.setup()
        response_data = self.combine_results()

        self.response_to_csv(response_data)


if __name__ == "__main__":
    obj = Load()
    obj.run()
