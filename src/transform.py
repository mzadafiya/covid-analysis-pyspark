###################
# Data Transform - Affected
# Find out country with highest confirmed covid cases.
###################

import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

import configparser
from setup_logger import create_logger


class Transform():

    def setup(self):
        self.logger.info('Reading config parameters src_file name')

        config = configparser.ConfigParser()
        config.read('./config/config.toml')

        self.src_filename = config['data']['src_file']

        self.spark = SparkSession \
            .builder \
            .master("local[1]") \
            .appName("SparkByExamples.com") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')

    def create_spark_dataframe(self):

        file_type = "csv"

        # CSV options
        infer_schema = "true"
        first_row_is_header = "true"
        delimiter = ","

        # The applied options are for CSV files. For other file types, these will be ignored.
        self.df = self.spark.read.format(file_type) \
            .option("inferSchema", infer_schema) \
            .option("header", first_row_is_header) \
            .option("sep", delimiter) \
            .load(self.src_filename)

        self.total_countries = self.df.count()

    def create_csv_from_res(self, res_filename, data, task):

        # create directories if not exist
        os.makedirs(os.path.dirname(res_filename), exist_ok=True)

        with open(res_filename, 'w+') as f:
            json.dump(data, f)

        if not os.path.exists(res_filename):
            self.logger.error('{}: Upload result to csv file is failed. A file does not exist'.format(task))
            self.logger.error("Task failed")
            raise Exception('{}: Upload result to csv file is failed. A file does not exist'.format(task))

        self.logger.info('{}: Upload result to csv file is successful'.format(task))

    def transform_impact(self):
        task = 'Covid Impact'

        self.logger.info('{}: Creating new dataframe'.format(task))

        affected_country_df = self.df \
            .withColumn("affected", round(col("deaths")/col("confirmed"), 5)) \
            .sort(col("affected")).collect()

        for deg, index in [('lowest', 0), ('highest', self.total_countries-1)]:
            data = {
                'country': affected_country_df[index][1],
                'kpi': task,
                'value': affected_country_df[index][8],
                'degree': deg
            }

            self.create_csv_from_res('./data/temp/transform_impact_{}_res.csv'.format(deg), data, task)

    def transform_confirmed(self):
        task = 'Confirmed cases'

        self.logger.info('{}: Creating new dataframe'.format(task))

        sort_by_total_cases_df = self.df.sort(col("confirmed")).collect()

        for deg, index in [('lowest', 0), ('highest', self.total_countries-1)]:
            data = {
                'country': sort_by_total_cases_df[index][1],
                'kpi': task,
                'value': sort_by_total_cases_df[index][4],
                'degree': deg
            }

            self.create_csv_from_res('./data/temp/transform_confirmed_{}_res.csv'.format(deg), data, task)

    def transform_efficiency(self):
        task = 'Covid Efficiency(recovered/confirmed)'

        self.logger.info('{}: Creating new dataframe'.format(task))

        efficiency_df = self.df \
            .withColumn("efficiency", round(col("recovered")/col("confirmed"), 4)) \
            .sort(col("efficiency")).collect()

        for deg, index in [('lowest', 0), ('highest', self.total_countries-1)]:
            data = {
                'country': efficiency_df[index][1],
                'kpi': task,
                'value': efficiency_df[index][8],
                'degree': deg
            }

            self.create_csv_from_res('./data/temp/transform_efficiency_{}_res.csv'.format(deg), data, task)

    def transform_suffering(self):
        task = 'Covid Suffering(active)'

        self.logger.info('{}: Creating new dataframe'.format(task))

        active_cases_df = self.df.sort(col("active")).collect()

        for deg, index in [('lowest', 0), ('highest', self.total_countries-1)]:
            data = {
                'country': active_cases_df[index][1],
                'kpi': task,
                'value': active_cases_df[index][7],
                'degree': deg
            }

            self.create_csv_from_res('./data/temp/transform_suffering_{}_res.csv'.format(deg), data, task)

    def transform_total_cases(self):
        task = 'Total Covid Cases'

        self.logger.info('{}: Creating new dataframe'.format(task))

        total_cases_df = self.df.groupBy().sum("confirmed").collect()

        data = {
            'country': 'Whole World',
            'kpi': task,
            'value': total_cases_df[0][0],
            'degree': '-'
        }

        self.create_csv_from_res('./data/temp/transform_total_res.csv', data, task)

    def run(self):

        self.logger = create_logger(task='Data Transform')
        self.logger.info('Starting task run...')

        self.setup()
        self.create_spark_dataframe()
        self.transform_impact()
        self.transform_confirmed()
        self.transform_efficiency()
        self.transform_suffering()
        self.transform_total_cases()


if __name__ == "__main__":
    obj = Transform()
    obj.run()
