from flask import Flask
from flask_restful import Resource, Api
import csv
import os
from operator import itemgetter

app = Flask(__name__)
api = Api(app)


filepath = './data/output/covid-analysis-result.csv'
results = {}
with open(filepath, encoding='utf-8') as csvf:
    # load csv file data using csv library's dictionary reader
    csvReader = csv.DictReader(csvf)
    # convert each csv row into python dict
    for row in csvReader:
        # transform this python dict
        country,kpi,value,degree = itemgetter('country','kpi','value','degree')(row)
        if kpi in results:
            results[kpi][degree] = (value, country)
        else:
            results[kpi] = {degree: (value, country)}

class LatestCovidData(Resource):
    def get(self):
        return results, 200


class CovidImpact(Resource):

    def get(self, degree):
        kpi = 'Covid Impact'
        value, country = results[kpi][degree]
        return '{} is having {} {} with death rate {}.'.format(country, degree, kpi, value), 200


class CovidConfirmedCases(Resource):

    def get(self, degree):
        kpi = 'Confirmed cases'
        value, country = results[kpi][degree]
        return '{} is having {} {} with confirmed cases {}.'.format(country, degree, kpi, value), 200


class TotalCases(Resource):

    def get(self):
        return '{} total covid cases across world.'.format(results['Total Covid Cases']['-'][0]), 200


class CovidEfficiency(Resource):

    def get(self, degree):
        kpi = 'Covid Efficiency(recovered/confirmed)'
        value, country = results[kpi][degree]
        return '{} is having {} {} with efficiency rate {}.'.format(country, degree, kpi, value), 200


class CovidSuffering(Resource):

    def get(self, degree):
        kpi = 'Covid Suffering(active)'
        value, country = results[kpi][degree]
        return '{} is having {} {} with suffering rate {}.'.format(country, degree, kpi, value), 200


api.add_resource(LatestCovidData, '/latest-covid-data')
api.add_resource(CovidImpact, '/covid-impact/<string:degree>')
api.add_resource(CovidEfficiency, '/covid-efficiency/<string:degree>')
api.add_resource(CovidConfirmedCases, '/confirmed-cases/<string:degree>')
api.add_resource(CovidSuffering, '/covid-suffering/<string:degree>')
api.add_resource(TotalCases, '/total-cases')

if __name__ == '__main__':
    app.run()
