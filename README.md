# Covid Analysis (Pyspark)

## Problem Statement

Write script for covid analysis in pyspark and create rest API for results.


1. Use Python to make a request to https://rapidapi.com/Gramzivi/api/covid-19-data/ for at least 20 countries using ‘/getLatestCountryDataByName’ API, fill up the csv file, create a dataFrame using Spark.
2. Using the Above dataFrame, find out the following:
  1. Most affected country among all the countries ( total death/total covid cases).
  2. Least affected country among all the countries ( total death/total covid cases).
  3. Country with highest covid cases.
  4. Country with minimum covid cases.
  5. Total cases.
  6. Country that handled the covid most efficiently( total recovery/ total covid cases).
  7. Country that handled the covid least efficiently( total recovery/ total covid cases).
  8. Country least suffering from covid ( least critical cases).
  9. Country still suffering from covid (highest critical cases).
3. Create a RestFul API to show datas collected in question 1.
4. Create each RestFul API to show the result of every sub question in question 2.


## Solution


pip3 install virtualenv

virtualenv --version

vi ~/.zshrc

export PATH=$PATH:/Users/mzadafiya/Library/Python/3.8/bin

source ~/.zshrc

virtualenv venv

source venv/bin/activate

git clone git@github.com:mzadafiya/pyspark-assignment.git

cd pyspark-assignment

git checkout assignment

pip3 install -r requirements.txt

nohup python3 src/rest_api/api.py & python3 -m src/flow.py
