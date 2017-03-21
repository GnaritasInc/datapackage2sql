# datapackage2sql
AWS Lambda micro service to generate SQL from Frictionless Data [Tabular Data Packages](http://specs.frictionlessdata.io/tabular-data-package/).

## Installation
* Clone the repo and run `npm install`

## Usage
Given a tablular data package (like [this one](https://github.com/datasets/gdp)), upload its `datapackage.json` file in the body of a POST request:
	
	curl --data @path/to/datapackage.json https://6goo1zkzoi.execute-api.us-east-1.amazonaws.com/prod/datapackage2sql

You'll get SQL output to generate the data stuctures it defines:

	create table `gdp` (
        `Country Name` varchar,
        `Country Code` varchar,
        `Year` date,
        `Value` decimal);

## TODO
* Modify to optionally accept a ZIP archive of the descriptor plus data and generate SQL to populate the database too.