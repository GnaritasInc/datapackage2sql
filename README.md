# datapackage2sql
AWS Lambda micro service to generate SQL from Frictionless Data [Tabular Data Packages](http://specs.frictionlessdata.io/tabular-data-package/).

## Installation
* Clone the repo and run `npm install`

## Usage
The service accepts http POST requests with a JSON payload. The JSON can include either the contents of the data package's `datapackage.json` descriptor file in the `descriptor` property, or a ZIP archive containing the descriptor plus CSV data as a base64 encoded string in the `datapackage` property, plus an optional `tablePrefix` property:

### With JSON descriptor:

	{
		"tablePrefix":"gn_",
		"descriptor": {
		  "name": "gdp",
		  "title": "Country, Regional and World GDP (Gross Domestic Product)",		  
		  "resources": [
		    {
		      "name": "gdp",
		      "path": "data/gdp.csv",
		      "schema": {
		        "fields": [
		          {
		            "name": "Country Name",
		            "type": "string"
		          },
		          
		         (etc.)
		}
	}


### With ZIP archive:

	{
		"tablePrefix":"gn_",
		"datapackage":"UEsDBBQAAAAI ...(base64 encoded binary data)... yqQ8hm1xp"
	}

When using the ZIP archive option, the `datapackage.json` file should be at the archive root.

The output will be SQL statements to create the tables described in the descriptor. Each `resource` object in the descriptor becomes a table with the name in its `name` property, prefixed with the string sepcified in the `tablePrefix` property, if present.

### Example:

	curl --data @path/to/json/file.json https://xxxx.execute-api.us-east-1.amazonaws.com/prod/datapackage2sql

## TODO
* Include insert statements to populate tables parsed out of ZIP archive data.
