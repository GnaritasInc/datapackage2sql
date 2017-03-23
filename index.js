require("buffer-v6-polyfill");

const printf = require('printf');
const Datapackage = require("datapackage").Datapackage;
const JSZip = require('jszip');


function sqlDataType (field) {
	var types = {
		"string":"varchar",
		"number":"decimal",
		"integer":"int",
		"boolean":"varchar",
		"date":"date",
		"time":"time",
		"datetime":"datetime"
	};

	return field.type in types ? types[field.type] : "varchar";
}

function getColumnDefs (schema) {
	var defs = [];
	schema.fields.forEach(function (field) {
		var def = printf("`%s` %s", field.name, sqlDataType(field));
		defs.push(def);
	});

	return defs;
}

function getTableDef (resource) {
	var output = "";
	output += printf("create table `%s` (", resource.name);	
	output += ("\n\t" + getColumnDefs(resource.descriptor.schema).join(",\n\t"));
	output += ("\n);\n");

	return output;
}

exports.handler = function (event, context, callback) {
	const done = (err, res) => callback(null, {
	    statusCode: err ? '400' : '200',
	    body: err ? err.message : res,
	    headers: {
	        'Content-Type': 'text/plain',
	    },
	});	
	
	var output = "";
	
	var zipFile = Buffer.from(event.body, 'base64');	

	JSZip.loadAsync(zipFile).then(function (zip) {
		return zip.file("datapackage.json").async("string");
	}).then(JSON.parse).then(function (data) {			
		return new Datapackage(data);
	}).then(function (datapackage) {
		console.log("Found "+datapackage.resources.length+" resources");
			
		datapackage.resources.forEach(function (resource) {		
			output += getTableDef(resource);		
		});
		
		return output;

	}).then(function (ouptut) {
		done(null, output);
	}).catch(function (err) {
		console.log("Error: "+err);
		done(new Error(err));
	});
		
};




