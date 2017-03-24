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

function oldParseBase64 (str) {
	return new Buffer(str, 'base64');
}

function parseBase64 (str) {
	return new Promise(function (resolve, reject) {
		var buf;
			try {
				buf = Buffer.from(str, 'base64');
			}
			catch (err) {
				try {
					buf = oldParseBase64(str);
				}
				catch(err) {
					console.log("Can't parse base 64!");
					reject("Base 64 parse failure: "+err.message);
				}
			}	
			resolve(buf);
	});	
}

exports.handler = function (event, context, callback) {
	const done = (err, res) => {
		if(err) {
			console.log("Error: "+err.message);
		}
		callback(null, {
		    statusCode: err ? '400' : '200',
		    body: err ? err.message : res,
		    headers: {
		        'Content-Type': 'text/plain',
		    },
		});	
	};		

	parseBase64(event.body).then(
		zipFile => JSZip.loadAsync(zipFile)		
	).then(function (zip) {
		var descriptor = zip.file("datapackage.json");
		if (!descriptor) {
			throw new Error("Can't find datapackage.json at archive root.");
		}
		return descriptor.async("string");
	}).then(JSON.parse).then(		
		data => new Datapackage(data)
	).then(function (datapackage) {		
		var output = "";
		datapackage.resources.forEach(function (resource) {		
			output += getTableDef(resource);		
		});
		
		return output;

	}).then(
		output => done(null, output)
	).catch(		
		err => done(new Error(err))
	);		
};

