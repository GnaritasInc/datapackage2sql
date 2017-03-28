const printf = require('printf');
const Datapackage = require("datapackage").Datapackage;
const JSZip = require('jszip');
const SqlString = require('sqlstring');


const MAX_KEY_LEN = 255;

var zipArchive = null;

function sqlDataType (field, schema) {
	var types = {
		"string":"text",
		"number":"decimal",
		"integer":"int",
		"boolean":"varchar(5)",
		"date":"date",
		"time":"time",
		"datetime":"datetime"
	};

	if (isEnum(field)) {
		return getEnumDef(field);
	}

	if (field.type == "string" && isKeyColumn(field, schema)) {
		return printf("varchar(%d)", MAX_KEY_LEN);
	}
	
	if(field.type == "string" && field.constraints && "maxLength" in field.constraints) {		
		return printf("varchar(%d)", field.constraints.maxLength);
	}

	return field.type in types ? types[field.type] : "text";
}

function inArray (elem, arr) {
	return arr.indexOf(elem) >= 0 ? true : false;
}

function isKeyColumn (field, schema) {
	return isPrimaryKey(field, schema) || isForeignKey(field, schema) || isUniqueKey(field);
}

function isUniqueKey (field) {
	return field.constraints && field.constraints.unique;
}

function isPrimaryKey (field, schema) {
	if (!schema.primaryKey) return false;
	var fieldName = field.name;
	var primaryKeys = getArray(schema.primaryKey);
	return inArray(fieldName, primaryKeys);
}

function isForeignKey (field, schema) {
	if (!schema.foreignKeys) return false;
	var fieldName = field.name;
	var fkCols = [];
	schema.foreignKeys.forEach(function (fk) {
		fkCols.push(fk.fields);
	});

	return inArray(fieldName, fkCols);
}

function isEnum (field) {
	return (field.constraints && field.constraints.enum && field.constraints.enum.length) ? true : false;
}

function getEnumDef (field) {	
	var format = "enum ("+ (new Array(field.constraints.enum.length)).fill("?").join(", ") +")";
	return SqlString.format(format, field.constraints.enum);
}

function getFieldConstraints (field) {
	var constraints = [];
	if (!field.constraints) {
		return "";
	}
	if (field.constraints.required) {
		constraints.push("not null");
	}
	if (field.constraints.unique) {
		constraints.push("unique");
	}

	return constraints.join(" ");
}

function getArray (val) {
	return Array.isArray(val) ? val : [val];
}

function getColumnDefs (schema) {
	var defs = [];
	schema.fields.forEach(function (field) {
		var def = SqlString.format("?? ", field.name);
		def += sqlDataType(field, schema);
		var constraints = getFieldConstraints(field);
		if (constraints) {
			def += " "+constraints;
		}
		defs.push(def);
	});

	if (schema.primaryKey) {
		var cols = getArray(schema.primaryKey);
		var format = (new Array(cols.length)).fill("??").join(", ");		
		defs.push("primary key ("+ SqlString.format(format, cols) +")");
	}

	if (schema.foreignKeys) {
		schema.foreignKeys.forEach(function (fk) {
			var def = SqlString.format("foreign key (??)", fk.fields);
			def += SqlString.format(" references ?? (??)", [fk.reference.resource, fk.reference.fields]);
			defs.push(def);
		});
	}

	return defs;
}

function getTableDef (resource) {
	var output = "";
	output += SqlString.format("create table ?? (", resource.name);	
	output += "\n\t" + getColumnDefs(resource.descriptor.schema).join(",\n\t");
	output += "\n);\n";

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
			console.log("Parsed base64 body");
			resolve(buf);
	});	
}

function parseBody (body) {
	zipArchive = null; // DS: This seems to hang around between invocations on AWS
	
	return new Promise(function (resolve, reject) {
		try {			
			if (body.descriptor) {				
				resolve(body.descriptor);
			}
			else if (body.datapackage) {
				resolve(parseBase64Body(body.datapackage));
			}
			else {
				throw "Request body must include either a JSON descriptor in 'descriptor' or a base 64 encoded datapackage ZIP archive in 'datapackage'.";
			}
		}
		catch (err) {
			reject(err);
		}	
	});
	
}

function parseBase64Body (body) {
	
	return parseBase64(body).then(
		zipFile => JSZip.loadAsync(zipFile)		
	).then(function (zip) {
		console.log("Parsed ZIP archive");
		zipArchive = zip;
		var descriptor = zip.file("datapackage.json");
		if (!descriptor) {
			throw new Error("Can't find datapackage.json at archive root.");
		}
		return descriptor.async("string");
	}).then(JSON.parse);

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
	
	parseBody(event.body).then(		
		data => new Datapackage(data)
	).then(function (datapackage) {		
		var output = "";
		datapackage.resources.forEach(function (resource) {		
			output += getTableDef(resource);		
		});
		
		if (zipArchive) {					
			output += "\n -- Insert statements from ZIP archive data to go here.\n"
		}

		return output;

	}).then(
		output => done(null, output)
	).catch(		
		
		err => done(new Error(err))
	);		
};

