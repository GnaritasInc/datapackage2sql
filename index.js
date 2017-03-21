const printf = require('printf');
const Datapackage = require("datapackage").Datapackage;

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



exports.handler = function (event, context, callback) {
	const done = (err, res) => callback(null, {
	    statusCode: err ? '400' : '200',
	    body: err ? err.message : res,
	    headers: {
	        'Content-Type': 'text/plain',
	    },
	});
	var output = "";
	try {
		var data = JSON.parse(event.body);
	}
	catch (e) {
		done(new Error("JSON parse error: "+e.message));
		return;
	}
	new Datapackage(data).then(function (datapackage) {
		datapackage.resources.forEach(function (resource) {		
			output += printf("create table `%s` (", resource.name);	
			output += ("\n\t" + getColumnDefs(resource.descriptor.schema).join(",\n\t"));
			output += (");");
		});
		done(null, output);
	}).catch(function (err) {
		done(err);
	});
};




