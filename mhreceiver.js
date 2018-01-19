"use strict";

// require dotenv
const dotenv=require('dotenv');
//var util = require("util");
var cfenv = require("cfenv");
dotenv.config();

// load local VCAP configuration and service credentials
var vcapLocal;
try {
  vcapLocal = require('./vcap-local.json');
  console.log("Loaded local VCAP");
} catch (e) { }

const appEnvOpts = vcapLocal ? { vcap: vcapLocal} : {}
const appEnv = cfenv.getAppEnv(appEnvOpts);

// Configure Message Hub service using VCAP or ENV properties
var mh_credentials ={};
var mh_configuration ={};
var opts ={};

if (appEnv.services['messagehub'] || appEnv.getService(/messagehub/)) {
    mh_credentials = appEnv.services['messagehub'][0].credentials;
    opts.brokers = mh_credentials.kafka_brokers_sasl;
    opts.username = mh_credentials.user;
    opts.password = mh_credentials.password;
    console.log("Retrieved Message Hub service credentials from VCAP file");
} else if (process.env.MHBROKERLIST) {
    opts.brokers = process.env.MHBROKERLIS;
    opts.username = process.env.MHUSERNAME;
    opts.password = process.env.MHPASSWORD;
    console.log("Retrieved Message Hub service credentials from environment variables")
} else {
    console.log("Could not find Message Hub service credentials")
}

var consumer_opts = {
  'metadata.broker.list': opts.brokers,
  'security.protocol': 'sasl_ssl',
  'ssl.ca.location': '/etc/ssl/certs',
  'sasl.mechanisms': 'PLAIN',
  'sasl.username': opts.username,
  'sasl.password': opts.password,
  'api.version.request': true,
  'security.protocol': 'sasl_ssl',
  'client.id': 'kafka-nodejs-console-sample-consumer',
  'group.id': 'kafka-nodejs-console-sample-group'
};

// Configure DB service using VCAP or ENV variables
var dbconfiguration;

if (appEnv.services['dashDB'] || appEnv.getService(/dashDB/)) {
    dbconfiguration = appEnv.services['dashDB'][0].credentials;
    console.log("Retrieved dashDB service credentials from vcap file");
} else if (process.env.DATABASE) {
    dbconfiguration = {
        db : process.env.DATABASE,
        port   : process.env.DBPORT,
        username : process.env.DBUID,
        password : process.env.DBPWD,
        hostname: process.env.DBHOSTNAME
    };
    console.log("Retrieved DB2 service credentials from environment variables")
} else {
    console.log("Could not find DB2 service credentials")
}

dbconfiguration.table = process.env.DBTABLE;
dbconfiguration.driver = '{DB2}';

/*require the ibm_db module*/
var ibmdb = require('ibm_db');
var format = require("string-template");
const db2 = require("./ibmdb2interface");

var dbconnection = format ("DRIVER={driver};DATABASE={db};UID={username};PWD={password};HOSTNAME={hostname};port={port}",dbconfiguration);

// Kafka/Message Hub Consumer creation
var Kafka = {};
Kafka = require('node-rdkafka');
var topicName = 'elevator-events';
var deviceType = "Elevator";

// Read from the topic... note that this creates a new stream on each call!
var stream = Kafka.Consumer.createReadStream(consumer_opts, {}, {topics: [topicName]});
console.log('Created Kafka read stream');

// Read data from the stream
stream.on('data', function(message) {
//  console.log('Got message ' + JSON.stringify(message));
//  console.log('Received value ' + message.value.toString());

  var payload = JSON.parse(message.value.toString());
  var realPayload = payload.d;
  var deviceId = realPayload.deviceId;
  var deviceType = realPayload.deviceType;

  if (realPayload.timestamp && deviceId && deviceType) {
    var sql_stmt = db2.createSQLMergeStatement(dbconfiguration.table,deviceId,deviceType,realPayload);
      db2.executeSQLStatement(dbconnection,deviceId,sql_stmt);
  } else {
    console.log('Missing properties timestamp, deviceId or deviceType in payload');
  }
});

  // Handle errors receiving from Kafka
stream.on('error', function (err) {
  console.error('Error in kafka stream');
  console.error(err);
  process.exit();
});

// Listen on port 8000 or Cloud provided Port
// This is only to enable frequent health checking in Containers or CF
var http = require('http');

var server = http.createServer(function (request, response) {
  response.writeHead(200, {"Content-Type": "text/plain"});
  response.end("Hello World\n");
});

var port = (process.env.PORT || 8000);
server.listen(port);
