const fs = require('fs');
const path = require('path');
const utils = require('./utils.js');
const aws = require('aws-sdk');

var s3 = new aws.S3();
var dynamodb = new aws.DynamoDB({ region: 'us-east-1' });
var docClient = new aws.DynamoDB.DocumentClient({ service: dynamodb });

aws.config.update({ endpoint: 'https://dynamodb.us-east-1.amazonaws.com' });

var params = {
  Bucket: 'connection-disconnection-logs' /* required */};

//var searchStrings = ['Connect Status: SUCCESS', 'Disconnect Status: SUCCESS'];
var searchPatterns = ['TRACEID:%s', 'IpAddress: %s ', 'SourcePort: %s'];
var dataArray = [];
var Connections = [];
var Disconnections = [];

// s3.listObjects(params, function(err, data) {
//   // List files in Bucket
//
//   if (err) console.log(err, err.stack); // an error occurred
//   else
//   {
//     var logFiles = data.Contents;
//     logFiles.forEach((logFile) => {
//       // Go through files in Bucket one by one
//
//       var logFileName = logFile.Key;
//       params.Key = logFileName;
//       // Get Log File Name
//
//       if(logFileName === 'CWL_Monitor_Logs.txt')
//       {

//var ConnDisconnParam = { LogFileName: '', ConnectionsField: '', DisconnectionsField: '' };

var ConnDisconnParams = [
  { LogFileName: 'CWL_Monitor_Logs.txt',
ConnectionType: 'Monitor' },

{ LogFileName: 'CWL_WebUI_Logs.txt',
ConnectionType: 'WebUI' }
]

ConnDisconnParams.forEach((ConnDisconnParam) => {
  params.Key = ConnDisconnParam.LogFileName;
  s3.getObject((params), (err, fileContents) => {
    // Read contents of fileContents

  if (err) throw err;
  var logContents = fileContents.Body;
  //var connectionTypeFieldName = JSON.stringify(ConnDisconnParam.ConnectionType);
  // var disconnectionsFieldName = JSON.stringify(ConnDisconnParam.DisconnectionsField);

  Connections = utils.parseMonitorLog(logContents, 'Connect Status: SUCCESS', searchPatterns);
  Disconnections = utils.parseMonitorLog(logContents, 'Disconnect Status: SUCCESS', searchPatterns);


  var dBaseParams = {
    TableName: 'ConnectionsDisconnections',
    Item: {
      'ConnectionType': ConnDisconnParam.ConnectionType,
      '#Connections': Connections.length,
      '#Disconnections': Disconnections.length
    }
  }

  docClient.put(dBaseParams, (err, data) => {
         if (err) {
             console.error('Unable to add item. Error JSON:', JSON.stringify(err, null, 2));
         } else {
             console.log('PutItem succeeded');
         }
      });
});

    });
//   );
//       };
//   });
//   };
// });

/*
fs.readdir(logsDir, (err, logFiles) => {
  logFiles.forEach((logFile) => {
    fs.readFile((path.join(logsDir, logFile)), (err, logContents) => {
      if (err) throw err;
      console.log(path.basename(logFile, '.txt'));
      var floatsArray = utils.parseStrForFloats(logContents, fields);
      console.log(floatsArray);
      console.log(utils.calcColAvgs(floatsArray));
    });
  });
});
*/
