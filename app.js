const fs = require('fs');
const path = require('path');
const utils = require('./utils.js');
const aws = require('aws-sdk');

var s3 = new aws.S3();
var dynamodb = new aws.DynamoDB({ region: 'us-west-2' });
var docClient = new aws.DynamoDB.DocumentClient({ service: dynamodb });

aws.config.update({ endpoint: 'https://s3.us-west-2.amazonaws.com' });

var params = {
  Bucket: 'connection-logs' /* required */};

var searchStrings = { Connection: 'Connect Status: SUCCESS', Disconnection: 'Disconnect Status: SUCCESS' };
var dataSearchPatterns = ['%s %s', 'TRACEID:%s', 'PRINCIPALID:%s', 'IpAddress: %s ', 'SourcePort: %s'];
var dataArray = [];
var AllConnections = [];
var Disconnections = [];
var ActiveConnections = [];

// var ConnDisconnParams = [
//   { LogFileName: 'CWL_Monitor_Logs.txt',
// ConnectionType: 'Monitor' },
//
// { LogFileName: 'CWL_WebUI_Logs.txt',
// ConnectionType: 'WebUI' }
// ]

s3.listObjects(params, function(err, data)
{
  if (err) console.log(err, err.stack); //an error occurred
  else
  {
    var logFiles = data.Contents;
    logFiles.forEach((logFile) => {
      var logFileName = logFile.Key;
      params.Key = logFileName;
      // Get Log File Name

      s3.getObject((params), (err, fileContents) =>
      {
        // Read contents of fileContents
        if (err) throw err;
        var logContents = fileContents.Body;

        Connections = utils.parseLog(logContents, searchStrings, dataSearchPatterns);

        console.log(Connections);

        // Put NumConnections & NumDisconnections in ConnectionsDisconnections table
        /*
        var dBaseParams = {
          TableName: 'Connections',
          Item: {
            'ConnectionType': ConnDisconnParam.ConnectionType,
            'NumConnections': Connections.length,
            'NumDisconnections': Disconnections.length
          }
        }

        docClient.put(dBaseParams, (err, data) =>
        {
          if (err) console.error('Unable to add item. Error JSON:', JSON.stringify(err, null, 2));
          else console.log('PutItem succeeded');
        });
        */

        // Put IpAddresses of Connections in Connections table.

        Connections.forEach((Connection) => {
        {
          var dBaseParams =
          {
          TableName: 'Connections',
          Item: {
                'PrincipalID': Connection[2],
                'IpAddress': Connection[3],
                'Status': Connection[5], //Connected or Disconnected
                'LastConnDisconn': Connection[0][0] + ' ' + Connection[0][1] // Concatenating Date and Time
                }
          }

          docClient.put(dBaseParams, (err, data) =>
          {
            if (err) console.error('Unable to add item. Error JSON:', JSON.stringify(err, null, 2));
            else console.log('PutItem succeeded');
          });
        }
      })
    });
  })
}});
