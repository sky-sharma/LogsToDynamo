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
var ConnectionsThisFile = []; // Clear array of Connections collected from last file
var logFiles = [];
var logFileNum = 0;

// var dataArray = [];
// var AllConnections = [];
// var Disconnections = [];

s3.listObjects(params, function(err, data)
{
  if (err) console.log(err, err.stack); //an error occurred
  else
  {
    logFiles = data.Contents;
    readS3AndGetPutConnection(logFiles, logFileNum++);
}});

function readS3AndGetPutConnection(logFiles, logFileIndex)
{
  // Recursive function to read s3 item, then put connections in dBase
  // This involves reading existing numbers of connections / disconnections
  // from the dBase and adding to the tallies.

  console.log('logFileIndex: ', logFileIndex);

  if (logFileIndex >= logFiles.length) return; // All done

  var logFileName = logFiles[logFileIndex].Key;
  params.Key = logFileName;
  // Get Log File Name

  s3.getObject((params), (err, fileContents) =>
  {
    // Read contents of fileContents
    if (err) throw err;
    var logContents = fileContents.Body;

    ConnectionsThisFile = utils.parseLog(logContents, searchStrings, dataSearchPatterns);
    getAndPutConnection(ConnectionsThisFile, 0);
  });
};

// Put IpAddresses of Connections in Connections table.
function getAndPutConnection(Connections, ConnectionNum)
{
  // Recursive function to make sure dBase read and write happen sequentially.
  // We read a record from the dBase, get the Number of Connections and Number of Disconnections
  // from the record and add to those.

console.log(Connections.length);

  if (ConnectionNum >= Connections.length)
  {
    readS3AndGetPutConnection(logFiles, logFileNum++);
    return; // All done.
  }

  Connection = Connections[ConnectionNum];
  var PrincipalID = Connection[2];
  var IpAddress = Connection[3];
  var Status = Connection[5];
  var LastConnDisconn = Connection[0][0] + ' ' + Connection[0][1];
  var TotalNumConnections = Connection[6];
  var TotalNumDisconnections = Connection[7];

  var dBasePutParams =
  {
    TableName: 'Indie_Connections',
    Item:
    {
      'PrincipalID': PrincipalID,
      'Last_IpAddress': IpAddress,
      'Current_Status': Status, //Connected or Disconnected
      'LastConnDisconn_Time': LastConnDisconn, // Concatenating Date and Time
      'TotalNumConnections': TotalNumConnections,
      'TotalNumDisconnections': TotalNumDisconnections
    }
  }

  var dBaseGetParams =
  {
    TableName: 'Indie_Connections',
    Key:
      {
        'PrincipalID': PrincipalID
      }
  }

  docClient.get(dBaseGetParams, (err, readRecord) =>
  {
    if (err) console.log(err);
    else if ((Object.keys(readRecord).length === 0) && (readRecord.constructor === Object))
    // The particular record is not found in the dBase, so enter it
    {
      docClient.put(dBasePutParams, (err, data) =>
      {
        if (err) console.error('Unable to add item. Error JSON:', JSON.stringify(err, null, 2));
        else
        {
          console.log('TotalNumDisconnections: ', TotalNumDisconnections);
          getAndPutConnection(Connections, ConnectionNum + 1); // This function calls itself recursively
        }
      });
    }

    else
    {
      // If Status is Connected then take TotalNumConnections for this PrincipalID in dBase,
      // add new TotalNumConnections and write this new val to dBase.

      // If Status is Disonnected then take TotalNumDisconnections for this PrincipalID in dBase,
      // add new TotalNumDisconnections and write this new val to dBase.

      dBasePutParams.Item.TotalNumConnections += readRecord.Item.TotalNumConnections;
      dBasePutParams.Item.TotalNumDisconnections += readRecord.Item.TotalNumDisconnections;
      docClient.put(dBasePutParams, (err, data) =>
      {
        if (err) console.error('Unable to add item. Error JSON:', JSON.stringify(err, null, 2));
        else
        {
          getAndPutConnection(Connections, ConnectionNum + 1); // This function calls itself recursively
        }
      });
    }
  });
}
