const fs = require('fs');
const path = require('path');
const utils = require('./utils.js');
const aws = require('aws-sdk');

var s3 = new aws.S3();
var dynamodb = new aws.DynamoDB({ region: 'us-west-2' });
var docClient = new aws.DynamoDB.DocumentClient({ service: dynamodb });

//aws.config.update({ endpoint: 'https://s3.us-west-2.amazonaws.com' });

var params = {
  Bucket: 'connection-logs' /* required */};

  var searchStrings =
  {
    Connection: 'Connect Status: SUCCESS',
    Disconnection: 'Disconnect Status: SUCCESS',
    PublishIn: 'PublishIn Status: SUCCESS',
    PublishOut: 'PublishOut Status: SUCCESS'
  };

var connInfoSearchPatterns = ['%s %s', 'TRACEID:%s', 'PRINCIPALID:%s', 'IpAddress: %s ', 'SourcePort: %s'];
var topicSearchPattern = 'TOPICNAME:%s ';

var ConnectionsThisFile = []; // Clear array of Connections collected from last file
var logFiles = [];
var logFileNum = 0;

var PrincipalID; // dBase Key field

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

  if (logFileIndex >= logFiles.length) return; // All done

  console.log('logFileIndex: ', logFileIndex);

  var logFileName = logFiles[logFileIndex].Key;
  params.Key = logFileName;
  // Get Log File Name

  s3.getObject((params), (err, fileContents) =>
  {
    // Read contents of fileContents
    if (err) throw err;
    var logContents = fileContents.Body;

    infoThisFile = utils.parseLog(logContents, searchStrings, connInfoSearchPatterns, topicSearchPattern);
    getAndPutConnection(infoThisFile, 0);
  });
}

// Put IpAddresses of Connections in Connections table.
function getAndPutConnection(infoForDbase, recordNum)
{
  // Recursive function to make sure dBase read and write happen sequentially.
  // We read a record from the dBase, get the Number of Connections and Number of Disconnections
  // from the record and add to those.

  var dBasePutParams =
  {
    TableName: 'Indie_Connections'
  }

  var dBaseGetParams =
  {
    TableName: 'Indie_Connections'
  }

  if (recordNum >= infoForDbase.length)
  {
    readS3AndGetPutConnection(logFiles, logFileNum++);
    return; // All done.
  }

  currentRecord = infoForDbase[recordNum];
  var recordContents = currentRecord.Contents;

  if (recordContents === 'ConnInfo')
  {
    PrincipalID = currentRecord.ConnInfo[2];
    var IpAddress = currentRecord.ConnInfo[3];
    var Status = currentRecord.ConnInfo[5];
    var LastConnDisconn = currentRecord.ConnInfo[0][0] + ' ' + currentRecord.ConnInfo[0][1];

    dBaseGetParams.Key = { 'PrincipalID': PrincipalID };

    dBasePutParams.Item =
    {
      'PrincipalID': PrincipalID,
      'LastIpAddress': IpAddress,
      'CurrentStatus': Status, //Connected or Disconnected
      'LastConnDisconnTime': LastConnDisconn, // Concatenating Date and Time
      'TotalNumConnections': 0, // Placeholder
      'TotalNumDisconnections': 0, // Placeholder
      'PubInTopic': ' ', // Placeholder
      'PubInTopicNumMsgs': 0, // Placeholder
      'PubOutTopic': ' ', // Placeholder
      'PubOutTopicNumMsgs': 0 // Placeholder
    }
  };

  if (recordContents === 'PubInTopic')
  {
    var PubInTopicName = currentRecord.TopicName;
    PrincipalID = currentRecord.TopicSubscriber[2];
    var IpAddress = currentRecord.TopicSubscriber[3];
    var Status = 'Connected'; //If a topic is being written, then device must be Connected
    var LastConnDisconn = currentRecord.TopicSubscriber[0][0] + ' ' + currentRecord.TopicSubscriber[0][1];

    dBaseGetParams.Key = { 'PrincipalID': PrincipalID };

    dBasePutParams.Item =
    {
      'PrincipalID': PrincipalID,
      'LastIpAddress': IpAddress,
      'CurrentStatus': Status, //Connected or Disconnected
      'LastConnDisconnTime': LastConnDisconn, // Concatenating Date and Time'PubInTopic': PubInTopicName
      'TotalNumConnections': 0, // Placeholder
      'TotalNumDisconnections': 0, // Placeholder
      'PubInTopic': PubInTopicName,
      'PubInTopicNumMsgs': 0, // Placeholder
      'PubOutTopic': ' ', // Placeholder
      'PubOutTopicNumMsgs': 0 // Placeholder
    }
  }

  if (recordContents === 'PubOutTopic')
  {
    var PubOutTopicName = currentRecord.TopicName;
    PrincipalID = currentRecord.TopicSubscriber[2];
    var IpAddress = currentRecord.TopicSubscriber[3];
    var Status = 'Connected'; //If a topic is being written, then device must be Connected
    var LastConnDisconn = currentRecord.TopicSubscriber[0][0] + ' ' + currentRecord.TopicSubscriber[0][1];

    dBaseGetParams.Key = { 'PrincipalID': PrincipalID };

    dBasePutParams.Item =
    {
      'PrincipalID': PrincipalID,
      'LastIpAddress': IpAddress,
      'CurrentStatus': Status, //Connected or Disconnected
      'LastConnDisconnTime': LastConnDisconn, // Concatenating Date and Time
      'TotalNumConnections': 0, // Placeholder
      'TotalNumDisconnections': 0, // Placeholder
      'PubInTopic': ' ', // Placeholder
      'PubInTopicNumMsgs': 0, // Placeholder
      'PubOutTopic': PubOutTopicName,
      'PubOutTopicNumMsgs': 0 // Placeholder
    }
  }

  docClient.get(dBaseGetParams, (err, readRecord) =>
  {
    if (err) console.log(err);
    else if ((Object.keys(readRecord).length === 0) && (readRecord.constructor === Object))
    // The particular record is not found in the dBase, so enter it
    {

      // If Status is Connected then set TotalNumConnections to 1 since this is first record
      // If Status is Disonnected then set TotalNumDisconnections to 1 since this is first record

      if (recordContents === 'ConnInfo')
      {
        dBasePutParams.Item.PubInTopicNumMsgs = 0;
        dBasePutParams.Item.PubOutTopicNumMsgs = 0;

        if (Status === 'Connected')
        {
          dBasePutParams.Item.TotalNumConnections = 1;
          dBasePutParams.Item.TotalNumDisconnections = 0;
        }
        else
        {
          dBasePutParams.Item.TotalNumConnections = 0;
          dBasePutParams.Item.TotalNumDisconnections = 1;
        }
      }

      if (recordContents === 'PubInTopic')
      {
        // Even though it makes sense that a connection must exist for topics to be written / read
        // we are strictly updating TotalNumConnections only when explicitly receiving "Status Connect: SUCCESS".
        // This will simplify things when we read from CloudWatch

        dBasePutParams.Item.TotalNumConnections = 0;
        dBasePutParams.Item.TotalNumDisconnections = 0;
        dBasePutParams.Item.PubInTopicNumMsgs = 1;
        dBasePutParams.Item.PubOutTopicNumMsgs = 0;
      }

      if (recordContents === 'PubOutTopic')
      {
        // Even though it makes sense that a connection must exist for topics to be written / read
        // we are strictly updating TotalNumConnections only when explicitly receiving "Status Connect: SUCCESS".
        // This will simplify things when we read from CloudWatch

        dBasePutParams.Item.TotalNumConnections = 0;
        dBasePutParams.Item.TotalNumDisconnections = 0;
        dBasePutParams.Item.PubInTopicNumMsgs = 0;
        dBasePutParams.Item.PubOutTopicNumMsgs = 1;
      }

      docClient.put(dBasePutParams, (err, data) =>
      {
        console.log('Entering for first time: ', dBasePutParams);
        if (err) console.error('Unable to add item. Error JSON:', JSON.stringify(err, null, 2));
        else
        {
          getAndPutConnection(infoForDbase, recordNum + 1); // This function calls itself recursively
        }
      });
    }

    else
    {
      // First read TotalNumConnections, TotalNumDisconnections, PubInTopicNumMsgs and PubOutTopicNumMsgs.
      // The appropriate ones will be incremented as needed.
      dBasePutParams.Item.TotalNumConnections = readRecord.Item.TotalNumConnections;
      dBasePutParams.Item.TotalNumDisconnections = readRecord.Item.TotalNumDisconnections;
      dBasePutParams.Item.PubInTopicNumMsgs = readRecord.Item.PubInTopicNumMsgs;
      dBasePutParams.Item.PubOutTopicNumMsgs = readRecord.Item.PubOutTopicNumMsgs;

      // If Status is Connected then take TotalNumConnections for this PrincipalID in dBase and increment
      // If Status is Disonnected then take TotalNumDisconnections for this PrincipalID in dBase and decrement.
      if (recordContents === 'ConnInfo')
      {
        if (Status === 'Connected')
        {
          dBasePutParams.Item.TotalNumConnections++;
        }
        else
        {
          dBasePutParams.Item.TotalNumDisconnections++;
        }
      }

      else if (recordContents === 'PubInTopic')
      {
        dBasePutParams.Item.PubInTopicNumMsgs++;
      }

      else if (recordContents === 'PubOutTopic')
      {
        dBasePutParams.Item.PubOutTopicNumMsgs++;
      }

      docClient.put(dBasePutParams, (err, data) =>
      {
        console.log('Adding to existing: ', dBasePutParams);
        if (err) console.error('Unable to add item. Error JSON:', JSON.stringify(err, null, 2));
        else
        {
          getAndPutConnection(infoForDbase, recordNum + 1); // This function calls itself recursively
        }
      });
    }
  });
}
