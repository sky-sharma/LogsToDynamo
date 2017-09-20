const fs = require('fs');
const path = require('path');
const utils = require('./utils.js');
const aws = require('aws-sdk');

//globally
aws.config.paramValidation = false;

var s3 = new aws.S3();
var dynamodb = new aws.DynamoDB({ region: 'us-west-2', apiVersion: '2012-08-10' });
var docClient = new aws.DynamoDB.DocumentClient({ service: dynamodb });

//aws.config.update({ endpoint: 'https://s3.us-west-2.amazonaws.com' });

var params = {
  Bucket: 'connection-logs' /* required */};

  var searchStrings =
  {
    Connection: 'Connect Status: SUCCESS',
    Disconnection: 'Disconnect Status: SUCCESS',
    PublishIn: 'PublishIn Status: SUCCESS',
    PublishOut: 'PublishOut Status: SUCCESS',
    Subscribe: 'Subscribe Status: SUCCESS'
  };

var connInfoSearchPatterns = ['%s %s', 'TRACEID:%s', 'PRINCIPALID:%s', 'IpAddress: %s ', 'SourcePort: %s'];
var topicSearchPattern = 'TOPICNAME:%s ';

var infoThisFile = []; // Clear array of Connections collected from last file
var logFiles = [];
var logFileNum = 0;
var PrincipalID; // dBase Key field
var PubInTopicNumMsgs = '';
var PubOutTopicNumMsgs = '';
var SubscribeTopicName = '';

s3.listObjects(params, function(err, data)
{
  if (err) console.log(err, err.stack); //an error occurred
  else
  {
    logFiles = data.Contents;
    // console.log(logFiles);
    readS3AndGetPutConnection(logFiles, logFileNum++);
}});

function readS3AndGetPutConnection(logFiles, logFileIndex)
{
  // Recursive function to read s3 item, then put connections in dBase
  // This involves reading existing numbers of connections / disconnections
  // from the dBase and adding to the tallies.

  if (logFileIndex >= logFiles.length) return; // All done

  console.log('logFileIndex: ', logFileIndex);
  // console.log('logFiles: ', logFiles);

  var logFileName = logFiles[logFileIndex].Key;
  // console.log('logFile: ', logFileName);

  params.Key = logFileName;
  // console.log(params);
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
      'LastConnDisconnTime': LastConnDisconn // Concatenating Date and Time
      // 'TotalNumConnections': 0, // Placeholder
      // 'TotalNumDisconnections': 0, // Placeholder
      // 'PubInTopic': ' ', // Placeholder
      // 'PubInTopicNumMsgs': 0, // Placeholder
      // 'PubOutTopic': ' ', // Placeholder
      // 'PubOutTopicNumMsgs': 0 // Placeholder
    }
  }

  if (recordContents === 'SubscribeTopic')
  {
    console.log('recordContents: ', recordContents);
    var SubscribeTopicName = currentRecord.TopicName;
    PrincipalID = currentRecord.TopicSubscriber[2];
    IpAddress = currentRecord.TopicSubscriber[3];
    // Normally, if a topic is being written, then we
    // should consider the device to be Connected.
    // However in this case we are ONLY considering
    // a device conneted when the following is received:
    // "Connect Status: SUCCESS"
    Status = currentRecord.TopicSubscriber[5];
    LastConnDisconn = currentRecord.TopicSubscriber[0][0] + ' ' + currentRecord.TopicSubscriber[0][1];

    dBaseGetParams.Key = { 'PrincipalID': PrincipalID };

    dBasePutParams.Item =
    {
      'PrincipalID': PrincipalID,
      'LastIpAddress': IpAddress,
      'CurrentStatus': Status, //Connected or Disconnected
      'LastConnDisconnTime': LastConnDisconn // Concatenating Date and Time
    }
  }

  if (recordContents === 'PubInTopic')
  {
    var PubInTopicName = currentRecord.TopicName;
    PrincipalID = currentRecord.TopicSubscriber[2];
    IpAddress = currentRecord.TopicSubscriber[3];
    // Normally, if a topic is being written, then we
    // should consider the device to be Connected.
    // However in this case we are ONLY considering
    // a device conneted when the following is received:
    // "Connect Status: SUCCESS"
    Status = currentRecord.TopicSubscriber[5];
    LastConnDisconn = currentRecord.TopicSubscriber[0][0] + ' ' + currentRecord.TopicSubscriber[0][1];
    PubInTopicNumMsgs = `PubIn: ${PubInTopicName}`; // Use Template string to create field name rather than field value.

    dBaseGetParams.Key = { 'PrincipalID': PrincipalID };

    dBasePutParams.Item =
    {
      'PrincipalID': PrincipalID,
      'LastIpAddress': IpAddress,
      'CurrentStatus': Status, //Connected or Disconnected
      'LastConnDisconnTime': LastConnDisconn // Concatenating Date and Time
    }
  }

  if (recordContents === 'PubOutTopic')
  {
    var PubOutTopicName = currentRecord.TopicName;
    PrincipalID = currentRecord.TopicSubscriber[2];
    IpAddress = currentRecord.TopicSubscriber[3];
    // Normally, if a topic is being written, then we
    // should consider the device to be Connected.
    // However in this case we are ONLY considering
    // a device conneted when the following is received:
    // "Connect Status: SUCCESS"
    Status = currentRecord.TopicSubscriber[5];
    LastConnDisconn = currentRecord.TopicSubscriber[0][0] + ' ' + currentRecord.TopicSubscriber[0][1];
    PubOutTopicNumMsgs = `PubOut: ${PubOutTopicName}`; // Use Template string to create field name rather than field value.

    dBaseGetParams.Key = { 'PrincipalID': PrincipalID };

    dBasePutParams.Item =
    {
      'PrincipalID': PrincipalID,
      'LastIpAddress': IpAddress,
      'CurrentStatus': Status, //Connected or Disconnected
      'LastConnDisconnTime': LastConnDisconn // Concatenating Date and Time
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
//        dBasePutParams.Item.PubInTopicNumMsgs = 0;
//        dBasePutParams.Item.PubOutTopicNumMsgs = 0;

        if (Status === 'Connected')
        {
          dBasePutParams.Item.TotalNumConnections = 1;
          dBasePutParams.Item.TotalNumDisconnections = 0;
        }
        else if (Status === 'Disconnected')
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

          dBasePutParams.Item[PubInTopicNumMsgs] = 1;
      }

      if (recordContents === 'SubscribeTopic')
      {
        console.log('dBasePutParams: ', dBasePutParams);
      }

      if (recordContents === 'PubOutTopic')
      {
        // Even though it makes sense that a connection must exist for topics to be written / read
        // we are strictly updating TotalNumConnections only when explicitly receiving "Status Connect: SUCCESS".
        // This will simplify things when we read from CloudWatch

          dBasePutParams.Item[PubOutTopicNumMsgs] = 1;
      }

      docClient.put(dBasePutParams, (err, data) =>
      {
        // console.log('Entering for first time: ', dBasePutParams);
        if (err) console.error('Unable to add item. Error JSON:', JSON.stringify(err, null, 2));
        else
        {
          getAndPutConnection(infoForDbase, recordNum + 1); // This function calls itself recursively
        }
      });
    }

    else
    {
      // First read TotalNumConnections and TotalNumDisconnections.
      // The appropriate ones will be incremented as needed.
      // If those fields are not present set them to 0.

      readRecord.Item.TotalNumConnections ? (dBasePutParams.Item.TotalNumConnections = readRecord.Item.TotalNumConnections) : (dBasePutParams.Item.TotalNumConnections = 0);
      readRecord.Item.TotalNumDisconnections ? (dBasePutParams.Item.TotalNumDisconnections = readRecord.Item.TotalNumDisconnections) : (dBasePutParams.Item.TotalNumDisconnections = 0);

      // If no Status info. in recently parsed record, use the last Status from dBase.
      if (dBasePutParams.Item.CurrentStatus === undefined) dBasePutParams.Item.CurrentStatus = readRecord.Item.CurrentStatus;

      // If Status is Connected then take TotalNumConnections for this PrincipalID in dBase and increment
      // If Status is Disonnected then take TotalNumDisconnections for this PrincipalID in dBase and decrement.
      if (recordContents === 'ConnInfo')
      {
        if (Status === 'Connected')
        {
          (dBasePutParams.Item.TotalNumConnections)++;
        }
        else
        {
          (dBasePutParams.Item.TotalNumDisconnections)++;
        }
      }

      else if (recordContents === 'Subscribe')
      {
        dBasePutParams.Item[PubInTopicNumMsgs].SubscriptionState = 'Subscribed';
      }

      else if (recordContents === 'PubInTopic')
      {
        readRecord.Item[PubInTopicNumMsgs] ? (dBasePutParams.Item[PubInTopicNumMsgs] = readRecord.Item[PubInTopicNumMsgs]) : (dBasePutParams.Item[PubInTopicNumMsgs] = 0);
        (dBasePutParams.Item[PubInTopicNumMsgs])++;
      }

      else if (recordContents === 'PubOutTopic')
      {
        readRecord.Item[PubOutTopicNumMsgs] ? (dBasePutParams.Item[PubOutTopicNumMsgs] = readRecord.Item[PubOutTopicNumMsgs]) : (dBasePutParams.Item[PubOutTopicNumMsgs] = 0);
        (dBasePutParams.Item[PubOutTopicNumMsgs])++;
      }

      // Missing fields from readRecord are the ones added using dBasePutParams
      var newRecord = {};
      newRecord.Item = Object.assign({}, readRecord.Item, dBasePutParams.Item);
      newRecord.TableName = dBasePutParams.TableName;

      // console.log(newRecord);

      docClient.put((newRecord), (err, data) =>
      {
        // console.log('Adding to existing: ', dBasePutParams);
        if (err) console.error('Unable to add item. Error JSON:', JSON.stringify(err, null, 2));
        else
        {
          getAndPutConnection(infoForDbase, recordNum + 1); // This function calls itself recursively
        }
      });
    }
  });
}
