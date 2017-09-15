const scanf = require('scanf')
const sscanf = require('scanf').sscanf;
var rawDataRows = [];
var connectionInfoFromRow = []; //1D array of connection info. from 1 row
var connectionInfoFromRowPlusStatus = []; //1D array of connection info. rom 1 row along with String indicating connection status
var infoFromAllRows = []; //2D Array of connection info. from all rows

module.exports.parseLog = (dataStr, searchStrings, connInfoSearchPatterns, topicSearchPattern) =>
{
  // Receives a string from a Monitor Log file,looks for searchString,
  // and returns the items defined by searchPatterns.

  var numConnections = 0;
  var numDisconnections = 0;
  connectionInfoFromAllRows = [];

  rawDataRows = dataStr.toString().split('\n');

  for (var i = 0; i < rawDataRows.length; i++)
  {
    connectionInfoFromRow = [];
    connectionInfoFromRowPlusStatus = [];

    // Check if dataRow contains either Connection or Disconnection searchString
    if(rawDataRows[i].indexOf(searchStrings.Connection) > -1)
    {
      connectionInfoFromRowPlusStatus = parseConnectionInfo(rawDataRows[i + 1], connInfoSearchPatterns);
      connectionInfoFromRowPlusStatus.push('Connected');
      // connectionInfoFromRowPlusStatus.push(++numConnections);
      // connectionInfoFromRowPlusStatus.push(numDisconnections);
      infoFromAllRows.push({Contents: 'ConnInfo', ConnInfo: connectionInfoFromRowPlusStatus});
    }
    else if(rawDataRows[i].indexOf(searchStrings.Disconnection) > -1)
    {
      connectionInfoFromRowPlusStatus = parseConnectionInfo(rawDataRows[i + 1], connInfoSearchPatterns);
      connectionInfoFromRowPlusStatus.push('Disconnected');
      // connectionInfoFromRowPlusStatus.push(numConnections);
      // connectionInfoFromRowPlusStatus.push(++numDisconnections);
      infoFromAllRows.push({Contents: 'ConnInfo', ConnInfo: connectionInfoFromRowPlusStatus});
    }
    else if(rawDataRows[i].indexOf(searchStrings.PublishIn) > -1)
    {
      infoFromAllRows.push(
        {
          Contents: 'PubInTopic',
          TopicName: sscanf(rawDataRows[i], topicSearchPattern),
          TopicSubscriber: parseConnectionInfo(rawDataRows[i + 1], connInfoSearchPatterns)
        });
    }
    else if(rawDataRows[i].indexOf(searchStrings.PublishOut) > -1)
    {
      infoFromAllRows.push({
        Contents: 'PubOutTopic',
        TopicName: sscanf(rawDataRows[i], topicSearchPattern),
        TopicSubscriber: parseConnectionInfo(rawDataRows[i + 1], connInfoSearchPatterns)
      });
    }
  };
  return infoFromAllRows;
};

function parseConnectionInfo(rowWithInfo, connInfoSearchPatterns)
{
  var connectionInfoFromRow = [];
  connInfoSearchPatterns.forEach((connInfoSearchPattern) =>
  {
    // Parse out parameter values found after searchPattern
    // and create row of those values.

    connectionInfoFromRow.push(sscanf(rowWithInfo, connInfoSearchPattern));
    // The first row contains the connection or
    // disconnection status. The second row contains the TraceID and other info.
  });

  return connectionInfoFromRow;
}
