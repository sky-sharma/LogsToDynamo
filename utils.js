const scanf = require('scanf')
const sscanf = require('scanf').sscanf;
var rawDataRows = [];
var connectionInfoFromAllRows = []; //2D Array of connection info. from all rows
var connectionInfoFromRowPlusStatus = []; //1D array of connection info. rom 1 row along with Bool indicating connection status

module.exports.parseLog = (dataStr, searchStrings, dataSearchPatterns) =>
{
  // Receives a string from a Monitor Log file,looks for searchString,
  // and returns the items defined by searchPatterns.

  connectionInfoFromAllRows = [];

  var numConnections = 0;
  var numDisconnections = 0;

  rawDataRows = dataStr.toString().split('\n');
  for (var i = 0; i < rawDataRows.length; i++)
  {
    // Check if dataRow contains either Connection or Disconnection searchString
    if(rawDataRows[i].indexOf(searchStrings.Connection) > -1)
    {
      connectionInfoFromRowPlusStatus = parseConnectionInfo(rawDataRows[i + 1], dataSearchPatterns);
      connectionInfoFromRowPlusStatus.push('Connected');
      connectionInfoFromRowPlusStatus.push(++numConnections);
      connectionInfoFromRowPlusStatus.push(numDisconnections);
    }
    else if(rawDataRows[i].indexOf(searchStrings.Disconnection) > -1)
    {
      connectionInfoFromRowPlusStatus = parseConnectionInfo(rawDataRows[i + 1], dataSearchPatterns);
      connectionInfoFromRowPlusStatus.push('Disconnected');
      connectionInfoFromRowPlusStatus.push(numConnections);
      connectionInfoFromRowPlusStatus.push(++numDisconnections);
    }
    connectionInfoFromAllRows.push(connectionInfoFromRowPlusStatus);
  };
  return connectionInfoFromAllRows;
};

function parseConnectionInfo(rowWithInfo, dataSearchPatterns)
{
  var connectionInfoFromRow = [];
  dataSearchPatterns.forEach((dataSearchPattern) =>
  {
    // Parse out parameter values found after searchPattern
    // and create row of those values.

    connectionInfoFromRow.push(sscanf(rowWithInfo, dataSearchPattern));
    // The first row contains the connection or
    // disconnection status. The second row contains the TraceID and other info.
  });

  return connectionInfoFromRow;
}
