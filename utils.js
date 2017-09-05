const scanf = require('scanf')
const sscanf = require('scanf').sscanf;
var returnDataArray = [];
var returnDataRows = [];

module.exports.parseMonitorLog = (dataStr, searchString, searchPatterns) => {
// Receives a string from a Monitor Log file,looks for searchString,
// and returns the items defined by searchPatterns.

returnDataArray = [];
var rawDataRows = dataStr.toString().split('\n');
for (var i = 0; i < rawDataRows.length; i++)
{
  returnDataRow = [];

  if(rawDataRows[i].indexOf(searchString) > -1)
  // Check if dataRow contains searchString
  {
    searchPatterns.forEach((searchPattern) => {

      // Parse out parameter values found after sarchPattern
      // and create row of those values.

      returnDataRow.push(sscanf(rawDataRows[i + 1], searchPattern));
      // The first row contains the connection or
      // disconnection status.  The second row contains the TraceID and other info.

    });

    // Put data row into 2D array
    returnDataArray.push(returnDataRow);
  }
}
return returnDataArray;
};
