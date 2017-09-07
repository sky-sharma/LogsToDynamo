const scanf = require('scanf')
const sscanf = require('scanf').sscanf;
var returnObject = {};
var returnData = [];
var returnObjects = [];

module.exports.parseMonitorWebUILog = (dataStr, searchStrings, keySearchPattern, dataSearchPatterns) =>
{
// Receives a string from a Monitor Log file,looks for searchString,
// and returns the items defined by searchPatterns.
console.log(dataSearchPatterns);
returnDataArray = [];
var rawDataRows = dataStr.toString().split('\n');
for (var i = 0; i < rawDataRows.length; i++)
{
  returnDataRow = [];

  if(rawDataRows[i].indexOf(searchStrings.Connection) > -1)
  // Check if dataRow contains Connection searchString
  {
    var returnObjectKey = sscanf(rawDataRows[i + 1], keySearchPattern);
    returnObject.key = returnObjectKey;

    dataSearchPatterns.forEach((dataSearchPattern) =>
    {
      // Parse out parameter values found after searchPattern
      // and create row of those values.

      returnData.push(sscanf(rawDataRows[i + 1], dataSearchPattern));
      // The first row contains the connection or
      // disconnection status. The second row contains the TraceID and other info.

      returnObject.Data = returnData;

      // Put returnObject into array
      returnObjects.push(returnObject);
    });
  } else if(rawDataRows[i].indexOf(searchStrings.Disconnection) > -1)
  // Check if dataRow contains Disconnection searchString
  {
    console.log(returnObjects.findIndex((returnObject, returnObjectKey) =>
    {
        return returnObject.key === returnObjectKey;
    }));
  }
  return returnObjects;
};
};
