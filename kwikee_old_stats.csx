using System;
using System.IO;
using System.Net;
using System.Text;
using System.Data.SqlClient;
using System.Configuration;
using Newtonsoft.Json;
using Dapper;

// This C# function is triggered by new EventHub messages
// It retrieves the message (coming from API calls), formats the content and pushed it
// into Azure SQL tables that are used by PowerBI to provide Kwikee content usage reports
// It then checks if there is information about the GTIN (product name, brand and manufacturer data) in a separate SQL table
// If not, it queries censhare for data on the GTIN, parses the returning JSON object and pushed it into the SQL table.
// Bruno Schrappe – June/July 2018
static string RetrieveAsset(string code, string key, string resource, TraceWriter log)
{
    string stringUrl = String.Join(code, $"https://cms-api.kwikee.com/ws/rest/service/assets/asset;censhare:resource-key={resource}/transform;key={key}=", ";model=standard/json");
    log.Info(stringUrl);
    WebRequest request = WebRequest.Create(stringUrl);
    ((HttpWebRequest)request).UserAgent = ".NET Framework Example Client";
    // Censhare credentials.
    NetworkCredential myCred = new NetworkCredential("bruno", "dea1801");
    request.Credentials = myCred;
    // Get the response.
    WebResponse response = request.GetResponse();
    // Get the stream containing content returned by the server.
    Stream dataStream = response.GetResponseStream();
    // Open the stream using a StreamReader for easy access.
    StreamReader reader = new StreamReader(dataStream);
    // Read the content.
    string responseFromServer = reader.ReadToEnd();
    // Display the content.
    log.Info(responseFromServer);
    // Clean up the streams and the response.
    reader.Close();
    response.Close();
    return responseFromServer;
}

static bool LookupAssetByCode(string code, SqlConnection connection, TraceWriter log)
{
    string sqlString = "SELECT gtin FROM gtinData WHERE gtin = '" + code + "'";
    log.Info(sqlString);

    SqlCommand cmd = new SqlCommand(sqlString, connection);

   string gtinCode = (string)cmd.ExecuteScalar();
    if (gtinCode == code)
    {
        log.Info("GTIN already exists.");
        return true;
    }
    else
    {
        return false;
    }
}

static bool LookupImageByCode(string code, SqlConnection connection, TraceWriter log)
{


    // Check whether that image asset ID data is on the SQL database table or not. If not, data will be retrieved from censhare and pushed into the table...
    string sqlString = "SELECT gtin FROM imageData WHERE imageId = " + code;
    log.Info(sqlString);

    SqlCommand cmd = new SqlCommand(sqlString, connection);


    string imageCode = (string)cmd.ExecuteScalar();

    if (imageCode != null)
    {
        log.Info("Image data already exists.");
        return true;
    }
    else
    {
        return false;
    }
}

public static void Run(string myEventHubMessage, TraceWriter log)
{
    log.Info($"C# Event Hub trigger function processed a message: {myEventHubMessage}");

    try
    {
        // Database connection string - this should be better placed at the Function settings level.
        var cnnString = "Server=kwikeecontentlog.database.windows.net;Database=kwikeeContentLog; User ID=schrappe;Password=B@mt18011994";

        using (var connection = new SqlConnection(cnnString))
        {
            if (!(myEventHubMessage.StartsWith("KDE")))
            {
                connection.Open();

                // Split the message into separate strings to be pushed into the database
                string[] words = myEventHubMessage.Split(',');



                string logDate = words[0];
                string logProd = words[1];
                string logUser = words[2];
                string logUsId = words[3];
                string logCode = words[4];
                string logCTyp = words[5];

                if (logCTyp == "GTIN")
                {
                    var executionString = $"INSERT INTO azureApiLog(date,apiMethod,userEmail,userId,gtin) VALUES (CONVERT(DATETIME,'{logDate}',101),'{logProd}','{logUser}',{logUsId},'{logCode}')";
                    // Insert a database row into the GTIN table
                    connection.Execute(executionString);
                    log.Info("Log added to database into the GTIN table successfully!");

                    if (!(LookupAssetByCode(logCode, connection, log)))
                    {
                        log.Info("Nothing there... let's populate that empty database");
                        // Retrieving information from censhare to populate the GTIN database:
                        // Create a request for the URL.
                        string responseFromServer = RetrieveAsset(logCode, "logGtinData;gtin", "logGtinData", log);

                        // Deserialize the freaking response
                        dynamic parsed = JsonConvert.DeserializeObject(responseFromServer);
                        int assetId = parsed.id;
                        string productName = parsed.name;
                        log.Info(productName);
                        int brandId = parsed.brandData.id;
                        string brandName = parsed.brandData.name;
                        log.Info(brandName);
                        int manufacturerId = parsed.brandData.parentMfgData.id;
                        string manufacturerName = parsed.brandData.parentMfgData.name;
                        log.Info(manufacturerName);

                        // Nice. Now let's push the freaking thing into the GTIN data table:
                        // var gtinDataString = $"INSERT INTO gtinData(gtin,assetId,productName,brandId,brandName,manufacturerId,manufacturerName) VALUES (" + logCode + ", " + assetId + ", " + productName +", " +brandId +", " +brandName +", " +manufacturerId + ", " +manufacturerName + ")";
                        string gtinDataString = string.Format($"INSERT INTO gtinData(gtin,assetId,productName,brandId,brandName,manufacturerId,manufacturerName) VALUES ('{{0}}',{{1}},'{{2}}',{{3}},'{{4}}',{{5}},'{{6}}')", logCode, assetId, productName.Replace("'", "''"), brandId, brandName.Replace("'", "''"), manufacturerId, manufacturerName.Replace("'", "''"));

                        log.Info(gtinDataString);
                        // Insert a database row into the GTIN table
                        connection.Execute(gtinDataString);
                        log.Info("Log added to database into the GTIN data table successfully!");
                        connection.Close();
                    }
                }
                else if (logCTyp == "URL")
                {
                    string[] urlComponents = logCode.Split('/');
                    string imageId = urlComponents[0];
                    log.Info($"THIS S THE ID STRING {imageId}");
                    var executionString = $"INSERT INTO azureApiImageLog(date,apiMethod,userEmail,userId,imageId) VALUES (CONVERT(DATETIME,'{logDate}',101),'{logProd}','{logUser}',{logUsId},'{imageId}')";
                    // Insert a database row into the Image table
                    connection.Execute(executionString);
                    log.Info("Log added to database into the Image Asset table successfully!");

                    if (!(LookupImageByCode(imageId, connection, log)))
                    {
                        log.Info("Nothing there on the image table... let's populate that empty database");
                        // Retrieving information from censhare to populate the Image database:
                        // Create a request for the URL.
                        string responseFromServer = RetrieveAsset(imageId, "logImageData;imageId", "logImageData", log);



                        // Deserialize the freaking response
                        dynamic parsed = JsonConvert.DeserializeObject(responseFromServer);
                        string imageName = parsed.name;
                        string viewCode = parsed.viewCode;
                        string parentGtin = parsed.parentGtin;

                        // Nice. Now let's push the freaking thing into the image data table:
                        string imageDataString = string.Format($"INSERT INTO imageData(imageId, gtin, viewCode, name) VALUES ({{0}},'{{1}}','{{2}}','{{3}}')", imageId, parentGtin, viewCode, imageName.Replace("'", "''"));

                        log.Info(imageDataString);
                        // Insert a database row into the GTIN table
                        connection.Execute(imageDataString);
                        log.Info("Image additional data included successfully!");
                        connection.Close();
                    }

                }
                else
                {
                    log.Info("The log message is not properly formatted!");
                }
            }
            else
            {
                log.Info("PROCESS MESSAGE FROM KDE - IN NEW FORMAT");

                string[] keys = myEventHubMessage.Split(';');
                string logDate = DateTime.Now.ToString("M/dd/yyyy HH:mm:ss tt");
                string logKey = keys[0];
                string logMethod = keys[1];
                string logTarget = keys[2];
                string logCodeType = keys[3];
                string logCodes = keys[4];
                string logEmail = keys[5];
                string[] codes = logCodes.Split(",".ToCharArray());

                connection.Open();

                switch (logCodeType)
                {
                    case "ASSETOBJ":
                        log.Info("KDE ASSET");

                        for (int i = 0; i < codes.Length; i++)
                        {
                            string code = codes[i];





                            var executionString = $"INSERT INTO azureApiLog(date,apiMethod,userEmail,userId,gtin) VALUES (CONVERT(DATETIME,'{logDate}',101),'{logMethod}','{logEmail}',0,'{code}')";
                            // Insert a database row into the GTIN table
                            connection.Execute(executionString);
                            log.Info("Log added to database into the GTIN table successfully!");

                            if (!(LookupAssetByCode(code, connection, log)))
                            {
                                log.Info("Nothing there... let's populate that empty database");
                                // Retrieving information from censhare to populate the GTIN database:
                                // Create a request for the URL.
                                string responseFromServer = RetrieveAsset(code, "logGtinData;gtin", "logGtinData", log);

                                // Deserialize the freaking response
                                dynamic parsed = JsonConvert.DeserializeObject(responseFromServer);
                                int assetId = parsed.id;
                                string productName = parsed.name;
                                log.Info(productName);
                                int brandId = parsed.brandData.id;
                                string brandName = parsed.brandData.name;
                                log.Info(brandName);
                                int manufacturerId = parsed.brandData.parentMfgData.id;
                                string manufacturerName = parsed.brandData.parentMfgData.name;
                                log.Info(manufacturerName);

                                // Nice. Now let's push the freaking thing into the GTIN data table:
                                // var gtinDataString = $"INSERT INTO gtinData(gtin,assetId,productName,brandId,brandName,manufacturerId,manufacturerName) VALUES (" + logCode + ", " + assetId + ", " + productName +", " +brandId +", " +brandName +", " +manufacturerId + ", " +manufacturerName + ")";
                                string gtinDataString = string.Format($"INSERT INTO gtinData(gtin,assetId,productName,brandId,brandName,manufacturerId,manufacturerName) VALUES ('{{0}}',{{1}},'{{2}}',{{3}},'{{4}}',{{5}},'{{6}}')", code, assetId, productName.Replace("'", "''"), brandId, brandName.Replace("'", "''"), manufacturerId, manufacturerName.Replace("'", "''"));

                                log.Info(gtinDataString);
                                // Insert a database row into the GTIN table
                                connection.Execute(gtinDataString);
                                log.Info("Log added to database into the GTIN data table successfully!");
                                connection.Close();
                            }
                        }
                        break;
                    case "GTINOBJ":
                        log.Info("KDE GTIN");

                        for (int i = 0; i < codes.Length; i++)
                        {
                            string code = codes[i];





                            var executionString = $"INSERT INTO azureApiLog(date,apiMethod,userEmail,userId,gtin) VALUES (CONVERT(DATETIME,'{logDate}',101),'{logMethod}','{logEmail}',0,'{code}')";
                            // Insert a database row into the GTIN table
                            connection.Execute(executionString);
                            log.Info("Log added to database into the GTIN table successfully!");

                            if (!(LookupAssetByCode(code, connection, log)))
                            {
                                log.Info("Nothing there... let's populate that empty database");
                                // Retrieving information from censhare to populate the GTIN database:
                                // Create a request for the URL.
                                string responseFromServer = RetrieveAsset(code, "logGtinData;gtin", "logGtinData", log);

                                // Deserialize the freaking response
                                dynamic parsed = JsonConvert.DeserializeObject(responseFromServer);
                                int assetId = parsed.id;
                                string productName = parsed.name;
                                log.Info(productName);
                                int brandId = parsed.brandData.id;
                                string brandName = parsed.brandData.name;
                                log.Info(brandName);
                                int manufacturerId = parsed.brandData.parentMfgData.id;
                                string manufacturerName = parsed.brandData.parentMfgData.name;
                                log.Info(manufacturerName);

                                // Nice. Now let's push the freaking thing into the GTIN data table:
                                // var gtinDataString = $"INSERT INTO gtinData(gtin,assetId,productName,brandId,brandName,manufacturerId,manufacturerName) VALUES (" + logCode + ", " + assetId + ", " + productName +", " +brandId +", " +brandName +", " +manufacturerId + ", " +manufacturerName + ")";
                                string gtinDataString = string.Format($"INSERT INTO gtinData(gtin,assetId,productName,brandId,brandName,manufacturerId,manufacturerName) VALUES ('{{0}}',{{1}},'{{2}}',{{3}},'{{4}}',{{5}},'{{6}}')", code, assetId, productName.Replace("'", "''"), brandId, brandName.Replace("'", "''"), manufacturerId, manufacturerName.Replace("'", "''"));

                                log.Info(gtinDataString);
                                // Insert a database row into the GTIN table
                                connection.Execute(gtinDataString);
                                log.Info("Log added to database into the GTIN data table successfully!");
                                connection.Close();
                            }
                        }
                        break;
                    case "IMAGEOBJ":

                        log.Info("KDE IMAGE");


                        for (int i = 0; i < codes.Length; i++)
                        {
                            string[] codeObj = codes[i].Split(":".ToCharArray());
                            log.Info(codeObj[0]);
                            string assetId = codeObj[0];
                            string assetType = codeObj[1];

                            if (!(LookupImageByCode(assetId, connection, log)))
                            {
                                string responseFromServer = RetrieveAsset(assetId, "logImageData;imageId", "logImageData", log);

                                dynamic parsed = JsonConvert.DeserializeObject(responseFromServer);
                                string imageName = parsed.name;
                                string viewCode = parsed.viewCode;
                                string parentGtin = parsed.parentGtin;

                                // Nice. Now let's push the freaking thing into the image data table:
                                string imageDataString = string.Format($"INSERT INTO imageData(imageId, gtin, viewCode, name) VALUES ({{0}},'{{1}}','{{2}}','{{3}}')", assetId, parentGtin, viewCode, imageName.Replace("'", "''"));

                                log.Info(imageDataString);


                                // Insert a database row into the GTIN table
                                connection.Execute(imageDataString);
                                log.Info("Image additional data included successfully!");


                                log.Info($"THIS IS THE SERVER RESULT: {imageName}, {viewCode}, {parentGtin}");
                            }


                            var executionString = $"INSERT INTO azureApiImageLog(date,apiMethod,userEmail,userId,imageId) VALUES (CONVERT(DATETIME,'{logDate}',101),'{logMethod}','{logEmail}',0,'{assetId}')";
                            // Insert a database row into the Image table
                            connection.Execute(executionString);
                            log.Info("Log added to database into the Image Asset table successfully!");

                        }
                        break;
                    default:
                        log.Info("Default case");
                        break;
                }

                connection.Close();
            }
        }
    }
    catch (Exception e)
    {
        log.Info($"Something weird happened! {e}");
    }
    return;
}
