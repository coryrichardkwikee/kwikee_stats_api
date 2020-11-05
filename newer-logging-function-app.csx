#r "Newtonsoft.Json"
#r "System.Data"
#r "System.Collections"
using System; // automatically imported, but we'll explicitly load it anyway
using System.IO; // automatically imported, but we'll explicitly load it anyway
using System.Net;
using System.Text;
//using System.Linq; // automatically imported, but we'll explicitly load it anyway
using System.Data.SqlClient;
// using System.Net.Http.Formatting;
using System.Configuration;
using System.Collections;
using System.Collections.Generic;  // automatically imported, but we'll explicitly load it anyway
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

public static Int64 insertInto(TraceWriter log, SqlConnection connection, string tableName, JObject data)
{
    Dictionary<string, string[]> tables = new Dictionary<string, string[]>() {
        {"image", new string[] {
        	"censhareAssetId", // should maybe be imageAssetId to match naming in product table
        	"viewCode",
        	"version",
        	"fileType",
        	"gs1Type",
        	"gs1Facing",
        	"gs1Angle",
        	"gs1State"
        } },
        {"event", new string[] {
            "dateTime",
            "userId", // not the censhare asset id
            "userEmailAddress",
            "system",
            "event",
            "productId", // not the censhare asset id
            "imageId", // not the censhare asset id
            "censhareCartId"
        } },
        {"product", new string[] {
        	"productAssetId", // not called censhareAssetId because this also has brand and mfr censhare asset ids in it
        	"gtin",
        	"brandAssetId",
        	"brandName",
        	"manufacturerAssetId",
        	"manufacturerName"
        } },
        {"user", new string[] {
            "firstName",
            "lastName",
            "emailAddress",
            "companyName",
            "censhareAssetId", // should maybe be userAssetId to match naming in product table
            "companyType",
            "address1",
            "address2",
            "city",
            "stateProvince",
            "zipPostal"
        } },
        {"combined", new string[] {
            // event
            "dateTime",
            "system",
            "event",
            "cartAssetId",

            // product
            "productAssetId",
            "gtin",
            "productName",
            "brandAssetId",
            "brandName",
            "manufacturerAssetId",
            "manufacturerName",

            // image
            "imageAssetId",
            "fileName",
            "viewCode",
            "version",
            "imageType",
            "fileType",
            "fileFormat",
            "gs1Type",
            "gs1Facing",
            "gs1Angle",
            "gs1State",

            // user
            "userAssetId",
            "firstName",
            "lastName",
            "emailAddress",
            "companyName",
            "companyType",
            "address1",
            "address2",
            "city",
            "stateProvince",
            "zipPostal"
        } }
    };

    SqlCommand command = new SqlCommand();
    command.Connection = connection;

    // build a list of only the fields that we received in JSON
    List<string> subset = new List<string>();
    foreach (string f in tables[tableName]) {
        if ( data[f] != null ) {
            subset.Add(f);
        }
    }

    if (subset.Count == 0) {
        throw new System.ArgumentException("No recognized fields found for "+tableName);
    }

    string sql = "INSERT INTO dbo.[" + tableName +
        "] (" +
        string.Join(", ", subset) + ") values (@" +
        string.Join(", @", subset) + ")"
        + ";SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    log.Info("SQL: " + sql);
    command.CommandText = sql;
    command.Prepare();

    // foreach (string f in tables[tableName]) {
    foreach (string f in subset) {
        log.Info("..f: " + f);
        command.Parameters.AddWithValue("@"+f, data[f].Value<string>());
    }
    log.Info("Getting ready to insert into " + tableName);

    Int64 pkid = (Int64)command.ExecuteScalar();

    log.Info("..inserted:" + pkid.ToString());
    // log.Info("..inserted");
    return pkid;
}

public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, TraceWriter log)
{
    log.Info("C# HTTP trigger function processed a request.");
    HttpStatusCode statusCode = HttpStatusCode.OK;
    dynamic resp = new JObject();
    resp.status = "success"; // default assumption
    resp.inserted = new JObject();
    JObject data = null;

    try
    {
        // Database connection string - this should be better placed at the Function settings level.
        var cnnString = "Server=kwikeecontentlog.database.windows.net;Database=kwikeeStatistics; User ID=schrappe;Password=B@mt18011994";
        using(var connection = new SqlConnection(cnnString))
        {
            connection.Open();
            log.Info("connection opened");

            // Get request body
            data = await req.Content.ReadAsAsync<JObject>();

            // if there is an event key in the JSON body, insert the record
            string[] tables = {"image","product","user","event","combined"};
            foreach (string table in tables) {
                if (data[table] != null) {
                    log.Info("sending "+table+" data");
                    // log.Info(data["event"].ToString());
                    Int64 pkid = insertInto(log, connection, table, data[table].Value<JObject>());
                    resp.inserted.Add(table, pkid);
                } // if
            } // foreach

            connection.Close();
        }
    }
    catch (Exception e)
    {
        log.Info("Exception: " + e.ToString() );
        statusCode = HttpStatusCode.BadRequest;
        resp.status = "error";
        resp.details = e.ToString();
    }

     return req.CreateResponse(statusCode, (JObject)resp, "application/json");

}
