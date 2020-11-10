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
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Primitives;

public static Int64 insertInto(ILogger log, SqlConnection connection, string tableName, JObject data)
{
    Dictionary<string, string[]> tables = new Dictionary<string, string[]>() {
        {"image", new string[] {
        	"image_asset_id",
            "order_asset_id",
            "cart_asset_id",
            "promotion",
            "view_code",
            "image_version",
            "image_type",
            "gs1_code_string",
            "gs1_type",
            "gs1_facing",
            "gs1_angle",
            "gs1_state",
            "special_purpose",
            "language",
            "sequence",
            "file_name",
            "file_type",
            "expiry_date",
            "end_date",
            "sequence_number",
            "variant"
        } },
        {"event", new string[] {
            "user_email",
            "event_id",
            "event_type",
            "event_action",
            "product_asset_id",
            "image_asset_id",
            "kde_export_id",
            "system",
            "time_stamp",
            "censhare_order_id",
            "order_asset_id",
            "cart_asset_id"
        } },
        {"product", new string[] {
        	"product_asset_id",
            "gtin",
            "product_name",
            "product_version",
            "product_record_version",
            "product_record_variant",
            "variant_description",
            "cpv_reason_code",
            "cpv_identifier",
            "cpv_description",
            "mfr_id",
            "mfr_name",
            "client_id",
            "client_name",
            "brand_id",
            "brand_name",
        } },
        {"user", new string[] {
            "user_email",
            "user_asset_id",
            "company_name",
            "company_type",
            "first_name",
            "last_name",
            "address_1",
            "address_2",
            "city",
            "state",
            "zip",
            "brand_portal_user_id",
            "kwikeesystems_user_id"
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

    log.LogInformation("SQL: " + sql);
    command.CommandText = sql;
    command.Prepare();

    // foreach (string f in tables[tableName]) {
    foreach (string f in subset) {
        log.LogInformation("..f: " + f);
        command.Parameters.AddWithValue("@"+f, data[f].Value<string>());
    }
    log.LogInformation("Getting ready to insert into " + tableName);

    var obj = command.ExecuteScalar();
    Int64 pkid;
    if (obj == DBNull.Value)
    {
        pkid = (Int64)0;
    }
    else
    {
        pkid = (Int64)obj;
    }
    // Int64 pkid = (Int64)command.ExecuteScalar();

    log.LogInformation("..inserted:" + pkid.ToString());
    // log.LogInformation("..inserted");
    return pkid;
}

public static async Task<IActionResult> Run(HttpRequest req, ILogger log)
{
    log.LogInformation("C# HTTP trigger function processed a request.");
    HttpStatusCode statusCode = HttpStatusCode.OK;
    dynamic resp = new JObject();
    resp.status = "success"; // default assumption
    resp.inserted = new JObject();
    // JObject data = null;

    try
    {
        // Database connection string - this should be better placed at the Function settings level.
        var cnnString = "Server=kwikeecontentlog.database.windows.net;Database=Kwikee_Stats; User ID=schrappe;Password=B@mt18011994";
        using(var connection = new SqlConnection(cnnString))
        {
            connection.Open();
            log.LogInformation("connection opened");

            // Get request body
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            log.LogInformation("test" + requestBody);
            JObject data = JsonConvert.DeserializeObject<JObject>(requestBody);

            // if there is an event key in the JSON body, insert the record
            string[] tables = {"image","product","user","event"};
            foreach (string table in tables) {
                if (data[table] != null) {
                    log.LogInformation("sending "+table+" data");
                    // log.LogInformation(data["event"].ToString());
                    Int64 pkid = insertInto(log, connection, table, data[table].Value<JObject>());
                    resp.inserted.Add(table, pkid);
                } // if
            } // foreach

            connection.Close();
        }
    }
    catch (Exception e)
    {
        log.LogInformation("Exception: " + e.ToString() );
        statusCode = HttpStatusCode.BadRequest;
        resp.status = "error";
        resp.details = e.ToString();
    }
    log.LogInformation("status code: " + statusCode );
    return new ObjectResult(resp) {
        StatusCode = (int)statusCode,
    };
}
