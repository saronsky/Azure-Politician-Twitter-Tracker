using System;
using System.Linq;
using FileHelpers;
using System.Data.SqlClient;   // System.Data.dll
using System.Collections.Generic;
using System.Net.Http;
using Newtonsoft.Json;
using Azure.Storage.Blobs;
using System.Text;
using System.IO;
using Azure.Storage.Blobs.Specialized;

namespace CreateAccountList
{

    public class Rootobject
    {
        public Data data { get; set; }
    }

    public class Data
    {
        public string username { get; set; }
        public string id { get; set; }
        public string name { get; set; }
        public string profile_image_url { get; set; }
        public Public_Metrics public_metrics { get; set; }
    }

    public class Public_Metrics
    {
        public int followers_count { get; set; }
        public int following_count { get; set; }
        public int tweet_count { get; set; }
        public int listed_count { get; set; }
    }



    [DelimitedRecord(",")]
    public class Congressperson
    {
        public string state;
        public string body;
        public string firstName;
        public string lastName;
        public string party;
        public string twitterHandle;

        [FieldHidden]
        public decimal dbID;

    }

    class InitalAccountsPopulate
    {
        //BLOB
        public static string blobConnectionString = "";
        private static AppendBlobClient accountsBlob = new AppendBlobClient(blobConnectionString, "twitterblob", "accounts");

        public static string usersResourceURL = "https://api.twitter.com/2/users/by/username/";
        private static string bearerToken = "";
        private static SqlConnectionStringBuilder cb = new SqlConnectionStringBuilder();
        private static SqlConnection connection;
        public static void Main()
        {
            //Initialize SQL Connection
            cb.DataSource = "governmenttwitter.database.windows.net";
            cb.UserID = "css436";
            cb.Password = "Program5";
            cb.InitialCatalog = "governmenttwitterdb";
            using (connection = new SqlConnection(cb.ConnectionString))
            {
                connection.Open();

                //Initialize Blob
                accountsBlob.CreateIfNotExists();

                //Parse Spreadsheet
                var engine = new FileHelperEngine<Congressperson>();

                // Iterate through spreadsheet
                var result = engine.ReadFile("C:\\Users\\Simon\\Desktop\\School\\CSS436 Cloud Computing\\Project5CSS436\\Database\\CreateAccountList\\TwitterListSenate.csv");
                foreach (Congressperson congressperson in result)
                {
                    uploadOfficialSQL(congressperson);
                    uploadOfficialBlob(congressperson);
                    getAccount(congressperson);
                }
            }
        }

        //Uploads Official object to SQL
        /*This method is used to save the offical objects to the SQL DB
         * Precondition: congressperson- the congressperson object containg the individuals data*/
        public static void uploadOfficialSQL(Congressperson congressperson)
        {
            string tsqlSourceCode = @"
INSERT INTO Official (firstName, lastName, body, party, state)
VALUES
    ('" + congressperson.firstName + "', '" + congressperson.lastName + "', '" + congressperson.body + "', '" + congressperson.party + "', '" + congressperson.state + "');" + "SELECT SCOPE_IDENTITY()";
            using (var command = new SqlCommand(tsqlSourceCode, connection))
            {
                congressperson.dbID = (decimal)command.ExecuteScalar();
                Console.WriteLine("DB ID of " + congressperson.firstName + " " + congressperson.lastName + " is " + congressperson.dbID);
            }


        }
        

        /*The method is used to save offical objects to the blob DB
         * Preconditions: congressperson- the congresspoerson object containt the individuals data*/
        public static void uploadOfficialBlob(Congressperson congressperson)
        {
            var content = Encoding.UTF8.GetBytes(congressperson.state + "," + congressperson.body + "," + congressperson.firstName + "," + congressperson.lastName + "," + congressperson.party + "," + congressperson.twitterHandle+"\n");
            using (var ms = new MemoryStream(content))
                accountsBlob.AppendBlock(ms);
        }

        //Uploads Account object to SQL
        /*This method is used to upload Account objects to the SQL DB
         * Precondition: congressperson- the congressperson object containg the individuals data*/
        public static void getAccount(Congressperson congressperson)
        {
            var client = new HttpClient();
            client.BaseAddress = new Uri(usersResourceURL);
            client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", bearerToken);
            string uri = congressperson.twitterHandle + "?user.fields=public_metrics,profile_image_url";
            string result = handleResponse(client, uri, -1);
            if (result.Length != 0)
            {
                string accountID = JsonConvert.DeserializeObject<Rootobject>(result).data.id;
                string name = JsonConvert.DeserializeObject<Rootobject>(result).data.name;
                int followerCount = JsonConvert.DeserializeObject<Rootobject>(result).data.public_metrics.followers_count;
                int tweetCount = JsonConvert.DeserializeObject<Rootobject>(result).data.public_metrics.tweet_count;
                string userName = JsonConvert.DeserializeObject<Rootobject>(result).data.username;
                string profileImageURL = JsonConvert.DeserializeObject<Rootobject>(result).data.profile_image_url;

                string tsqlSourceCode = @"
INSERT INTO Account (id, name, username, followers_count, tweet_count, officialID, profile_image_url)
VALUES
    ('" + accountID + "', '" + name + "', '" + userName + "', '" + followerCount.ToString() + "', '" + tweetCount.ToString() + "', '" + congressperson.dbID.ToString()+"', '" + profileImageURL + "');";
                using (var command = new SqlCommand(tsqlSourceCode, connection))
                {
                    command.ExecuteNonQuery();
                    Console.WriteLine("Added " + name + "s account");
                }
            }
        }

        /* This method is used to handle the responses by the API. Handles exponential backoff and failed requests
           Precondtion: client- HTTP client. uri- the header portion of the URL for the API request, retry- the number of seconds to wait
           Postcondition: string- a string of the JSON output*/
        static string handleResponse(HttpClient client, string uri, int retry)
        {
            if (retry > 0)
                System.Threading.Thread.Sleep(retry * 1000);
            HttpResponseMessage response = client.GetAsync(uri).Result;
            if (!response.IsSuccessStatusCode)
            {
                int statusCode = (int)response.StatusCode;
                if (statusCode / 100 == 5 && retry < 8)
                {
                    if (retry < 1)
                        retry++;
                    else
                        retry *= 2;
                    return handleResponse(client, uri, retry);
                }
                Console.WriteLine("API request failed. Status code: " + response.StatusCode);
                if (response.StatusCode == (System.Net.HttpStatusCode)429)
                {
                    Console.WriteLine("Waiting for 15 minutes. Too many requests");
                    System.Threading.Thread.Sleep(15 * 60 * 1000);
                    return handleResponse(client, uri, -1);
                }
                else
                    return "";
            }
            return response.Content.ReadAsStringAsync().Result;
        }
    }
}
