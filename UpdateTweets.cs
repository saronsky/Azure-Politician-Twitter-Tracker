using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Net.Http;
using System.Text;
using Azure.Storage.Blobs.Specialized;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using Azure.AI.TextAnalytics;
using Azure;

namespace GovernmentTwitterFunction
{

    public class RootobjectUpdate
    {
        public Datum[] data { get; set; }
        public Meta meta { get; set; }
    }

    public class Meta
    {
        public string oldest_id { get; set; }
        public string newest_id { get; set; }
        public int result_count { get; set; }
        public string next_token { get; set; }
    }

    public class Datum
    {
        public string conversation_id { get; set; }
        public Entities entities { get; set; }
        public string text { get; set; }
        public string id { get; set; }
        public Public_Metrics public_metrics { get; set; }
        public string author_id { get; set; }
        public DateTime created_at { get; set; }
    }

    public class Entities
    {
        public Url[] urls { get; set; }
        public Annotation[] annotations { get; set; }
        public Mention[] mentions { get; set; }
        public Hashtag[] hashtags { get; set; }
    }

    public class Url
    {
        public int start { get; set; }
        public int end { get; set; }
        public string url { get; set; }
        public string expanded_url { get; set; }
        public string display_url { get; set; }
    }

    public class Annotation
    {
        public int start { get; set; }
        public int end { get; set; }
        public float probability { get; set; }
        public string type { get; set; }
        public string normalized_text { get; set; }
    }

    public class Mention
    {
        public int start { get; set; }
        public int end { get; set; }
        public string username { get; set; }
    }

    public class Hashtag
    {
        public int start { get; set; }
        public int end { get; set; }
        public string tag { get; set; }
    }

    public class Public_Metrics
    {
        public int retweet_count { get; set; }
        public int reply_count { get; set; }
        public int like_count { get; set; }
        public int quote_count { get; set; }
    }




    public class UpdateTweets
    {
        public const int maxReplies = 10;
        public const string startDate = "2021-03-15T00:00:00";
        private static ILogger logs;
        //BLOB
        public static string blobConnectionString = Environment.GetEnvironmentVariable("blobConnectionStringFromVault", EnvironmentVariableTarget.Process);
        private static AppendBlobClient tweetsBlob = new AppendBlobClient(blobConnectionString, "twitterblob", "tweets");

        //Twitter
        public static string usersResourceURL = "https://api.twitter.com/2/users/";
        public static string conversationResourceURL = "https://api.twitter.com/2/tweets/";
        private static string bearerToken = Environment.GetEnvironmentVariable("twitterBearerTokenFromVault", EnvironmentVariableTarget.Process);

        //SQL
        private static string sqlConnectionString = Environment.GetEnvironmentVariable("sqlConnectionStringFromVault", EnvironmentVariableTarget.Process);
        private static SqlConnection connection;

        //Text Analysis
        private static string cognitiveServicesKeyString = Environment.GetEnvironmentVariable("CognitiveServicesKeyFromVault", EnvironmentVariableTarget.Process);
        private static AzureKeyCredential cognitiveServicesKey = new AzureKeyCredential(cognitiveServicesKeyString);
        public static Uri cognitiveEndpoint = new Uri("https://topicanalyzer.cognitiveservices.azure.com/");

        [FunctionName("UpdateTweets")]
        public static void Run([TimerTrigger("0 0 * * * *")] TimerInfo myTimer, ILogger log)
        {
            logs = log;
            logs.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            using (connection = new SqlConnection(sqlConnectionString))
            {
                connection.Open();
                tweetsBlob.CreateIfNotExists();

                //Collect tweets for all Accounts in DB
                string tsqlSourceCode = @"SELECT id,last_updated FROM [dbo].[Account]";
                using (SqlDataAdapter myDataAdapter = new SqlDataAdapter(tsqlSourceCode, connection))
                {
                    DataTable dtResult = new DataTable();
                    myDataAdapter.Fill(dtResult);

                    foreach (DataRow row in dtResult.Rows)
                    {
                        string nextToken = null;
                        string accountID = row.Field<long>("id").ToString();
                        var lastUpdated = new DateTime();
                        var lastUpdatedNew = new DateTime();
                        if (row.ItemArray[1] != System.DBNull.Value)
                            lastUpdated = row.Field<DateTime>("last_updated");
                        var client = new HttpClient();
                        client.BaseAddress = new Uri(usersResourceURL);
                        client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", bearerToken);

                        //Iterate through whole timeline via nextToken
                        do
                        {
                            string uri = accountID + "/tweets?tweet.fields=created_at,entities,public_metrics,conversation_id,author_id&max_results=100";
                            if (lastUpdated.Ticks != 0)
                            {
                                lastUpdated.AddSeconds(1);
                                uri += ("&start_time=" + lastUpdated.ToString("yyyy-MM-ddTHH\\:mm\\:ss") + "Z");
                            }
                            else
                                uri += ("&start_time=" + startDate + "Z");
                            if (nextToken != null)
                                uri += ("&pagination_token=" + nextToken);
                            string result = handleResponse(client, uri, -1);
                            if (result.Length != 0)
                            {
                                nextToken = saveTweets(result, false, ref lastUpdatedNew, "-1");
                                tweetsBlob.AppendBlock(new MemoryStream(Encoding.UTF8.GetBytes(result)));
                            }
                            if (nextToken != null)
                                Console.WriteLine("Going to next token");

                        } while (nextToken != null);

                        //update account.last_updated 
                        updateAccount(lastUpdatedNew, accountID);
                    }
                    queryConversations();
                }
            }

        }

        /* This method is used to collect the replies for each government tweet*/
        private static void queryConversations()
        {
            Console.WriteLine("Getting Conversations");
            string queryString = "SELECT id, conversationID FROM(SELECT  id, conversationID,ROW_NUMBER() OVER(PARTITION BY conversationID ORDER BY ID DESC) rn FROM[dbo].[Tweet]) a WHERE rn = 1";

            SqlCommand command =
                new SqlCommand(queryString, connection);

            SqlDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                saveReplies((reader[1]).ToString(), reader[0].ToString());
            }
            reader.Close();
        }

        /*This method is used to save tweet replies to the SQL DB
         * Precondition: conversationID- the conversationID associated with the tweet from the twitterAPI*/
        private static void saveReplies(string conversationID, string tweetID)
        {
            var client = new HttpClient();
            client.BaseAddress = new Uri(conversationResourceURL);
            client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", bearerToken);
            string nextToken = null;
            string uri = "search/recent?query=conversation_id:" + conversationID + "&tweet.fields=created_at,entities,public_metrics,conversation_id&max_results=" + maxReplies.ToString() + "&";
            if (nextToken != null)
                uri += ("&next_token=" + nextToken);
            string result = handleResponse(client, uri, -1);
            if (result.Length != 0)
            {
                DateTime delete = new DateTime();
                nextToken = saveTweets(result, true, ref delete, tweetID);
                tweetsBlob.AppendBlock(new MemoryStream(Encoding.UTF8.GetBytes(result)));
            }
        }

        /*This method is used to save the new tweets to the SQL DB
         * Precondition: result- serialized JSON from twitter API
                         conversation- is the tweet a reply or an original
                         lastUpdated- the lastUpdated value of the account
           Postcondition: string- the next-token to be used to retrieve the rest of the results from twitter API*/
        private static string saveTweets(string result, bool conversation, ref DateTime lastUpdated, string tweetID)
        {
            Datum[] datum = JsonConvert.DeserializeObject<RootobjectUpdate>(result).data;
            string replies = "";
            if (datum != null)
            {
                foreach (Datum data in datum)
                {
                    string text = data.text;
                    string topics = AnalyzeTopics(text);
                    string hashtags = "";
                    if (data.entities != null && data.entities.hashtags != null)
                    {
                        foreach (Hashtag hashtag in data.entities.hashtags)
                        {
                            if (hashtags != "")
                                hashtags += ",";
                            hashtags += hashtag.tag;
                        }
                    }
                    string id = data.id;
                    var createdAt = data.created_at;
                    string tsqlSourceCodeAddTweet;
                    if (!conversation)
                        tsqlSourceCodeAddTweet = @"
INSERT INTO Tweet (id, text, created_at, authorID, hashtags, retweetCount, replyCount, likeCount, quoteCount, conversationID, topics)
VALUES (@id, @text, @createdAt, @accountID, @hashtags, @retweetCount, @replyCount, @likeCount, @quoteCount, @conversationID, @topics);";
                    else
                        tsqlSourceCodeAddTweet = @"
INSERT INTO Tweet (id, text, created_at, hashtags, retweetCount, replyCount, likeCount, quoteCount, conversationID)
VALUES (@id, @text, @createdAt, @hashtags, @retweetCount, @replyCount, @likeCount, @quoteCount, @conversationID);";
                    using (SqlCommand command = new SqlCommand(tsqlSourceCodeAddTweet, connection))
                    {
                        command.Parameters.Add("@id", SqlDbType.BigInt).Value = Convert.ToInt64(id);
                        command.Parameters.Add("@text", SqlDbType.Text).Value = text;
                        command.Parameters.Add("@createdAt", SqlDbType.DateTime2).Value = createdAt;
                        if (!conversation)
                        {
                            command.Parameters.Add("@accountID", SqlDbType.BigInt).Value = Convert.ToInt64(data.author_id);
                            command.Parameters.Add("@topics", SqlDbType.VarChar).Value = topics;
                        }
                        else
                            replies += (text + ".");
                        command.Parameters.Add("@hashtags", SqlDbType.Text).Value = hashtags;
                        command.Parameters.Add("@retweetCount", SqlDbType.Int).Value = data.public_metrics.retweet_count;
                        command.Parameters.Add("@replyCount", SqlDbType.Int).Value = data.public_metrics.reply_count;
                        command.Parameters.Add("@likeCount", SqlDbType.Int).Value = data.public_metrics.like_count;
                        command.Parameters.Add("@quoteCount", SqlDbType.Int).Value = data.public_metrics.quote_count;
                        command.Parameters.Add("@conversationID", SqlDbType.BigInt).Value = Convert.ToInt64(data.conversation_id);
                        int commandResult = -1;
                        try
                        {
                            commandResult = command.ExecuteNonQuery();
                            logs.LogInformation("Updated Tweets: " + id);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e + " " + text);
                        }
                        if (commandResult != -1 && (lastUpdated == null || lastUpdated <= createdAt))
                            lastUpdated = createdAt;
                        if (commandResult == -1)
                            Console.WriteLine("Tweet Submission Failure");
                    }
                }
            }
            if (conversation)
                AnalyzeSentiment(replies, tweetID);
            return JsonConvert.DeserializeObject<RootobjectUpdate>(result).meta.next_token;
        }


        /*This method is used to update each Accounts last_updated attribute in the SQL DB
         * Precondition: lastUpdated- the new last_updated value
                         accountID- the id associated with the account in the db*/
        static void updateAccount(DateTime lastUpdated, string accountID)
        {
            string tsqlSourceCodeUpdate = @"
UPDATE Account
SET last_updated=@lastupdated
WHERE id=@accountID";
            using (SqlCommand command = new SqlCommand(tsqlSourceCodeUpdate, connection))
            {
                command.Parameters.Add("@lastupdated", SqlDbType.DateTime2).Value = lastUpdated;
                command.Parameters.Add("@accountID", SqlDbType.BigInt).Value = accountID;
                int commandResult = command.ExecuteNonQuery();
                if (commandResult == -1)
                    Console.WriteLine("Update Failure");
            }
        }

        /*This method is used to update the sentiment value for each Official Tweet in the SQL DB
          Precondition: sentiment- the reply sentiment according to Azure cognitive serivices
                        tweetId- the tweet ID of the corresponding tweet being updated*/
        static void updateSentiment(string sentiment, string tweetID)
        {
            string tsqlSourceCodeUpdate = @"
UPDATE Tweet
SET sentiment=@sentiment
WHERE id=@tweetID";
            using (SqlCommand command = new SqlCommand(tsqlSourceCodeUpdate, connection))
            {
                command.Parameters.Add("@sentiment", SqlDbType.VarChar).Value = sentiment;
                command.Parameters.Add("@tweetID", SqlDbType.BigInt).Value = Convert.ToInt64(tweetID);
                int commandResult = command.ExecuteNonQuery();
                if (commandResult == -1)
                    Console.WriteLine("Update Failure");
            }
        }

        /*This method is used to analyze the topics in each tweet
          Precondition: text- the text of the tweet
          Postcondition: string- the topics*/
        private static string AnalyzeTopics(string text)
        {
            string topics = "";
            Console.WriteLine("Starting topic analysis on tweet");
            var client = new TextAnalyticsClient(cognitiveEndpoint, cognitiveServicesKey);
            try
            {
                var response = client.ExtractKeyPhrases(text);
                foreach (string keyphrase in response.Value)
                {
                    if (text.Contains(keyphrase))
                    {
                        if (topics.Length > 0)
                            topics += ",";
                        topics += (keyphrase);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            Console.WriteLine("Finished topic analysis");
            return topics;
        }

        /*This method is used to analyze the sentiment of the replies to the tweet
         Precondition: text- the text of the tweet
                       tweetId- the tweet ID of the corresponding tweet being updated*/
        private static void AnalyzeSentiment(string text, string tweetID)
        {
            Console.WriteLine("Starting sentiment analysis");
            var client = new TextAnalyticsClient(cognitiveEndpoint, cognitiveServicesKey);
            string sentiment = "";
            try
            {
                var response = client.AnalyzeSentiment(text);
                sentiment = response.Value.Sentiment.ToString();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            Console.WriteLine("Finished sentiment analysis");
            updateSentiment(sentiment, tweetID);
        }

        /* This method is used to handle the responses by the API. Handles exponential backoff and failed requests
         * Precondtion: client- HTTP client. uri- the header portion of the URL for the API request, retry- the number of seconds to wait
         * Postcondition: string- a string of the JSON output*/
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
                    //Waits 15 minutes if too many requests
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
