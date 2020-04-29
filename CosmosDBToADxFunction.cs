using System;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using System.IO;
using System.Threading;
using Kusto.Cloud.Platform.Utils;

namespace CosmosDBToADx
{
    public static class CosmosDBToADxFunction
    {
        [FunctionName("CosmosDBToADxFunction")]
        public static void Run([CosmosDBTrigger(

            databaseName: "data",
            collectionName: "device",
            ConnectionStringSetting = "rbdemocosmosdb_DOCUMENTDB",
            LeaseCollectionName = "leases", CreateLeaseCollectionIfNotExists = true)]IReadOnlyList<Document> input, ILogger log)
            {
                if (input != null && input.Count > 0)
                {
                    log.LogInformation("Documents modified " + input.Count);
                    log.LogInformation("First document Id " + input[0].Id);
                    //input[0].ToString() is the data that needs to be ingested.
                    var tenantId = System.Environment.GetEnvironmentVariable($"tenantId");
                    var applicationClientId = System.Environment.GetEnvironmentVariable($"applicationClientId");
                    var applicationKey = System.Environment.GetEnvironmentVariable($"applicationKey");
                    var serviceNameAndRegion = System.Environment.GetEnvironmentVariable($"serviceNameAndRegion");
                    var kustoDatabase = "device";
                    var kustoTableName = "Staging";
                    var mappingName = "deviceMapping";

                    var kustoIngestConnectionStringBuilder =
                        new KustoConnectionStringBuilder($"https://ingest-{serviceNameAndRegion}.kusto.windows.net")
                        {
                            FederatedSecurity = true,
                            InitialCatalog = kustoDatabase,
                            ApplicationClientId = applicationClientId,
                            ApplicationKey = applicationKey,
                            Authority = tenantId
                        };
                    using(var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(kustoIngestConnectionStringBuilder))
                    {
                        var properties = new KustoQueuedIngestionProperties(kustoDatabase , kustoTableName){
                            Format =  DataSourceFormat.json,
                            JSONMappingReference = mappingName,
                            ReportLevel = IngestionReportLevel.FailuresAndSuccesses,
                            ReportMethod = IngestionReportMethod.Queue
                        };
                        string inputValue = input[0].ToString().Replace("\r\n","");
                        //string inputValue = "{ \"Id\":\"1\", \"Timestamp\":\"2\", \"Message\":\"This is a test message\" }";
                        //object val = Newtonsoft.Json.JsonConvert.DeserializeObject(inputValue);
                        var stream = new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes(inputValue));
                        stream.Seek(0,SeekOrigin.Begin);
                        // Post ingestion message
                        ingestClient.IngestFromStream(stream, properties, leaveOpen: true);

                         // Wait and retrieve all notifications
                        //  - Actual duration should be decided based on the effective Ingestion Batching Policy set on the table/database
                        Thread.Sleep(10000);
                        var errors = ingestClient.GetAndDiscardTopIngestionFailures().GetAwaiter().GetResult();
                        var successes = ingestClient.GetAndDiscardTopIngestionSuccesses().GetAwaiter().GetResult();

                        errors.ForEach((f) => { Console.WriteLine($"Ingestion error: {f.Info.Details}"); });
                        successes.ForEach((s) => { Console.WriteLine($"Ingested: {s.Info.IngestionSourcePath}"); });
                    }
                    
                }
            }
    }
}
