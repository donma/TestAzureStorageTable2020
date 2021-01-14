using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace TestAzureStorageTable2020
{
    class Program
    {
        static string _ConnectionString = "";

        static void Main(string[] args)
        {
            while (true)
            {
                Console.WriteLine("\r\n\r\n--------------------------------------------");
                Console.WriteLine("\r\nDONMA AZURE TABLE TEST 2020\r\n\r\n--------------------------------------------");
                Console.WriteLine("1>Add Table : table1.");
                Console.WriteLine("2>Delete Table.");
                Console.WriteLine("3>Upsert Data.");
                Console.WriteLine("4>List Datas.");
                Console.WriteLine("5>Delete One Data.");
                Console.WriteLine("6>Delete All PartitionKey: CLASSA.");
                Console.WriteLine("7>InsertOrMerge VS InsertOrReplace.");
                Console.WriteLine("8>Test Update Data by ETag.");
                Console.WriteLine("9>Search Data.");
                Console.WriteLine("10>Upsert And Get Data with Complex Type.");
                Console.WriteLine("11> Get Data with Complex Type.");

                Console.WriteLine("--------------------------------------------\r\n");

                Console.Write(">");
                var ans = Console.ReadLine();

                if (ans == "1")
                {
                    CreateTable("table1");
                }
                if (ans == "2")
                {
                    DropTable("table1");
                }

                if (ans == "3")
                {
                    AddData();
                }
                if (ans == "4")
                {
                    ListData();
                }
                if (ans == "5")
                {
                    DeleteOneData();
                }
                if (ans == "6")
                {
                    DeleteByPartitionKey();
                }
                if (ans == "7")
                {
                    InsertOrMergeVSInsertOrReplace();
                }
                if (ans == "8")
                {
                    UpdateDataByETag();
                }
                if (ans == "9")
                {
                    SearchData();
                }
                if (ans == "10")
                {
                    UpsertDataWithComplexData();
                }
                if (ans == "11")
                {
                    GetDatabySegmented();
                }
            }

            static void GetDatabySegmented()
            {


                var tableClient = CloudStorageAccount.Parse(_ConnectionString).CreateCloudTableClient(new TableClientConfiguration());
                var table = tableClient.GetTableReference("table1");

                string pkFilter = new TableQuery<User>()
                   .Where(TableQuery.GenerateFilterCondition("PartitionKey",
                     QueryComparisons.Equal, "CLASSA")).FilterString;

                string ageGreaterFilter = new TableQuery<User>()
                  .Where(TableQuery.GenerateFilterConditionForInt("Age",
                    QueryComparisons.GreaterThan, 100)).FilterString;

                string ageLessequalFilter = new TableQuery<User>()
                .Where(TableQuery.GenerateFilterConditionForInt("Age",
                  QueryComparisons.LessThanOrEqual, 2000)).FilterString;


                string combineAgeFilter = TableQuery.CombineFilters(ageGreaterFilter, TableOperators.And, ageLessequalFilter);
                string combinePKAndAgeFilter = TableQuery.CombineFilters(pkFilter, TableOperators.And, combineAgeFilter);

                TableQuery<User> query = new TableQuery<User>().Where(combinePKAndAgeFilter);


                //只選擇固定欄位回來
                query.SelectColumns = new List<string>();
                query.SelectColumns.Add("Age");
                query.OrderByDesc("Age");

                //Max:1000
                //超過 1000 會有 Exception.
                //One of the request inputs is not valid.
                query.TakeCount = 500;


                TableContinuationToken token = null;

                //紀錄資料總數
                var allCount = 0;

                //記錄分頁數
                var segmentCount = 0;
                do
                {

                    TableQuerySegment<User> segment = table.ExecuteQuerySegmented(query, token);

              

                    if (segment.Results.Count > 0)
                    {
                        segmentCount++;
                        Console.WriteLine();
                        Console.WriteLine("Segment Page {0}", segmentCount);
                    }

                    token = segment.ContinuationToken;

                    foreach (User entity in segment)
                    {
                        Console.WriteLine("\t Data: {0},{1},Age:{2}", entity.PartitionKey, entity.RowKey, entity.Age);
                        allCount++;
                    }

                    Console.WriteLine();
                }
                while (token != null);

                Console.WriteLine("Segment Page Count:" + segmentCount);
                Console.WriteLine("All Data Count:" + allCount);


            }



            static void UpsertDataWithComplexData()
            {
                var tableClient = CloudStorageAccount.Parse(_ConnectionString).CreateCloudTableClient(new TableClientConfiguration());

                var table = tableClient.GetTableReference("table1");

                // Add Data into Table.
                var sampleObject = new User("CLASSA", "DATAKEYSAMPLE")
                {
                    Name = "DONMASAMPLE",
                    Age = 99,
                    Create = new DateTime(2010, 1, 1).AddDays(1)
                };

                //Add Complex Data.
                sampleObject.Friends.Add(new User("CLASSB", "DATAKEYSAMPLE_FRIEND") { Age = 100, Name = "DATAKEYSAMPLE' FRIEND", Create = DateTime.Now });

                var upsertOperation = TableOperation.InsertOrReplace(sampleObject);
                var result = table.Execute(upsertOperation);

                //Read Data.

                var data1 = table.Execute(TableOperation.Retrieve<User>("CLASSA", "DATAKEYSAMPLE")).Result as User;
                Console.WriteLine(data1.Name + ":" + data1.Create);
                Console.WriteLine(data1.Friends[0].PartitionKey + "," + data1.Friends[0].RowKey + ":" + data1.Friends[0].Name + "," + data1.Friends[0].Create);
            }

            static void SearchData()
            {
                var tableClient = CloudStorageAccount.Parse(_ConnectionString).CreateCloudTableClient(new TableClientConfiguration());
                var table = tableClient.GetTableReference("table1");

                string pkFilter = new TableQuery<User>()
                   .Where(TableQuery.GenerateFilterCondition("PartitionKey",
                     QueryComparisons.Equal, "CLASSA")).FilterString;

                string ageGreaterFilter = new TableQuery<User>()
                  .Where(TableQuery.GenerateFilterConditionForInt("Age",
                    QueryComparisons.GreaterThan, 100)).FilterString;

                string ageLessequalFilter = new TableQuery<User>()
                .Where(TableQuery.GenerateFilterConditionForInt("Age",
                  QueryComparisons.LessThanOrEqual, 200)).FilterString;


                string combineAgeFilter = TableQuery.CombineFilters(ageGreaterFilter, TableOperators.And, ageLessequalFilter);
                string combinePKAndAgeFilter = TableQuery.CombineFilters(pkFilter, TableOperators.And, combineAgeFilter);

                TableQuery<User> query = new TableQuery<User>().Where(combinePKAndAgeFilter);


                //只選擇固定欄位回來
                query.SelectColumns = new List<string>();
                query.SelectColumns.Add("Age");
                var entities = table.ExecuteQuery<User>(query);

               
                Console.WriteLine("Search Age <=200 And > 100 Count :" + entities.Count());
                foreach (var data in entities)
                {
                    Console.Write(data.RowKey + ":" + data.Age + " ; ");
                }
            }

            static void UpdateDataByETag()
            {

                var tableClient = CloudStorageAccount.Parse(_ConnectionString).CreateCloudTableClient(new TableClientConfiguration());

                var table = tableClient.GetTableReference("table1");


                var data1 = table.Execute(TableOperation.Retrieve<User>("CLASSA", "DATAKEY1")).Result as User;
                var data2 = table.Execute(TableOperation.Retrieve<User>("CLASSA", "DATAKEY1")).Result as User;
                Console.WriteLine("Source Data :\r\n ");
                Console.WriteLine(JsonConvert.SerializeObject(data1));


                Console.WriteLine("");
                Console.WriteLine("Edit File And Replce Use data1");
                data1.Meta = "EDIT DATA1";
                try
                {
                    table.Execute(TableOperation.Replace(data1));
                    Console.WriteLine("SUCCESS!");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("EDIT DATA1 FAIL:" + ex.Message);
                }


                Console.WriteLine("");
                Console.WriteLine("Edit File And Replce Use data2 , Old ETag");

                data2.Meta = "EDIT DATA2";
                try
                {
                    table.Execute(TableOperation.Replace(data2));
                    Console.WriteLine("SUCCESS!");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("EDIT DATA2 FAIL , Old ETag:" + ex.Message);
                }


                Console.WriteLine("");
                Console.WriteLine("Edit File And Replce Use data2 , Etag=*");
                data2.Meta = "EDIT DATA2 - ETAG = * ";
                data2.ETag = "*";
                try
                {
                    table.Execute(TableOperation.Replace(data2));
                    Console.WriteLine("SUCCESS!");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("EDIT DATA2 , Etag=* FAIL:" + ex.Message);
                }



            }
            static void RetrieveData()
            {
                var tableClient = CloudStorageAccount.Parse(_ConnectionString).CreateCloudTableClient(new TableClientConfiguration());
                var table = tableClient.GetTableReference("table1");

                var data1 = table.Execute(TableOperation.Retrieve<User>("CLASSA", "DATAKEY1")).Result;
                Console.WriteLine(JsonConvert.SerializeObject(data1));
            }

            static void InsertOrMergeVSInsertOrReplace()
            {
                var tableClient = CloudStorageAccount.Parse(_ConnectionString).CreateCloudTableClient(new TableClientConfiguration());

                var table = tableClient.GetTableReference("table1");

                //InsertOrReplace
                var sampleObject = new User("CLASSA", "DATAKEY1")
                {
                    Name = "DONMA1-INSERTUPDATE",
                    Age = 1,
                    Meta = "META-INSERTUPDATE",
                    Create = new DateTime(2001, 1, 1).AddDays(1),
                    LastLoginDate = DateTime.Now
                };

                table.Execute(TableOperation.InsertOrReplace(sampleObject));


                //Read Data After InsertOrReplace
                Console.WriteLine("------ Read Data After InsertOrReplace ------");
                var data1 = table.Execute(TableOperation.Retrieve<User>("CLASSA", "DATAKEY1")).Result;
                Console.WriteLine(JsonConvert.SerializeObject(data1));

                Console.WriteLine("\r\n\r\n\r\n");

                //InsertOrMerge
                var sampleObject2 = new User("CLASSA", "DATAKEY1")
                {
                    Name = "DONMA1-INSERTORMERGE",
                    Age = null,
                    Meta = null,
                    Create = new DateTime(2020, 1, 1).AddDays(1),
                    LastLoginDate = null
                };

                table.Execute(TableOperation.InsertOrMerge(sampleObject2));


                //Read Data After InsertOrMerge
                Console.WriteLine("------ Read Data After InsertOrMerge ------");
                var data2 = table.Execute(TableOperation.Retrieve<User>("CLASSA", "DATAKEY1")).Result;
                Console.WriteLine(JsonConvert.SerializeObject(data2));

            }



            static IEnumerable<IEnumerable<T>> Split<T>(IEnumerable<T> source, int pageCount)
            {
                return source
                    .Select((x, i) => new { Index = i, Value = x })
                    .GroupBy(x => x.Index / pageCount)
                    .Select(x => x.Select(v => v.Value).ToList())
                    .ToList();
            }

            static void DeleteByPartitionKey()
            {
                var tableClient = CloudStorageAccount.Parse(_ConnectionString).CreateCloudTableClient(new TableClientConfiguration());

                var table = tableClient.GetTableReference("table1");

                var queryAllRowKeysByPK = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey",
                    QueryComparisons.Equal, "CLASSA")).Select(new[] { "RowKey" });

                //抓回所有 PartitionKey = CLASSA 的結果
                var entities = table.ExecuteQuery(queryAllRowKeysByPK);

                // 因為TableBatchOperation 不能加入超過100
                //所以要分組 每100個為一組
                var split100Entities = Split(entities, 100);
                foreach (var split100Entry in split100Entities)
                {
                    var batch = new TableBatchOperation();
                    //將100加入到 TableBatchOperation  中 
                    foreach (var wannaDeleteData in split100Entry)
                    {
                        batch.Add(TableOperation.Delete(wannaDeleteData));
                    }
                    try
                    {
                        table.ExecuteBatch(batch);

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error:" + ex.Message);
                    }
                }
            }


            static void DeleteOneData()
            {
                var tableClient = CloudStorageAccount.Parse(_ConnectionString).CreateCloudTableClient(new TableClientConfiguration());

                var table = tableClient.GetTableReference("table1");

                try
                {
                    var deleteReult = table.Execute(TableOperation.Delete(new TableEntity { RowKey = "DATAKEY11", PartitionKey = "CLASSA", ETag = "*" }));


                    Console.WriteLine("SUCCESS");

                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error:" + ex.Message);
                }
            }

            static void ListData()
            {
                var tableClient = CloudStorageAccount.Parse(_ConnectionString).CreateCloudTableClient(new TableClientConfiguration());

                var table = tableClient.GetTableReference("table1");

                var queryResult = table.ExecuteQuery(new TableQuery<User>(), null);

                var count = 0;
                foreach (var data in queryResult)
                {

                    Console.WriteLine(JsonConvert.SerializeObject(data));
                    count++;
                }
                Console.WriteLine("--");
                Console.WriteLine("DATA COUNT:" + count);

            }
            static void AddData()
            {
                var tableClient = CloudStorageAccount.Parse(_ConnectionString).CreateCloudTableClient(new TableClientConfiguration());

                var table = tableClient.GetTableReference("table1");
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                Parallel.For(1, 10001,
                      i =>
                      {
                          var sampleObject = new User("CLASSA", "DATAKEY" + i)
                          {
                              Name = "DONMA" + i,
                              Age = i,
                              Create = new DateTime(2010, 1, 1).AddDays(i)
                          };

                          var upsertOperation = TableOperation.InsertOrReplace(sampleObject);

                          var result = table.Execute(upsertOperation);

                          Console.WriteLine("PartitionKey:" + (result.Result as User).PartitionKey + "," +
                              "RowKey:" + (result.Result as User).RowKey + "," +
                              "ETag:" + result.Etag);
                      });

                Console.WriteLine(stopwatch.Elapsed);
            }


            static void DropTable(string tableName)
            {

                var storageAccount = CloudStorageAccount.Parse(_ConnectionString);
                var tableClient = storageAccount.CreateCloudTableClient(new TableClientConfiguration());

                var res = tableClient.GetTableReference(tableName).DeleteIfExists();
                if (res)
                {
                    Console.WriteLine("SUCCESS !!");
                }
                else
                {
                    Console.WriteLine("NO EXISTED TABLE.");
                }
            }
            static void CreateTable(string tableName)
            {

                //https://docs.microsoft.com/en-us/rest/api/storageservices/Delete-Table?redirectedfrom=MSDN
                //delete may use 40s , create after deleting should be waiting for 40s.

                var storageAccount = CloudStorageAccount.Parse(_ConnectionString);
                var tableClient = storageAccount.CreateCloudTableClient(new TableClientConfiguration());

                var table = tableClient.GetTableReference(tableName);
                if (table.CreateIfNotExists())
                {
                    Console.WriteLine("SUCCESS !!");
                }
                else
                {
                    Console.WriteLine("ALREADY EXISTED.");
                }
            }
        }
    }
}