using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Configuration;
using Parquet;
using System.Collections.Generic;
using Parquet.Schema;
using Parquet.Data;

namespace DemoConditioningApp
{
   //
   // DemoConditioningFxn
   //
   // Description:
   //    A demonstration of an Azure Function that will be triggered to run when a file
   //    has been uploaded to a specific directory in an Azure Data Lake Gen2 Storage
   //    account. It will then take that data, read it, condition it, and upload it
   //    to the conditioned directory. The original, raw data is then deleted to prevent
   //    it from being accessed again.
   //

   public class DemoConditioningFxn
   {
      // Reference to the configuration for reading in the Azure storage account name and key.
      private readonly IConfiguration _configuration;

      /// <summary>
      /// Constructor
      /// </summary>
      /// <param name="configuration">Dependency-injected reference to the app configuration.</param>
      public DemoConditioningFxn(IConfiguration configuration)
      {
         _configuration = configuration;
      }

      /// <summary>
      /// Main App Function method
      /// </summary>
      /// <param name="req">HttpRequest defining the data that is used by the function.</param>
      /// <param name="log">Logger</param>
      /// <returns></returns>
      [FunctionName("condition")]
      public async Task Run([BlobTrigger("demo-metrics/raw/{filename}", Connection = "datalakestr")] Stream eventDetails, string filename, ILogger log)
      {
         // Debug logging.
         log.LogInformation($"C# Blob trigger function Processed blob\n Name:{filename} \n Size: {eventDetails.Length} Bytes");

         //
         // Establish a handle to the data lake's file system as well as the raw and conditioned file directories.
         //

         DataLakeServiceClient client = GetDataLakeServiceClient();
         var fileSystemClient = client.GetFileSystemClient("demo-metrics");
         var rawDirectoryClient = fileSystemClient.GetDirectoryClient("raw");
         var conditionedDirectoryClient = fileSystemClient.GetDirectoryClient("conditioned");

         //
         // Take the data that was uploaded to the raw directory and condition it.
         //

         // Get the file's data from the data lake.
         DataLakeFileClient rawFileClient = rawDirectoryClient.GetFileClient(filename);
         using (Stream rawFileStream = await rawFileClient.OpenReadAsync())
         {
            //
            // From the streamed data lake file data, create a temporary local file and copy it over.
            // This will be a writeable stream.
            //

            using (var tempFileStream = File.Create("temp.parquet"))
            {
               rawFileStream.Seek(0, SeekOrigin.Begin);
               rawFileStream.CopyTo(tempFileStream);

               // Create the data lake file in the conditioned directory.
               DataLakeFileClient conditionedFileClient = await conditionedDirectoryClient.CreateFileAsync(filename);

               //
               // Read the Parquet file to get the fields.
               // Note: This is using Parquet.Net, an MIT-licensed open source library.
               //

               ParquetSchema schema;
               Dictionary<DataField, DataColumn> originalData = new Dictionary<DataField, DataColumn>();
               tempFileStream.Position = 0;
               using (ParquetReader reader = await ParquetReader.CreateAsync(tempFileStream))
               {
                  schema = reader.Schema;

                  for (int i = 0; i < reader.RowGroupCount; ++i)
                  {
                     using (ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(i))
                     {
                        foreach (DataField df in reader.Schema.GetDataFields())
                        {
                           DataColumn columnData = await rowGroupReader.ReadColumnAsync(df);
                           originalData.Add(df, columnData);
                        }
                     }
                  }
               }

               //
               // Now write out the data, only mutating one of the columns for demonstration purposes.
               //

               tempFileStream.Position = 0;
               using (ParquetWriter writer = await ParquetWriter.CreateAsync(schema, tempFileStream, append: true))
               {
                  using (ParquetRowGroupWriter rgw = writer.CreateRowGroup())
                  {
                     int count = 0;
                     DataField[] fields = schema.GetDataFields();
                     foreach (var field in fields)
                     {
                        DataColumn column;
                        if (field.Name.Equals("body", StringComparison.OrdinalIgnoreCase))
                        {
                           column = new DataColumn(field, new Int64[] { 1, 2, 3, 4 });
                        }
                        else
                        {
                           column = originalData.GetValueOrDefault(field);
                        }

                        await rgw.WriteColumnAsync(column);
                        ++count;
                     }
                  }
               }

               //
               // Upload the conditioned data to the conditioned directory, then remove the original file from raw.
               //

               tempFileStream.Position = 0;
               long fileSize = tempFileStream.Length;
               try
               {
                  await conditionedFileClient.AppendAsync(tempFileStream, offset: 0);
                  await conditionedFileClient.FlushAsync(position: fileSize);
               }
               catch (Exception ex)
               {
                  log.LogInformation($"Unable to append conditioned data to file handle: {ex.Message}");
               }
            }
         }

         //
         // Remove the original file from the raw directory.
         //

         try
         {
            await rawDirectoryClient.DeleteFileAsync(filename);
         }
         catch (Exception ex)
         {
            log.LogInformation($"Unable to delete raw file: {ex.Message}");
         }
      }

      /// <summary>
      /// Helper method to create a client connection to the data lake service.
      /// </summary>
      /// <returns>Connection to the data lake service.</returns>
      public DataLakeServiceClient GetDataLakeServiceClient()
      {
         //
         // THIS IS NOT USING BEST PRACTICES
         //
         //    It is not advised to use the account key credentials for authentication.
         //    It is preferable to use Azure AD. This code is simplified for demonstration
         //    purposes ONLY.
         //

         return new DataLakeServiceClient(_configuration["datalakestr"]);
      }
   }
}
