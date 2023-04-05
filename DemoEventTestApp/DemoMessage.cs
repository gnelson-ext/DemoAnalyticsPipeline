using Azure.Messaging.EventHubs.Producer;
using Azure.Messaging.EventHubs;
using Parquet.Thrift;

namespace DemoEventTestClient
{
   /// <summary>
   /// This class represents a test event, which will connect to the specified Event Hub namespace and send 
   /// one, short message with JSON as the event body.
   /// </summary>
   internal class DemoMessage
   {
      public async Task<bool> SendTestEvent()
      {
         //
         // For simplicity, the demo will use SAS to connect. This is not best practice. Azure AD
         // would be preferred. To run the demo, simply add your EventHub SAS connection string
         // as an environment variable called 'DEMO_AZURE_EVENTHUB_SAS'.
         //

         string? eventhubSas = string.Empty;

         try
         {
            eventhubSas = Environment.GetEnvironmentVariable("DEMO_AZURE_EVENTHUB_SAS");
         }
         catch (Exception)
         {
            Console.WriteLine("Environment variable 'DEMO_AZURE_EVENTHUB_SAS' was not found. Please add your EventHub's SaS connection string as an environment variable to continue.");
         }

         //
         // Establish a connection with the event hub, then read in the test data for the
         // message body and send it.
         //

         EventHubProducerClient producerClient = new EventHubProducerClient(eventhubSas, "demo-event");
         using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

         try
         {
            string fileData = File.ReadAllText("testdata.json");
            EventData eventData = new EventData(System.Text.Encoding.UTF8.GetBytes(fileData));
            eventData.ContentType = "application/json";

            if (!eventBatch.TryAdd(eventData))
            {
               return false;
            }
         }
         catch (Exception)
         {
            Console.WriteLine("Error: Could not read data from the test input file.");
            return false;
         }

         try
         {
            await producerClient.SendAsync(eventBatch);
         }
         catch (Exception)
         {
            Console.WriteLine("Error: Unable to send batch!");
            return false;
         }
         finally
         {
            await producerClient.DisposeAsync();
         }

         Console.WriteLine("Success: Sent");
         return true;
      }
   }
}
