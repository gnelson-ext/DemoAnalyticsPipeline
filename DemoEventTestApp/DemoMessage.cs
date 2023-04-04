using Azure.Messaging.EventHubs.Producer;
using Azure.Messaging.EventHubs;

namespace DemoEventTestClient
{
   /// <summary>
   /// This class represents a test event, which will connect to the specified Event Hub namespace and send 
   /// one, short message with an Avro file as the event body.
   /// </summary>
   internal class DemoMessage
   {
      public async Task<bool> SendTestEvent()
      {
         string? eventhubSas = string.Empty;

         try
         {
            eventhubSas = Environment.GetEnvironmentVariable("DEMO_AZURE_EVENTHUB_SAS");
         }
         catch (Exception)
         {
            Console.WriteLine("Environment variable 'DEMO_AZURE_EVENTHUB_SAS' was not found. Please add your EventHub's SaS connection string as an environment variable to continue.");
         }

         EventHubProducerClient producerClient = new EventHubProducerClient(eventhubSas, "demo-event");

         using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

         byte[] eventBody = File.ReadAllBytes("testdata.avro");
         if (!eventBatch.TryAdd(new EventData(eventBody)))
         {
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
