# Demo Azure Analytics Pipeline 📝  

## Overview 🚀  
This repository is a small scale demonstration of capturing analytics with Azure services to an Azure Data Lake Gen2 in Parquet format using an Event Hub, Stream Analytics Job, and an Azure Function. It is not meant to be a full scale application pipeline. Everything provided in this repository is simplified for maximum understanding of basic principles.

## Architecture
![Azure Architecture Diagram](/architecture.png)

The pipeline is split into two sub-pipelines. The first of these handles the raw, unaltered data being logged. The second conditions the data as-needed, saving it to a different location and deleting the original, raw file. Splitting the pipeline into two distinct phases allows for the tuning of performance, scale, and timing based on the different needs. In the first phase, it's important to ingest the data quickly without hinderance. The data comes in, the data is converted to Parquet, then saved. Done. For the second phase, it is less important to be processed inline with the first as we have the raw data and conditioned data takes second place. This second phase can be tuned to use less resources and happen less frequently to tightly control one's budget.

### Sub-Pipeline 1

The DemoEventTestApp sends a test event to an Event Hub instance, which receives the message and hands it off to a Stream Analytics Job. The Stream Analytics Job converts the JSON payload of the event message into Parquet, then saves it to the Azure Data Lake Storage Gen2 account in the "raw" folder.

### Sub-Pipeline 2

When the raw data is saved to the data lake, an Azure Function listening for a BlobTrigger will be executed. This Azure Function in the DemoConditioningApp project, will download the Parquet file, add a row group, save the conditioned file to the "conditioned" directory, then delete the original, raw file., save the conditioned file to the "conditioned" directory, then delete the original, raw file.

## Azure Resources

Below is a list of the resource group created for this demonstration:
![Azure Resource Group List](/services.png)

Please note that the Application Insights and Log Analytics Workspace resources were created automatically. The "democonditioningfxnstore" storage account is only required if your Azure Function will need to write to temporary storage space when it runs.
