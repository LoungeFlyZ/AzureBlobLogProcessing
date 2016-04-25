using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Net;

public static async Task<string> Run(string input, TraceWriter log)
{    
    
    log.Verbose("Starting ");
    
    //FROM: cloud show logs
    CloudStorageAccount sourceStorageAccount = new CloudStorageAccount(new StorageCredentials("STORAGE_ACCOUNT_NAME", "STORAGE_ACCOUNT_ACCESS_KEY"), true);
    
    //TO: cloud show storage 2 
    CloudStorageAccount destStorageAccount = new CloudStorageAccount(new StorageCredentials("STORAGE_ACCOUNT_NAME", "STORAGE_ACCOUNT_ACCESS_KEY"), true);
    
    CloudBlobClient sourceCloudBlobClient = sourceStorageAccount.CreateCloudBlobClient();
    CloudBlobClient destCloudBlobClient = destStorageAccount.CreateCloudBlobClient();
    
    log.Verbose("Getting destination container");
    
    // destination container
    string blobDestinationContainer = "cloudshowlogs/";
    CloudBlobContainer targetContainer = destCloudBlobClient.GetContainerReference(blobDestinationContainer);

    log.Verbose("Getting blobs");
    
    // list matching blobs
    string blobPrefix = "$logs" + "/";
    IEnumerable<IListBlobItem> blobs = sourceCloudBlobClient.ListBlobs(blobPrefix, true);

    foreach(var item in blobs)
    {
        log.Verbose($"Found blob: {item.Uri}");
        
        if (item is CloudBlockBlob)
        {
            CloudBlockBlob blockBlob = (CloudBlockBlob)item;
            
            var exists = BlobExists(destCloudBlobClient, blobDestinationContainer, blockBlob.Name, log);
            
            if(exists)
            {
                log.Verbose("skipping, already exists");
            }
            else
            {
                log.Verbose("copying.");
                
                string sharedAccessUri = GetContainerSasUri(blockBlob);
                CloudBlockBlob sourceBlob = new CloudBlockBlob(new Uri(sharedAccessUri));
                
                //copy it
                CloudBlockBlob targetBlob = targetContainer.GetBlockBlobReference(blockBlob.Name);
                await targetBlob.StartCopyAsync(sourceBlob);
            
                log.Verbose("copied.");
            }            
        }
        else
        {
            log.Verbose("skipping. not a block blob");
        }
    }

    return "Done";
}

public static bool BlobExists(CloudBlobClient client, string containerName, string key, TraceWriter log)
{
     return client.GetContainerReference(containerName)
                  .GetBlockBlobReference(key)
                  .Exists();  
}

static string GetContainerSasUri(CloudBlockBlob blob)
{
    SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy();
    
    sasConstraints.SharedAccessStartTime = DateTime.UtcNow.AddMinutes(-5);
    sasConstraints.SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24);
    sasConstraints.Permissions = SharedAccessBlobPermissions.Read;

    //Generate the shared access signature on the container, setting the constraints directly on the signature.
    string sasBlobToken = blob.GetSharedAccessSignature(sasConstraints);

    //Return the URI string for the container, including the SAS token.
    return blob.Uri + sasBlobToken;
}