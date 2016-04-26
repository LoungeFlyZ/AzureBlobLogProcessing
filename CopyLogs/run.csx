using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Net;

public static async Task<string> Run(TimerInfo myTimer, TextReader inputBlob, TextWriter outputBlob, TraceWriter log)
{    
    log.Verbose("Starting");

    //FROM: cloud show logs
    CloudStorageAccount sourceStorageAccount = new CloudStorageAccount(new StorageCredentials("STORAGE_ACCOUNT_NAME", "STORAGE_ACCOUNT_ACCESS_KEY"), true);
    
    //TO: cloud show storage 2 
    CloudStorageAccount destStorageAccount = new CloudStorageAccount(new StorageCredentials("STORAGE_ACCOUNT_NAME", "STORAGE_ACCOUNT_ACCESS_KEY"), true);
    
    CloudBlobClient sourceCloudBlobClient = sourceStorageAccount.CreateCloudBlobClient();
    CloudBlobClient destCloudBlobClient = destStorageAccount.CreateCloudBlobClient();

    // destination container
    string blobDestinationContainer = "cloudshowlogs/";
    CloudBlobContainer targetContainer = destCloudBlobClient.GetContainerReference(blobDestinationContainer);

    //
    // list matching blobs
    //
    
    //log.Verbose((inputBlob == null).ToString());
    log.Verbose($"Currently: {DateTime.UtcNow.ToString("O")}");
    
    DateTime d = DateTime.UtcNow;
    
    // read in the last DateTime we did a sync
    if(inputBlob != null)
    {
        var txt = inputBlob.ReadToEnd();
        
        if(!string.IsNullOrEmpty(txt))
        {
            d = DateTime.Parse(txt);
            log.Verbose($"SyncPoint: {d.ToString("O")}");
        }
        else
        {
            d = DateTime.UtcNow;
            log.Verbose($"Sync point file didnt have a date. Setting to now.");
        }
    }
    
    var now = DateTime.UtcNow;
    
    // calculate the difference in days
    var diff = now - d;
    log.Verbose($"Difference found of {Math.Ceiling(diff.TotalDays)} days from Now");
    
    // check each days logs
    var days = Math.Ceiling(diff.TotalDays); // always round up to the next day too
    for(int i = 0; i <= days; i++)
    {
        var add = new System.TimeSpan(0 - i, 0, 0, 0);
        var date = now.Add(add);

        string blobPrefix = "$logs" + "/blob/" + date.Year + "/" + date.Month.ToString("D2") + "/" + date.Day.ToString("D2") + "/";
        log.Verbose($"Scanning:  {blobPrefix}");
    
        IEnumerable<IListBlobItem> blobs = sourceCloudBlobClient.ListBlobs(blobPrefix, true);

        foreach(var item in blobs)
        {
            //log.Verbose($"Found blob: {item.Uri}");
            
            if (item is CloudBlockBlob)
            {
                CloudBlockBlob blockBlob = (CloudBlockBlob)item;
                
                var exists = BlobExists(destCloudBlobClient, blobDestinationContainer, blockBlob.Name, log);
                
                if(exists)
                {
                    //log.Verbose("skipping, already exists");
                }
                else
                {
                    log.Verbose($"Syncing: {item.Uri}");
                    
                    string sharedAccessUri = GetContainerSasUri(blockBlob);
                    
                    CloudBlockBlob sourceBlob = new CloudBlockBlob(new Uri(sharedAccessUri));
                    
                    //copy it
                    CloudBlockBlob targetBlob = targetContainer.GetBlockBlobReference(blockBlob.Name);
                    
                    await targetBlob.StartCopyAsync(sourceBlob);
                    
                    //log.Verbose("copied.");
                }            
            }
            else
            {
                log.Verbose("skipping. not a block blob");
            }
        }
    }
    
    // write out the current time to the sync point 
    outputBlob.Write(DateTime.UtcNow.ToString("O"));


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