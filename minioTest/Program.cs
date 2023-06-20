

using System;
using System.Diagnostics;
using System.Net;
using System.Net.Mail;
using System.Reactive.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading.Tasks.Dataflow;
using Microsoft.VisualBasic.FileIO;
using Minio;
using Minio.DataModel;
using Minio.DataModel.Replication;
using Minio.Exceptions;

namespace MinioTest;


//https://github.com/minio/minio-dotnet/tree/master

public static class Program
{
    private static async Task Main(string[] args)
    {
        var protocol = "http://";
        var MinioEndpoint = "192.168.1.161";
        var MinioPort = 9000;
        var MinioUser = "abc";
        var MinioPassword = "Aa123456*";
        var MinioUseSSL = false;
        var MinioAlias = "localminio";
        var bucketName = "dbucket";
        var objectName = "doge.jpg";
        // var filePath = "C:\\Users\\rc.gonzalez\\Downloads\\doge.jpg";
        var filePath = "..\\..\\..\\doge.jpg";
        var contentType = "image";

        string shellFile = "C:\\Users\\rc.gonzalez\\Downloads\\mc.exe";
        string shellArguments = "alias set " + MinioAlias +  " " + protocol + MinioEndpoint + ":" + MinioPort + " " + MinioUser + " " + MinioPassword;
        string shellArguments2 = "find " + MinioAlias + "/" + bucketName + " --newer-than 100m --older-than 10s";

        Console.BackgroundColor = ConsoleColor.Yellow;
        Console.ForegroundColor = ConsoleColor.Black;
        Console.WriteLine("RC MinIO Tester 20-06-2023");
        Console.ResetColor();

        // define minio client connection settings
        ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12
                                               | SecurityProtocolType.Tls12;

        using var minio = new MinioClient()
            .WithEndpoint(MinioEndpoint, MinioPort)
            .WithCredentials(MinioUser, MinioPassword)
            .WithSSL(MinioUseSSL)
            .Build();

        //list all buckets
        await getAllBuckets(minio);

        //  check if a bucket exists
        var bucketFound = await isBucketExists(minio, bucketName).ConfigureAwait(false);
        printSectionHeader("Check Bucket " + bucketName + " exists ...");
        Console.WriteLine("Bucket " + bucketName + " exists? = " + bucketFound);
        Console.WriteLine();

        //delete an existing bucket
        await deleteBucket(minio, "mybucketlist");

        //create bucket
        await createBucket(minio, "mybucketlist");

        // check if an object exists
        var objectFound = await checkObjectExists(minio, bucketName, objectName).ConfigureAwait(false);
        printSectionHeader("Check object " + bucketName + "/" + objectName );
        if (objectFound != null)
        {
            Type ot = objectFound.GetType();
            PropertyInfo[] pi = ot.GetProperties();
            foreach (PropertyInfo p in pi )
                System.Console.WriteLine(p.Name + " : " + p.GetValue(objectFound));
        }
        else
        {
            Console.WriteLine("object " + bucketName + "/" + objectName + " not found");
        }  
        Console.WriteLine();

        //get bucket notifications
        await getBucketNotifications(minio, bucketName);

        //delete an object
        await deleteObject(minio, bucketName, objectName);

        // Add objects to a bucket
       await uploadObject(minio, bucketName, objectName, filePath, contentType);

        //Create URL
       await createURL(minio, bucketName, objectName, 1200);

        //list objects in a bucket
        await listBucketObjects(minio, bucketName);

        // call minio console mc
       await executeShell(shellFile , shellArguments);


        await executeShell(shellFile, shellArguments2);

        Console.ReadLine();
    }

    private static void printSectionHeader(string headerName)
    {
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("===========================================================");
        Console.WriteLine(headerName);
        Console.WriteLine("===========================================================");
        Console.ResetColor();
    }
    private static Task<bool> isBucketExists(IMinioClient minio, string bucketName)
    {
        var bktExistsArgs = new BucketExistsArgs().WithBucket(bucketName);
        return minio.BucketExistsAsync(bktExistsArgs);
    }

    private async static Task getAllBuckets(MinioClient minio)
    {
        var listBuckets = await minio.ListBucketsAsync().ConfigureAwait(false);

        printSectionHeader("Listing All Buckets ... " + listBuckets.Buckets.Count + " buckets found");

        foreach (var bucket in listBuckets.Buckets)
            Console.WriteLine("bucket-name: " + bucket.Name + " , create-date: " + bucket.CreationDateDateTime);

        Console.WriteLine();
    }

    private async static Task deleteBucket (MinioClient minio, string bucketName)
    {
        printSectionHeader("Deleting bucket ... " + bucketName );
        if (await isBucketExists(minio, bucketName).ConfigureAwait(false))
        {
            var remBuckArgs = new RemoveBucketArgs().WithBucket(bucketName);
            await minio.RemoveBucketAsync(remBuckArgs).ConfigureAwait(false);
            Console.WriteLine("Bucket " + bucketName + " was succesfully deleted");
            Console.WriteLine();
        }
        else
        {
            Console.WriteLine("Bucket " + bucketName + " does not exist");
            Console.WriteLine();
        }
    }

    private async static Task createBucket (MinioClient minio, string bucketName)
    {
        printSectionHeader("Creating bucket ... " + bucketName);
        if  (! await isBucketExists(minio, bucketName).ConfigureAwait(false))
        {
            var mkBktArgs = new MakeBucketArgs().WithBucket(bucketName);
            await minio.MakeBucketAsync(mkBktArgs).ConfigureAwait(false);
            Console.WriteLine("Bucket " + bucketName + " was succesfully created");
            Console.WriteLine();
        }
        else
        {
            Console.WriteLine("Bucket " + bucketName + " already exists");
            Console.WriteLine();
        }
    }

    private  static async Task  listBucketObjects(MinioClient minio , string searchBucket)
    {
        ListObjectsArgs args1 = new ListObjectsArgs()
          .WithBucket(searchBucket);

        var observable = minio.ListObjectsAsync(args1);

        int i = 0;
        foreach (var myItem in observable)
            i= i + 1;

            printSectionHeader("Listing all objects in bucket ... " + searchBucket + " " + i + " items found ");
     
        var subscription =   observable.Subscribe(
            item => Console.WriteLine($"Object_name: {item.Key}"),
            ex => Console.WriteLine($"OnError: {ex}",
        () => Console.WriteLine($"Listed all objects in bucket {searchBucket} \n"))
        );
    }

    private  static async Task<ObjectStat> checkObjectExists(MinioClient minio , string bucketName, string objectName)
    {
        StatObjectArgs statObjectArgs = new StatObjectArgs()
                                        .WithBucket(bucketName)
                                        .WithObject(objectName);
       
        try 
        { 
            var OExists = await minio.StatObjectAsync(statObjectArgs).ConfigureAwait(false);
            return OExists;    
        }
           
        catch (Exception )
        { return null;  }

    }

    private async static Task deleteObject (MinioClient minio, string bucketName , string objectName)
    {
        RemoveObjectArgs rmArgs = new RemoveObjectArgs()
                              .WithBucket(bucketName)
                              .WithObject(objectName);
        printSectionHeader("Deleting object " + bucketName + "/" + objectName);

        var objectExists = await checkObjectExists(minio, bucketName, objectName);
        if (objectExists != null)
        {
            await minio.RemoveObjectAsync(rmArgs);
            Console.WriteLine("Object " + bucketName + "/" + objectName + " was deleted");
        }
        else
        {
            Console.WriteLine("Object " + bucketName + "/" + objectName + " does not exist");
        }
        Console.WriteLine();
    }

        private async static Task uploadObject(MinioClient minio, string bucketName , string objectName, string objectPath , string contentType)
    {
        try
        {
         //   var objectExists = await checkObjectExists(minio, bucketName, objectName);

            var putObjectArgs = new PutObjectArgs()
                .WithBucket(bucketName)
                .WithObject(objectName)
                .WithFileName(objectPath)                
                .WithContentType(contentType);

            await minio.PutObjectAsync(putObjectArgs).ConfigureAwait(false);

            printSectionHeader("Uploading object " + bucketName + "/" + objectName);

            Console.WriteLine("Successfully uploaded " + bucketName + "/" + objectName);
            Console.WriteLine();
        }
        catch (MinioException e)
        {
            Console.WriteLine("File Upload Error: {0}", e.Message);
        }
    }

    private async static Task createURL(MinioClient minio ,string bucketName , string objectName, int expirationTime)
    {
        var reqParams = new Dictionary<string, string>(StringComparer.Ordinal)
            { { "response-content-type", "image/jpeg" } };

        PresignedGetObjectArgs args = new PresignedGetObjectArgs()
            .WithBucket(bucketName)
            .WithObject(objectName)
            .WithHeaders(reqParams)
            .WithExpiry(expirationTime);

        string url = await minio.PresignedGetObjectAsync(args);

        printSectionHeader("Getting public URL for object " + bucketName + "/" + objectName);
        Console.WriteLine(url);
        Console.WriteLine();
    }

    private static  async Task getBucketNotifications (MinioClient minio, string bucketName) {       
         List <EventType> events = null;
        string prefix = "";
        string suffix = "";
      //  bool recursive = true;

        try
        {
            printSectionHeader("listening to bucket " + bucketName  + " notifications ...");
            Console.WriteLine();
            Console.WriteLine("Respose will be async , it will pop up later");
            events ??= new List<EventType> { EventType.ObjectCreatedAll };
            var args = new ListenBucketNotificationsArgs()
                .WithBucket(bucketName)
                .WithPrefix(prefix)
                .WithEvents(events)
                .WithSuffix(suffix);
            var observable = minio.ListenBucketNotificationsAsync(bucketName, events, prefix, suffix);
          
            var subscription = observable.Subscribe(
                notification => Console.WriteLine($"Notification: {notification.json}"),
                ex => Console.WriteLine($"OnError: {ex}"),
                () => Console.WriteLine("Stopped listening for bucket notifications\n"));
           
            // subscription.Dispose();
        }
        catch (Exception e)
        {
            Console.WriteLine($"[Bucket]  Exception: {e}");
        }
    }

    private static async Task executeShell(string shellFile,  string shellArgs)
    {
        // requires installing minio console client mc and adding it to the PATH
        // https://min.io/docs/minio/linux/reference/minio-mc.html
        printSectionHeader("testing minio shell (mc) ...");

        var psi = new ProcessStartInfo
        {
            FileName =  shellFile,
            Arguments = shellArgs,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        var proc = new Process
        {
            StartInfo = psi
        };

        proc.Start();

        Task.WaitAll(Task.Run(() =>
        {
            while (!proc.StandardOutput.EndOfStream)
            {
                var line = proc.StandardOutput.ReadLine();
                Console.WriteLine(line);
            }
        }), Task.Run(() =>
        {
            while (!proc.StandardError.EndOfStream)
            {
                var line = proc.StandardError.ReadLine();
                Console.WriteLine(line);
            }
        }));
        proc.WaitForExit();
        //Console.WriteLine(proc.ExitCode);
    }
}



