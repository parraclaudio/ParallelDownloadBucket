using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Google.Apis.Download;
using Google.Cloud.Storage.V1;

namespace ChunkReadFile
{
    class Program
    {
        private class Range
        {
            public long Start { get; set; }
            public long End { get; set; }
        }
        
        static void Main(string[] args)
        {
            ParallelDownload().GetAwaiter().GetResult();
            
            Console.WriteLine("FIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIM");
            Console.ReadKey();
        }

        private static async Task SimpleDownload()
        {
            var progress = new Progress<IDownloadProgress>(
                p => Console.WriteLine($"bytes: {p.BytesDownloaded}, status: {p.Status}")
            );
            
            var gcpClient = await StorageClient.CreateAsync();
            var gcpObject = await gcpClient.GetObjectAsync("", "");

            var tempFilePath = $"single_download";
            using (var fileStream = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write,
                       FileShare.Write))
            {
                gcpClient.DownloadObject("", "", fileStream, null, progress);
            }
        }
        
        private static async Task ParallelDownload(int numberOfParallelDownloads = 0)
        {
            var destinationFilePath = "hello";
            
            if (numberOfParallelDownloads <= 0)
            {
                numberOfParallelDownloads = Environment.ProcessorCount;
            }

            if (File.Exists(destinationFilePath))
            {
                File.Delete(destinationFilePath);
            }
            
            var tempFilesDictionary = new ConcurrentDictionary<int, string>();
            var gcpClient = await StorageClient.CreateAsync();
            var gcpObject = await gcpClient.GetObjectAsync("", "");
            var responseLength = (long)gcpObject.Size;

            var readRanges = new List<Range>();
            for ( var chunk = 0; chunk < numberOfParallelDownloads - 1; chunk++)
            {
                var range = new Range()
                {
                    Start = chunk * (responseLength / numberOfParallelDownloads),
                    End = ((chunk + 1) * (responseLength / numberOfParallelDownloads)) - 1
                };
                readRanges.Add(range);
            }

            readRanges.Add(new Range()  
            {  
                Start = readRanges.Any() ? readRanges.Last().End + 1 : 0,  
                End = responseLength - 1  
            });  

            var index = 0;
            Parallel.ForEach(readRanges,
                new ParallelOptions() {MaxDegreeOfParallelism = numberOfParallelDownloads},
                readRange =>
                {
                    var progress = new Progress<IDownloadProgress>(
                        p => Console.WriteLine($"bytes: {p.BytesDownloaded}, status: {p.Status}")
                    );
                    
                    var opt = new DownloadObjectOptions
                    {
                        ChunkSize = 2048, 
                        Range = new RangeHeaderValue()
                        {
                            Ranges = {new RangeItemHeaderValue(readRange.Start, readRange.End)}
                        }
                    };

                    var tempFilePath = $"{readRange.Start}";
                    using (var fileStream = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write,
                               FileShare.Write))
                    {
                        gcpClient.DownloadObject("", "", fileStream, opt, progress);
                        tempFilesDictionary.TryAdd((int) index, tempFilePath);
                    }
                        
                    index++;

                });

            var result = new Tuple<int, string>(0,"");
            foreach (var tempFile in tempFilesDictionary.OrderBy(b =>Convert.ToInt64(b.Value)))
            {
                await using (var destinationStream = new FileStream(destinationFilePath, FileMode.Append))
                {
                    var tempFileBytes = await File.ReadAllBytesAsync(tempFile.Value);
                    destinationStream.Write(tempFileBytes, 0, tempFileBytes.Length);
                }

                result = ProcessStreamReader(result.Item1,0, result.Item2);
                File.Delete(tempFile.Value);
            }
        }
        
        private static Tuple<int,string> ProcessStreamReader(int total = 0, int position = 0, string linhaQuebrada = "")
        {
            var memoryStream = new MemoryStream();

            using var input =
                File.OpenRead(
                    @"");

            input.CopyTo(memoryStream);

            if (memoryStream.CanSeek)
            {
                memoryStream.Position = position;
            }
            
            var line = "";
            var linhaAtual = 0;
            using var gzip = new GZipStream(memoryStream, CompressionMode.Decompress,true);
            using var reader = new StreamReader(gzip, Encoding.UTF8);
            using var sw = File.AppendText("WriteText.txt");
            while ((line = reader.ReadLine()) != null)
            {
                if(total != 0 && linhaAtual < total)
                    continue;
                
                if (!string.IsNullOrEmpty(linhaQuebrada))
                {
                    line = linhaQuebrada + line;
                    linhaQuebrada = "";
                }

                if (line.Contains('\n'))
                {
                    sw.WriteLine(line);
                }
                else
                {
                    linhaQuebrada = line;
                    break;
                }

                linhaAtual += 1;
                Console.WriteLine(total.ToString());
            }
            
            total += linhaAtual;
            return new Tuple<int, string>(total, linhaQuebrada);
        }
    }
}