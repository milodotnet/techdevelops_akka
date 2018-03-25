namespace classLib
{
    using System;
    using System.Threading.Tasks;
    using Akka.Actor;
    using Akka.Event;
    using Messages;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;

    public class PartitionActor : ReceiveActor 
    {
        public ILoggingAdapter Log { get; } = Context.GetLogger();

        public static Props Props(DocumentClient client)
        {
            return Akka.Actor.Props.Create(() => new PartitionActor(client));
        }

        protected override void PreStart() {
            Log.Info("Partition Actor Started");
        }

        protected override void PostStop() => Log.Info("Et e brutus?");

        public PartitionActor(DocumentClient client){
            Receive<ProcessPartition>(message => {                
                string continuation = message.ContinueFrom;
                IDocumentQuery<Document> query = client.CreateDocumentChangeFeedQuery(
                    message.CollectionUri,
                    new ChangeFeedOptions
                    {
                        PartitionKeyRangeId = message.Id,
                        StartFromBeginning = true,
                        RequestContinuation = continuation,
                        MaxItemCount = 20,
                    });
                int numChangesRead = 0;
                FeedResponse<Document> readChangesResponse = query.ExecuteNextAsync<Document>().Result;
                
                foreach (Document changedDocument in readChangesResponse)
                {
                    Console.WriteLine("\tRead document {0} from the change feed.", changedDocument.Id);
                    numChangesRead++;
                }
                if (readChangesResponse.ResponseContinuation == continuation)
                {
                    Console.WriteLine($"Nothing new, waiting at {message}");
                    Task.Delay(TimeSpan.FromSeconds(10)).GetAwaiter().GetResult();
                }
                else
                {
                    Log.Info($"started processing partition {message}");
                    Console.WriteLine("Read {0} documents from the change feed", numChangesRead);
                }
                Context.Parent.Tell(new CheckpointPartition(message.Id, readChangesResponse.ResponseContinuation));
            });
            Receive<Die>(message => {
                Log.Info($"Die Hard");
                throw new ArgumentException();
            });
        }
    }
}