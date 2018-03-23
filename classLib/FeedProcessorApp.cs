using System;
using Akka.Actor;
using Akka.Event;


namespace classLib
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Security;
    using System.Threading.Tasks;
    using Akka.Persistence;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Microsoft.Azure.Documents.Partitioning;
    using Microsoft.Azure.Documents.SystemFunctions;

    public class PartitionActor : ReceiveActor 
    {
        public ILoggingAdapter Log { get; } = Context.GetLogger();

        private DateTime _startedAt;

        private readonly DocumentClient _client;


        public static Props Props(DocumentClient client)
        {
            return Akka.Actor.Props.Create(() => new PartitionActor(client));
        }

        protected override void PreStart() {
            Log.Info("I Live!");
            _startedAt = DateTime.Now;
            Log.Info(_startedAt.ToString(CultureInfo.InvariantCulture));
        }

        protected override void PostStop() => Log.Info("Et e brutus?");

        public PartitionActor(DocumentClient client){
            _client = client;
            Receive<Message.ProcessPartition>(message => {                
                string continuation = message.ContinueFrom;
//                Dictionary<string, string> checkpoints = new Dictionary<string, string>();
//                checkpoints.TryGetValue(message.Id, out continuation);

                IDocumentQuery<Document> query = _client.CreateDocumentChangeFeedQuery(
                    message.CollectionUri,
                    new ChangeFeedOptions
                    {
                        PartitionKeyRangeId = message.Id,
                        StartFromBeginning = true,
                        RequestContinuation = continuation,
                        MaxItemCount = 20,
                        // Set reading time: only show change feed results modified since StartTime
                        //StartTime = DateTime.Now - TimeSpan.FromSeconds(30)
                    });

                int numChangesRead = 0;
//                while (query.HasMoreResults)
//                {
                //Task.Delay(TimeSpan.FromMilliseconds(100)).GetAwaiter().GetResult();
                FeedResponse<Document> readChangesResponse = query.ExecuteNextAsync<Document>().Result;
                
                foreach (Document changedDocument in readChangesResponse)
                {
                    Console.WriteLine("\tRead document {0} from the change feed.", changedDocument.Id);
                    numChangesRead++;
                }

//                    checkpoints[message.Id] = readChangesResponse.ResponseContinuation;
                //}

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
                //
                //Self.Tell(new Message.ProcessPartition(message.Id, message.CollectionUri, continuation));
                Context.Parent.Tell(new CheckpointPartition(message.Id, readChangesResponse.ResponseContinuation));
            });
            Receive<Die>(message => {
                Log.Info($"Die Hard");
                throw new ArgumentException();
            });
        }
    }

    public class CheckpointPartition
    {
        public override string ToString()
        {
            return $"{nameof(PartitionId)}: {PartitionId}, {nameof(Checkpoint)}: {Checkpoint}";
        }

        public readonly string PartitionId;
        public readonly string Checkpoint;

        public CheckpointPartition(string partitionId, string checkpoint)
        {
            PartitionId = partitionId;
            Checkpoint = checkpoint;
        }
    }

    public class PartitionRangeActor : ReceivePersistentActor
    {
        private readonly Dictionary<string, string> _checkpoints = new Dictionary<string, string>();
        private readonly Dictionary<string, IActorRef> _partitionActors = new Dictionary<string, IActorRef>();
        public ILoggingAdapter Log { get; } = Context.GetLogger();

        public override string PersistenceId => "me";

        public static Props Props(DocumentClient client)
        {
            return Akka.Actor.Props.Create(() => new PartitionRangeActor(client));
        }

        public PartitionRangeActor(DocumentClient client)
        {
            Uri collectionUri = UriFactory.CreateDocumentCollectionUri("CustomerReturn", "CustomerReturnEvents");
            if (client == null)
            {
                Log.Info("Document Client Does Not Exist");
            }
            Recover<CheckpointPartition>(message =>
            {
                _checkpoints[message.PartitionId] = message.Checkpoint;
            });
            Command<CheckpointPartition>(unpersistedMessage => Persist(unpersistedMessage,
                (persistedMessage) =>
                {
                    var processPartition = new Message.ProcessPartition(persistedMessage.PartitionId, collectionUri, persistedMessage.Checkpoint);
                    //Log.Info($"checkpointed, continuing {persistedMessage}");
                    _partitionActors[persistedMessage.PartitionId].Tell(processPartition);                    
                }));

            Command<Message.StartReadingPartitions>(unpersistedMessage => Persist(unpersistedMessage, (persistedMessage) => {
                //start polling to see which partitions exits, and when i find one, send the partition detected message
                
                string pkRangesResponseContinuation = null;
                List<PartitionKeyRange> partitionKeyRanges = new List<PartitionKeyRange>();

                do
                {
                    FeedResponse<PartitionKeyRange> pkRangesResponse = client.ReadPartitionKeyRangeFeedAsync(
                        collectionUri,
                        new FeedOptions { RequestContinuation = pkRangesResponseContinuation })
                        .GetAwaiter()
                        .GetResult();

                    partitionKeyRanges.AddRange(pkRangesResponse);
                    pkRangesResponseContinuation = pkRangesResponse.ResponseContinuation;

                }
                while (pkRangesResponseContinuation != null);

                partitionKeyRanges.ForEach(range =>
                {
                    IActorRef actorRef = Context.ActorOf(
                        PartitionActor.Props(client),
                        $"partition-actor-{range.Id}");
                    _partitionActors.Add(range.Id, actorRef);
                    if (_checkpoints.ContainsKey(range.Id))
                    {
                        actorRef.Tell(new Message.ProcessPartition(range.Id, collectionUri, _checkpoints[range.Id]));
                    }
                    else
                    {
                        actorRef.Tell(new Message.ProcessPartition(range.Id, collectionUri, null));
                    }                    
                    Log.Info($"Partition detected! {persistedMessage}");
                });
            }));
        }
    }

    public class CollectionSupervisor : UntypedActor
    {
        public ILoggingAdapter Log { get; } = Context.GetLogger();

        protected override void PreStart()
        {

            var uri = Environment.GetEnvironmentVariable("feedprocessoruri", EnvironmentVariableTarget.User);
            var endpointUri = new Uri(uri);
            var authKeyString = Environment.GetEnvironmentVariable("feedprocessorkey", EnvironmentVariableTarget.User);
            //emulator settings
            //var endpointUri = new Uri("https://localhost:8081");
            //var authKeyString = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
            var client = new DocumentClient(endpointUri, authKeyString);

            IActorRef actorRef = Context.ActorOf(
                PartitionRangeActor.Props(client), 
                "partition-range");
            actorRef.Tell(new Message.StartReadingPartitions());
            Log.Info("Feed Processor Application started"); 
        }

        protected override void PostStop() => Log.Info("Feed Processor Application stopped");
        
    
        // No need to handle any messages
        protected override void OnReceive(object message)
        {        
        }

        public static Props Props() => Akka.Actor.Props.Create<CollectionSupervisor>();
    }

    public class FeedProcessorApp
    {
        public static void Init()
        {
            

            using (var system = ActorSystem.Create("feedprocessor-system"))
            {
                // Create top level supervisor
                var supervisor = system.ActorOf(CollectionSupervisor.Props(), "collection-supervisor");
                // Exit the system after ENTER is pressed
                Console.WriteLine("x to exit, anything else to poison");
                var input = Console.ReadLine();
                while(input != "x") {
                    system.ActorSelection("/user/collection-supervisor/partition-range/partition-actor-1*").Tell(new Die());
                    input = Console.ReadLine();
                }
            }
        }
    }

}
