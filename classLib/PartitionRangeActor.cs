namespace classLib
{
    using System;
    using System.Collections.Generic;
    using Akka.Actor;
    using Akka.Persistence;
    using Messages;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;

    public sealed class PartitionRangeActor : ReceivePersistentActor
    {
        private readonly Dictionary<string, string> _checkpoints = new Dictionary<string, string>();
        private readonly Dictionary<string, IActorRef> _partitionActors = new Dictionary<string, IActorRef>();

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
                    var processPartition = new ProcessPartition(persistedMessage.PartitionId, collectionUri, persistedMessage.Checkpoint);
                    _partitionActors[persistedMessage.PartitionId].Tell(processPartition);
                }));

            Command<CreatePartitionActors>(cmd =>
            {
                Console.WriteLine("Create PartitionActors hit");
                cmd.PartitionKeyRanges.ForEach(range =>
                {
                    IActorRef actorRef = Context.ActorOf(
                        PartitionActor.Props(client),
                        $"partition-actor-{range.Id}");
                    _partitionActors.Add(range.Id, actorRef);
                    if (_checkpoints.ContainsKey(range.Id))
                    {
                        actorRef.Tell(new ProcessPartition(range.Id, collectionUri, _checkpoints[range.Id]));
                    }
                    else
                    {
                        actorRef.Tell(new ProcessPartition(range.Id, collectionUri, null));
                    }
                });
            });

            Command<StartReadingPartitions>(unpersistedMessage => Persist(unpersistedMessage, (persistedMessage) =>
            {
                //start polling to see which partitions exits, and when i find one, send the partition detected message
                Console.WriteLine("Start reading partitions received");
                client?.ReadPartitionKeyRangeFeedAsync(collectionUri, new FeedOptions())
                    .ContinueWith(pkRangesResponse =>
                    {
                        Console.WriteLine("Continuation hit");

                        try
                        {
                            var result = pkRangesResponse.Result;
                            var partitionKeyRanges = new List<PartitionKeyRange>();
                            partitionKeyRanges.AddRange(result);
                            Console.WriteLine("sending create partition actors");
                            Console.WriteLine($"{partitionKeyRanges.Count}");
                            return new CreatePartitionActors(partitionKeyRanges);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"{ex.Message}");
                            return new CreatePartitionActors(new List<PartitionKeyRange>());                            
                        }
                    })
                    .PipeTo(Self);


            }));
        }
    }
}