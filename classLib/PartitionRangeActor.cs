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

            Command<StartReadingPartitions>(unpersistedMessage => Persist(unpersistedMessage, (persistedMessage) => {
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
                        actorRef.Tell(new ProcessPartition(range.Id, collectionUri, _checkpoints[range.Id]));
                    }
                    else
                    {
                        actorRef.Tell(new ProcessPartition(range.Id, collectionUri, null));
                    }                    
                    Log.Info($"Partition detected! {persistedMessage}");
                });
            }));
        }
    }
}