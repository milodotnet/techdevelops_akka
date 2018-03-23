namespace classLib
{
    using System;

    public class Message
    {
        public class ProcessPartition
        {
            public readonly string Id;
            public readonly string ContinueFrom;
            public readonly Uri CollectionUri;

            public ProcessPartition(string id, Uri collectionUri, string continueFrom)
            {
                Id = id;
                CollectionUri = collectionUri;
                ContinueFrom = continueFrom;
            }

            public override string ToString()
            {
                return $"{nameof(Id)}: {Id}, {nameof(ContinueFrom)}: {ContinueFrom}, {nameof(CollectionUri)}: {CollectionUri}";
            }
        }
        public class StartReadingPartitions
        {

        }
    }
}