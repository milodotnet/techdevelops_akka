namespace classLib.Messages
{
    using System.Collections.Generic;
    using Microsoft.Azure.Documents;

    public class CreatePartitionActors
    {
        public readonly List<PartitionKeyRange> PartitionKeyRanges;

        public CreatePartitionActors(List<PartitionKeyRange> partitionKeyRanges)
        {
             this.PartitionKeyRanges = partitionKeyRanges;
        }
    }
}