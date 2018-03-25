namespace classLib
{
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
}