namespace classLib
{
    public class Message
    {
        public class ProcessPartition
        {
            public readonly string Id;

            public ProcessPartition(string id)
            {
                Id = id;
            }

            public override string ToString()
            {
                return Id;
            }
        }
        public class StartReadingPartitions
        {

        }
    }
}