using System;
using Akka.Actor;
using Akka.Event;

namespace classLib
{
    using System.Globalization;

    public class PartitionActor : ReceiveActor 
    {
        public ILoggingAdapter Log { get; } = Context.GetLogger();

        private DateTime _startedAt;

        protected override void PreStart() {
            Log.Info("I Live!");
            _startedAt = DateTime.Now;
            Log.Info(_startedAt.ToString(CultureInfo.InvariantCulture));
        }

        protected override void PostStop() => Log.Info("Et e brutus?");

        public PartitionActor(){
            Receive<Message.ProcessPartition>(message => {
                Log.Info($"started processing partition {message}");
            });
            Receive<Die>(message => {
                Log.Info($"Die Hard");
                throw new ArgumentException();
            });
        }
    }

    public class PartitionRangeActor : ReceiveActor
    {
        public ILoggingAdapter Log { get; } = Context.GetLogger();
        
        public PartitionRangeActor()
        {
            Receive<Message.StartReadingPartitions>(message => {
                //start polling to see which partitions exits, and when i find one, send the partition detected message
                var actorRef = Context.ActorOf<PartitionActor>($"partition-actor-1");
                actorRef.Tell(new Message.ProcessPartition ("1"));
                Log.Info($"Partition detected! {message}");                
            });
        }
    }

    public class CollectionSupervisor : UntypedActor
    {
        public ILoggingAdapter Log { get; } = Context.GetLogger();

        protected override void PreStart() {
            IActorRef actorRef = Context.ActorOf<PartitionRangeActor>("partition-range");
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
