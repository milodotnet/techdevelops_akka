using System;
using Akka.Actor;
using Akka.Event;

namespace classLib
{

    public class StartReadingPartitions {

    }

    public class PartitionDetected {
        public string Id {get;set;}

        public override string ToString(){
            return Id;
        }
    }

    public class PartitionRangeActor : ReceiveActor
    {
        public ILoggingAdapter Log { get; } = Context.GetLogger();
    
        public PartitionRangeActor()
        {
            Receive<StartReadingPartitions>(message => {
                            //start polling to see which partitions exits, and when i find one, send the partition detected message
               Context.Parent.Tell(new PartitionDetected {  Id = "1" });
               Log.Info("Start processing");
            });
        }
    }

    public class CollectionSupervisor : UntypedActor
    {
        public ILoggingAdapter Log { get; } = Context.GetLogger();

        protected override void PreStart() {
            IActorRef actorRef = Context.ActorOf<PartitionRangeActor>("partition-range");
            actorRef.Tell(new StartReadingPartitions());
            Log.Info("Feed Processor Application started"); 
        }

        protected override void PostStop() => Log.Info("Feed Processor Application stopped");
        
    
        // No need to handle any messages
        protected override void OnReceive(object message)
        {
            if(message is PartitionDetected){
                Log.Info($"Partition detected! {message}");
            }
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
                Console.ReadLine();
            }
        }
    }

}
