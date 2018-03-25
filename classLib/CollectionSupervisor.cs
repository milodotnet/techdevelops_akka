namespace classLib
{
    using System;
    using Akka.Actor;
    using Akka.Event;
    using Messages;
    using Microsoft.Azure.Documents.Client;

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
            actorRef.Tell(new StartReadingPartitions());
            Log.Info("Feed Processor Application started"); 
        }

        protected override void PostStop() => Log.Info("Feed Processor Application stopped");
    
        // No need to handle any messages
        protected override void OnReceive(object message)
        {        
        }

        public static Props Props() => Akka.Actor.Props.Create<CollectionSupervisor>();
    }
}