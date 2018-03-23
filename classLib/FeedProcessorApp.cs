using System;
using Akka.Actor;
using Akka.Event;

namespace classLib
{

    public class CollectionSupervisor : UntypedActor
    {
        public ILoggingAdapter Log { get; } = Context.GetLogger();

        protected override void PreStart() => Log.Info("Feed Processor Application started");
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
                var supervisor = system.ActorOf(Props.Create<CollectionSupervisor>(), "collection-supervisor");
                // Exit the system after ENTER is pressed
                Console.ReadLine();
            }
        }
    }

}
