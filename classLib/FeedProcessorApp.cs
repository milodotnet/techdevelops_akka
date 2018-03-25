namespace classLib
{
    using System;
    using Akka.Actor;
    using Messages;

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
