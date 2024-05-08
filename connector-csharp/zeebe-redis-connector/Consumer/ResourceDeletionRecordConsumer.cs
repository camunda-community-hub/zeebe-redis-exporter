using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ResourceDeletionRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:RESOURCE_DELETION";

        private readonly Action<ResourceDeletionRecord> _consumer;

        public ResourceDeletionRecordConsumer(Action<ResourceDeletionRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ResourceDeletionRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
