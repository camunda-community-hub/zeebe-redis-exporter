using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ResourceRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:RESOURCE";

        private readonly Action<ResourceRecord> _consumer;

        public ResourceRecordConsumer(Action<ResourceRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ResourceRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
