using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class GroupRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:GROUP";

        private readonly Action<GroupRecord> _consumer;

        public GroupRecordConsumer(Action<GroupRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out GroupRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
