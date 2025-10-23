using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class RoleRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:ROLE";

        private readonly Action<RoleRecord> _consumer;

        public RoleRecordConsumer(Action<RoleRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out RoleRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
