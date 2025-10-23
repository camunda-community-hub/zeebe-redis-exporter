using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class TenantRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:TENANT";

        private readonly Action<TenantRecord> _consumer;

        public TenantRecordConsumer(Action<TenantRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out TenantRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
