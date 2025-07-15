using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class AuthorizationRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:AUTHORIZATION";

        private readonly Action<AuthorizationRecord> _consumer;

        public AuthorizationRecordConsumer(Action<AuthorizationRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out AuthorizationRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
