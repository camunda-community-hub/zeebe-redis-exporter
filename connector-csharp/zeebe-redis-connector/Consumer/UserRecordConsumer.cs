using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class UserRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:USER";

        private readonly Action<UserRecord> _consumer;

        public UserRecordConsumer(Action<UserRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out UserRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
