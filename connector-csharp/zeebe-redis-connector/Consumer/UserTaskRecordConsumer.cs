using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class UserTaskRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:USER_TASK";

        private readonly Action<UserTaskRecord> _consumer;

        public UserTaskRecordConsumer(Action<UserTaskRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out UserTaskRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
