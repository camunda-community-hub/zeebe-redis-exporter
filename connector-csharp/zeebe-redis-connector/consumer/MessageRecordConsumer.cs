using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class MessageRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:MESSAGE";

        private readonly Action<MessageRecord> _consumer;

        public MessageRecordConsumer(Action<MessageRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out MessageRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
