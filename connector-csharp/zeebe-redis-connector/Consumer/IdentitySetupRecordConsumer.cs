using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class IdentitySetupRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:IDENTITY_SETUP";

        private readonly Action<IdentitySetupRecord> _consumer;

        public IdentitySetupRecordConsumer(Action<IdentitySetupRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out IdentitySetupRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}
