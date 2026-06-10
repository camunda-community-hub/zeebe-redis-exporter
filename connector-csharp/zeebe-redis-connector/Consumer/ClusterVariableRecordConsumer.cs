using Io.Zeebe.Exporter.Proto;
using System;

namespace Io.Zeebe.Redis.Connect.Csharp.Consumer
{
    public class ClusterVariableRecordConsumer : IRecordConsumer
    {
        public static string STREAM = "zeebe:CLUSTER_VARIABLE";

        private readonly Action<ClusterVariableRecord> _consumer;

        public ClusterVariableRecordConsumer(Action<ClusterVariableRecord> action)
        {
            _consumer = action;
        }

        public void Consume(Record record)
        {
            record.Record_.TryUnpack(out ClusterVariableRecord unpacked);
            _consumer.Invoke(unpacked);
        }
    }
}

