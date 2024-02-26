using Io.Zeebe.Exporter.Proto;

namespace zeebe_redis_connector.consumer
{
    public interface IRecordConsumer
    {
        void Consume(Record record);
    }
}
