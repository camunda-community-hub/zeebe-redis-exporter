using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace Io.Zeebe.Redis.Connect.Csharp
{
    public class ZeebeRedisOptions
    {
        private string _redisConfigString = "localhost";
        public virtual string RedisConfigString
        {
#pragma warning disable CS8603 // Possible null reference return.
            get { return GetEnvironmentVariable("REDIS_CONFIG_STRING", _redisConfigString); }
#pragma warning restore CS8603 // Possible null reference return.
            set { _redisConfigString = value; }
        }

        private string? _redisConsumerGroup = null;
        public virtual string? RedisConsumerGroup
        {
            get { return GetEnvironmentVariable("REDIS_CONSUMER_GROUP", _redisConsumerGroup); }
            set { _redisConsumerGroup = value; }
        }

        private int _pollIntervalMillis = 500;

        public virtual int RedisPollIntervallMillis
        {
            get { return GetEnvironmentVariable("REDIS_POLL_INTERVALL_MILLIS", _pollIntervalMillis); }
            set { _pollIntervalMillis = value; }
        }

        public bool Validate()
        {
            if (String.IsNullOrWhiteSpace(_redisConfigString)) { throw new ArgumentNullException($"{nameof(ZeebeRedisOptions.RedisConfigString)}", $"'{nameof(RedisConfigString)}' cannot be empty or whitespace."); }
            return true;
        }

        public static string? GetEnvironmentVariable(string name, string? defaultValue)
            => Environment.GetEnvironmentVariable(name) is string v && v.Length > 0 ? v : defaultValue;

        public static int GetEnvironmentVariable(string name, int defaultValue)
            => Environment.GetEnvironmentVariable(name) is string v && v.Length > 0 ? Int32.Parse(v) : defaultValue;
    }
}
