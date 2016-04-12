using System;

namespace Foundatio.Elasticsearch.Extensions {
    public static class TimeSpanExtensions {
        public static TimeSpan Round(this TimeSpan time, TimeSpan roundingInterval, MidpointRounding roundingType = MidpointRounding.ToEven) {
            return new TimeSpan(Convert.ToInt64(Math.Round((double)time.Ticks / (double)roundingInterval.Ticks, roundingType)) * roundingInterval.Ticks);
        }
    }
}