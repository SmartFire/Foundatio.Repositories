using System;

namespace Foundatio.Repositories.Elasticsearch.Models {
    public class NumbersStatsResult {
        public long Total { get; set; }
        public double[] Numbers { get; set; }
    }
}