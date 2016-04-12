using System;

namespace Foundatio.Repositories.Elasticsearch.Models {
    public class NumbersTermStatsItem {
        public string Term { get; set; }
        public long Total { get; set; }
        public double[] Numbers { get; set; }
    }
}