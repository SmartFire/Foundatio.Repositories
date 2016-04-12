using System;

namespace Foundatio.Repositories.Elasticsearch.Models {
    public class NumbersTimelineItem {
        public DateTime Date { get; set; }
        public long Total { get; set; }
        public double[] Numbers { get; set; }
    }
}