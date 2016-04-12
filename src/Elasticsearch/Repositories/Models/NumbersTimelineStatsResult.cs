using System;
using System.Collections.Generic;

namespace Foundatio.Repositories.Elasticsearch.Models {
    public class NumbersTimelineStatsResult : NumbersStatsResult {
        public NumbersTimelineStatsResult() {
            Timeline = new List<NumbersTimelineItem>();
        }

        public ICollection<NumbersTimelineItem> Timeline { get; private set; }
    }
}