using System;
using System.Collections.Generic;

namespace Foundatio.Repositories.Elasticsearch.Models {
    public class NumbersTermStatsResult : NumbersStatsResult {
        public NumbersTermStatsResult() {
            Terms = new List<NumbersTermStatsItem>();
        }

        public ICollection<NumbersTermStatsItem> Terms { get; private set; }
    }
}