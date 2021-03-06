﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Elasticsearch.Repositories;
using Foundatio.Elasticsearch.Repositories.Queries;
using Foundatio.Elasticsearch.Repositories.Queries.Options;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public class EmployeeRepository : AppRepositoryBase<Employee> {
        public EmployeeRepository(ElasticRepositoryContext<Employee> context) : base(context) { }

        public Task<Employee> GetByAgeAsync(int age) {
            return FindOneAsync(new AgeQuery().WithAge(age));
        }

        public Task<FindResults<Employee>> GetAllByAgeAsync(int age) {
            return FindAsync(new AgeQuery().WithAge(age));
        }

        public Task<Employee> GetByCompanyAsync(string company) {
            return FindOneAsync(new CompanyQuery().WithCompany(company));
        }

        public Task<FindResults<Employee>> GetAllByCompanyAsync(string company) {
            return FindAsync(new CompanyQuery().WithCompany(company));
        }
        
        public Task<long> GetCountByCompanyAsync(string company) {
            return CountAsync(new CompanyQuery().WithCompany(company).WithCacheKey(company));
        }

        protected override async Task InvalidateCacheAsync(ICollection<ModifiedDocument<Employee>> documents) {
            if (!IsCacheEnabled)
                return;

            var options = Options as IQueryOptions;
            if (documents != null && documents.Count > 0 && options != null && options.HasIdentity) {
                var keys = documents.Select(d => $"count:{d.Value.CompanyId}").Distinct().ToList();

                if (keys.Count > 0)
                    await Cache.RemoveAllAsync(keys);
            }

            await base.InvalidateCacheAsync(documents);
        }
    }
}