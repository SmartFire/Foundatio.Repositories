﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Elasticsearch.Net;
using Foundatio.Caching;
using Foundatio.Elasticsearch.Extensions;
using Foundatio.Elasticsearch.Jobs;
using Foundatio.Elasticsearch.Repositories.Configuration;
using Foundatio.Extensions;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Logging;
using Foundatio.Queues;
using Nest;

namespace Foundatio.Elasticsearch.Configuration {
    public abstract class ElasticConfigurationBase {
        protected readonly IQueue<WorkItemData> _workItemQueue;
        protected readonly ILockProvider _lockProvider;
        protected IDictionary<Type, string> _indexMap;
        protected readonly ILogger _logger;

        public ElasticConfigurationBase(IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, ILoggerFactory loggerFactory = null) {
            _workItemQueue = workItemQueue;
            _lockProvider = new ThrottlingLockProvider(cacheClient, 1, TimeSpan.FromMinutes(1));
            _logger = loggerFactory.CreateLogger(GetType());
        }

        public virtual IElasticClient GetClient(IEnumerable<Uri> serverUris) {
            var indexes = GetIndexes().ToList();
            _indexMap = indexes.ToIndexTypeNames();

            var settings = GetConnectionSettings(serverUris, indexes);
            var client = new ElasticClient(settings);
            ConfigureIndexes(client, indexes);
            return client;
        }

        protected virtual ConnectionSettings GetConnectionSettings(IEnumerable<Uri> serverUris, IEnumerable<IElasticIndex> indexes, Func<ConnectionSettings, IElasticsearchSerializer> serializerFactory = null) {
            return new ConnectionSettings(new StaticConnectionPool(serverUris), new KeepAliveHttpConnection(), serializerFactory)
                .MapDefaultTypeIndices(t => t.AddRange(indexes.ToTypeIndices()))
                .MapDefaultTypeNames(t => t.AddRange(indexes.ToIndexTypeNames()));
        }

        public virtual void ConfigureIndexes(IElasticClient client, IEnumerable<IElasticIndex> indexes = null) {
            if (indexes == null)
                indexes = GetIndexes();

            foreach (var idx in indexes) {
                int currentVersion = GetAliasVersion(client, idx.AliasName);

                IResponse response = null;
                var templatedIndex = idx as ITemplatedElasticIndex;
                if (templatedIndex != null)
                    response = client.PutIndexTemplate(idx.VersionedName, template => templatedIndex.CreateTemplate(template).Aliases(a => a.Alias(idx.AliasName)));
                else if (!client.IndexExists(idx.VersionedName).Exists)
                    response = client.CreateIndex(idx.VersionedName, descriptor => idx.CreateIndex(descriptor).Aliases(a => a.Alias(idx.AliasName)));

                Debug.Assert(response == null || response.IsValid, response?.ServerError != null ? response.ServerError.Error.Reason : "An error occurred creating the index or template.");
                
                // Add existing indexes to the alias.
                if (!client.AliasExists(a => a.Name(idx.AliasName)).Exists) {
                    if (templatedIndex != null) {
                        var indices = client.IndicesStats(Indices.All).Indices.Where(kvp => kvp.Key.StartsWith(idx.VersionedName)).Select(kvp => kvp.Key).ToList();
                        if (indices.Count > 0) {
                            var descriptor = new BulkAliasDescriptor();
                            foreach (string name in indices)
                                descriptor.Add(add => add.Index(name).Alias(idx.AliasName));

                            response = client.Alias(descriptor);
                        }
                    } else {
                        response = client.Alias(a => a.Add(add => add.Index(idx.VersionedName).Alias(idx.AliasName)));
                    }

                    Debug.Assert(response != null && response.IsValid, response?.ServerError != null ? response.ServerError.Error.Reason : "An error occurred creating the alias.");
                }

                // already on current version
                if (currentVersion >= idx.Version || currentVersion < 1)
                    continue;

                var reindexWorkItem = new ReindexWorkItem {
                    OldIndex = String.Concat(idx.AliasName, "-v", currentVersion),
                    NewIndex = idx.VersionedName,
                    Alias = idx.AliasName,
                    DeleteOld = true,
                    ParentMaps = idx.GetIndexTypes()
                            .Select(kvp => new ParentMap {Type = kvp.Value.Name, ParentPath = kvp.Value.ParentPath})
                            .Where(m => !String.IsNullOrEmpty(m.ParentPath))
                            .ToList()
                };

                bool isReindexing = _lockProvider.IsLockedAsync(String.Concat("reindex:", reindexWorkItem.Alias, reindexWorkItem.OldIndex, reindexWorkItem.NewIndex)).Result;
                // already reindexing
                if (isReindexing)
                    continue;

                // enqueue reindex to new version
                _lockProvider.TryUsingAsync("enqueue-reindex", () => _workItemQueue.EnqueueAsync(reindexWorkItem), TimeSpan.Zero, CancellationToken.None).Wait();
            }
        }

        public virtual void DeleteIndexes(IElasticClient client, IEnumerable<IElasticIndex> indexes = null) {
            if (indexes == null)
                indexes = GetIndexes();
            
            foreach (var idx in indexes) {
                IIndicesResponse deleteResponse;

                var templatedIndex = idx as ITemplatedElasticIndex;
                if (templatedIndex != null) {
                    deleteResponse = client.DeleteIndex(idx.VersionedName + "-*");

                    if (client.IndexTemplateExists(idx.VersionedName).Exists) {
                        var response = client.DeleteIndexTemplate(idx.VersionedName);
                        Debug.Assert(response.IsValid, response.ServerError != null ? response.ServerError.Error.Reason : "An error occurred deleting the index template.");
                    }
                } else {
                    deleteResponse = client.DeleteIndex(idx.VersionedName);
                }

                Debug.Assert(deleteResponse.IsValid, deleteResponse.ServerError != null ? deleteResponse.ServerError.Error.Reason : "An error occurred deleting the indexes.");
            }
        }

        protected string GetIndexAliasForType(Type entityType) {
            return _indexMap.ContainsKey(entityType) ? _indexMap[entityType] : null;
        }

        protected abstract IEnumerable<IElasticIndex> GetIndexes();

        protected virtual int GetAliasVersion(IElasticClient client, string alias) {
            var res = client.GetAlias(a => a.Name(alias));
            if (!res.Indices.Any())
                return -1;

            string indexName = res.Indices.FirstOrDefault().Key;
            string versionString = indexName.Substring(indexName.LastIndexOf("-", StringComparison.Ordinal));

            int version;
            if (!Int32.TryParse(versionString.Substring(2), out version))
                return -1;

            return version;
        }
    }
}
