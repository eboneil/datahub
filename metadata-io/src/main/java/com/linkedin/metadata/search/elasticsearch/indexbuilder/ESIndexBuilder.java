package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import com.linkedin.metadata.config.ElasticSearchConfiguration;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.client.tasks.TaskSubmissionResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;


@Slf4j
@RequiredArgsConstructor
public class ESIndexBuilder {

  private final RestHighLevelClient searchClient;
  @Getter
  private final int numShards;

  @Getter
  private final int numReplicas;

  @Getter
  private final int numRetries;

  @Getter
  private final int refreshIntervalSeconds;

  @Getter
  private final Map<String, Map<String, String>> indexSettingOverrides;

  @Getter
  private final boolean enableIndexSettingsReindex;

  @Getter
  private final boolean enableIndexMappingsReindex;

  private final ElasticSearchConfiguration elasticSearchConfiguration;


  public ReindexConfig buildReindexState(String indexName, Map<String, Object> mappings, Map<String, Object> settings) throws IOException {
    ReindexConfig.ReindexConfigBuilder builder = ReindexConfig.builder()
            .name(indexName)
            .enableIndexSettingsReindex(enableIndexSettingsReindex)
            .enableIndexMappingsReindex(enableIndexMappingsReindex)
            .targetMappings(mappings);

    Map<String, Object> baseSettings = new HashMap<>(settings);
    baseSettings.put("number_of_shards", numShards);
    baseSettings.put("number_of_replicas", numReplicas);
    baseSettings.put("refresh_interval", String.format("%ss", refreshIntervalSeconds));
    baseSettings.putAll(indexSettingOverrides.getOrDefault(indexName, Map.of()));
    Map<String, Object> targetSetting = ImmutableMap.of("index", baseSettings);
    builder.targetSettings(targetSetting);

    // Check if index exists
    boolean exists = searchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
    builder.exists(exists);

    // If index doesn't exist, no reindex
    if (!exists) {
      return builder.build();
    }

    Settings currentSettings = searchClient.indices()
            .getSettings(new GetSettingsRequest().indices(indexName), RequestOptions.DEFAULT)
            .getIndexToSettings()
            .valuesIt()
            .next();
    builder.currentSettings(currentSettings);

    Map<String, Object> currentMappings = searchClient.indices()
            .getMapping(new GetMappingsRequest().indices(indexName), RequestOptions.DEFAULT)
            .mappings()
            .values()
            .stream()
            .findFirst()
            .get()
            .getSourceAsMap();
    builder.currentMappings(currentMappings);

    return builder.build();
  }

  @Deprecated
  public void buildIndex(String indexName, Map<String, Object> mappings, Map<String, Object> settings) throws IOException {
    buildIndex(buildReindexState(indexName, mappings, settings));
  }

  public void buildIndex(ReindexConfig indexState) throws IOException {
    // If index doesn't exist, create index
    if (!indexState.exists()) {
      createIndex(indexState.name(), indexState);
      return;
    }

    // If there are no updates to mappings and settings, return
    if (!indexState.requiresApplyMappings() && !indexState.requiresApplySettings()) {
      log.info("No updates to index {}", indexState.name());
      return;
    }

    if (!indexState.requiresReindex()) {
      // no need to reindex and only new mappings or dynamic settings

      // Just update the additional mappings
      if (indexState.isPureMappingsAddition()) {
        log.info("Updating index {} mappings in place.", indexState.name());
        PutMappingRequest request = new PutMappingRequest(indexState.name()).source(indexState.targetMappings());
        searchClient.indices().putMapping(request, RequestOptions.DEFAULT);
        log.info("Updated index {} with new mappings", indexState.name());
      }

      if (indexState.requiresApplySettings()) {
        UpdateSettingsRequest request = new UpdateSettingsRequest(indexState.name());
        Map<String, Object> indexSettings = ((Map<String, Object>) indexState.targetSettings().get("index"))
                .entrySet().stream()
                .filter(e -> ReindexConfig.SETTINGS_DYNAMIC.contains(e.getKey()))
                .collect(Collectors.toMap(e -> "index." + e.getKey(), Map.Entry::getValue));
        request.settings(indexSettings);

        boolean ack = searchClient.indices().putSettings(request, RequestOptions.DEFAULT).isAcknowledged();
        log.info("Updated index {} with new settings. Settings: {}, Acknowledged: {}", indexState.name(),
                ReindexConfig.OBJECT_MAPPER.writeValueAsString(indexSettings), ack);
      }
    } else {
      reindex(indexState.name(), indexState);
    }
  }

  private void reindex(String indexName, ReindexConfig indexState) throws IOException {
    final long startTime = System.currentTimeMillis();

    String tempIndexName = indexName + "_" + startTime;
    createIndex(tempIndexName, indexState);

    try {
      final int maxReindexHours = 6;
      final long initialCheckIntervalMilli = 1000;
      final long finalCheckIntervalMilli = 30000;
      final long timeoutAt = startTime + (1000 * 60 * 60 * maxReindexHours);

      ReindexRequest reindexRequest = new ReindexRequest()
              .setSourceIndices(indexName)
              .setDestIndex(tempIndexName)
              .setMaxRetries(numRetries)
              .setAbortOnVersionConflict(false)
              .setRefresh(true)
              .setTimeout(TimeValue.timeValueHours(maxReindexHours))
              .setSourceBatchSize(2500);

      TaskSubmissionResponse reindexTask = searchClient.submitReindexTask(reindexRequest, RequestOptions.DEFAULT);

      boolean reindexTaskCompleted = false;
      int count = 0;

      while (System.currentTimeMillis() < timeoutAt) {
        log.info("Task: {} - Reindexing from {} to {} in progress...", reindexTask.getTask(), indexName, tempIndexName);
        ListTasksRequest request = new ListTasksRequest()
                .setWaitForCompletion(true)
                .setParentTaskId(new TaskId(reindexTask.getTask()));
        Optional<TaskInfo> taskInfo = searchClient.tasks().list(request, RequestOptions.DEFAULT).getTasks().stream()
                .filter(task -> task.getTaskId().toString().equals(reindexTask.getTask()))
                .findFirst();
        if (taskInfo.isEmpty()) {
          log.info("Task: {} - Reindexing {} to {} task has completed, will now check if reindex was successful",
                  reindexTask.getTask(), indexName, tempIndexName);
          reindexTaskCompleted = true;
          break;
        }
        try {
          count = count + 1;
          Thread.sleep(Math.min(finalCheckIntervalMilli, initialCheckIntervalMilli * count));
        } catch (InterruptedException e) {
          log.info("Trouble sleeping while reindexing {} to {}: Exception {}. Retrying...", indexName, tempIndexName,
                  e.toString());
        }
      }
      if (!reindexTaskCompleted) {
        throw new RuntimeException(
                String.format("Reindex from %s to %s failed-- task exceeded time limit", indexName, tempIndexName));
      }

    } catch (Exception e) {
      log.info("Failed to reindex {} to {}: Exception {}", indexName, tempIndexName, e.toString());
      searchClient.indices().delete(new DeleteIndexRequest().indices(tempIndexName), RequestOptions.DEFAULT);
      throw e;
    }

    // Check whether reindex succeeded by comparing document count
    // There can be some delay between the reindex finishing and count being fully up to date, so try multiple times
    long originalCount = 0;
    long reindexedCount = 0;
    for (int i = 0; i < this.numRetries; i++) {
      // Check if reindex succeeded by comparing document counts
      originalCount = getCount(indexName);
      reindexedCount = getCount(tempIndexName);
      if (originalCount == reindexedCount) {
        break;
      }
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (originalCount != reindexedCount) {
      if (elasticSearchConfiguration.getBuildIndices().isAllowDocCountMismatch()
              && elasticSearchConfiguration.getBuildIndices().isCloneIndices()) {
        log.warn("Index: {} - Post-reindex document count is different, source_doc_count: {} reindex_doc_count: {}\n"
                + "This condition is explicitly ALLOWED, please refer to latest clone if original index is required.",
                indexName, originalCount, reindexedCount);
      } else {
        log.error("Index: {} - Post-reindex document count is different, source_doc_count: {} reindex_doc_count: {}",
                indexName, originalCount, reindexedCount);
        diff(indexName, tempIndexName, Math.max(originalCount, reindexedCount));
        searchClient.indices().delete(new DeleteIndexRequest().indices(tempIndexName), RequestOptions.DEFAULT);
        throw new RuntimeException(String.format("Reindex from %s to %s failed. Document count %s != %s", indexName, tempIndexName,
                originalCount, reindexedCount));
      }
    }

    log.info("Reindex from {} to {} succeeded", indexName, tempIndexName);
    String indexNamePattern = indexName + "_*";
    // Check if the original index is aliased or not
    GetAliasesResponse aliasesResponse = searchClient.indices()
        .getAlias(new GetAliasesRequest(indexName).indices(indexNamePattern), RequestOptions.DEFAULT);
    // If not aliased, delete the original index
    if (aliasesResponse.getAliases().isEmpty()) {
      searchClient.indices().delete(new DeleteIndexRequest().indices(indexName), RequestOptions.DEFAULT);
    } else {
      searchClient.indices()
          .delete(new DeleteIndexRequest().indices(aliasesResponse.getAliases().keySet().toArray(new String[0])),
              RequestOptions.DEFAULT);
    }

    // Add alias for the new index
    AliasActions removeAction = AliasActions.remove().alias(indexName).index(indexNamePattern);
    AliasActions addAction = AliasActions.add().alias(indexName).index(tempIndexName);
    searchClient.indices()
        .updateAliases(new IndicesAliasesRequest().addAliasAction(removeAction).addAliasAction(addAction),
            RequestOptions.DEFAULT);
    log.info("Finished setting up {}", indexName);
  }

  private void diff(String indexA, String indexB, long maxDocs) {
    if (maxDocs <= 100) {

      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.size(100);
      searchSourceBuilder.sort(SortBuilders.fieldSort("_id").order(SortOrder.ASC));

      SearchRequest indexARequest = new SearchRequest(indexA);
      indexARequest.source(searchSourceBuilder);
      SearchRequest indexBRequest = new SearchRequest(indexB);
      indexBRequest.source(searchSourceBuilder);

      try {
        SearchResponse responseA = searchClient.search(indexARequest, RequestOptions.DEFAULT);
        SearchResponse responseB = searchClient.search(indexBRequest, RequestOptions.DEFAULT);

        Set<String> actual = Arrays.stream(responseB.getHits().getHits())
                .map(SearchHit::getId).collect(Collectors.toSet());

        log.error("Missing {}", Arrays.stream(responseA.getHits().getHits())
                .filter(doc -> !actual.contains(doc.getId()))
                .map(SearchHit::getSourceAsString).collect(Collectors.toSet()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private long getCount(@Nonnull String indexName) throws IOException {
    return searchClient.count(new CountRequest(indexName).query(QueryBuilders.matchAllQuery()), RequestOptions.DEFAULT)
        .getCount();
  }

  private void createIndex(String indexName, ReindexConfig state) throws IOException {
    log.info("Index {} does not exist. Creating", indexName);
    CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
    createIndexRequest.mapping(state.targetMappings());
    createIndexRequest.settings(state.targetSettings());
    searchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    log.info("Created index {}", indexName);
  }
}
