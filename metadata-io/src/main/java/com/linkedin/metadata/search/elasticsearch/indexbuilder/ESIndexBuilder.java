package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.google.common.collect.ImmutableMap;

import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.version.GitVersion;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import com.linkedin.metadata.config.ElasticSearchConfiguration;
import com.linkedin.util.Pair;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
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

  @Getter
  private final ElasticSearchConfiguration elasticSearchConfiguration;

  @Getter
  private final GitVersion gitVersion;

  final private static RequestOptions REQUEST_OPTIONS = RequestOptions.DEFAULT.toBuilder()
          .setRequestConfig(RequestConfig.custom()
                  .setSocketTimeout(180 * 1000).build()).build();


  public ReindexConfig buildReindexState(String indexName, Map<String, Object> mappings, Map<String, Object> settings) throws IOException {
    ReindexConfig.ReindexConfigBuilder builder = ReindexConfig.builder()
            .name(indexName)
            .enableIndexSettingsReindex(enableIndexSettingsReindex)
            .enableIndexMappingsReindex(enableIndexMappingsReindex)
            .targetMappings(mappings)
            .version(gitVersion.getVersion());

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
      reindex(indexState);
    }
  }

  private void reindex(ReindexConfig indexState) throws IOException {
    final long startTime = System.currentTimeMillis();

    final int maxReindexHours = 8;
    final long initialCheckIntervalMilli = 1000;
    final long finalCheckIntervalMilli = 30000;
    final long timeoutAt = startTime + (1000 * 60 * 60 * maxReindexHours);

    String tempIndexName = indexState.name() + "_" + startTime;

    try {
      ListTasksRequest listTasksRequest = new ListTasksRequest()
              .setTimeout(TimeValue.timeValueSeconds(180))
              .setDetailed(true);
      List<TaskInfo> taskInfos = searchClient.tasks().list(listTasksRequest, REQUEST_OPTIONS).getTasks();
      Optional<TaskInfo> previousTaskInfo = taskInfos.stream()
          .filter(info -> ESUtils.prefixMatch(info.getHeaders().get(ESUtils.OPAQUE_ID_HEADER), gitVersion.getVersion(),
                  indexState.name())).findFirst();

      String parentTaskId;
      if (previousTaskInfo.isPresent()) {
        log.info("Reindex task {} in progress with description {}. Attempting to continue task from breakpoint.",
                previousTaskInfo.get().getTaskId(), previousTaskInfo.get().getDescription());
        parentTaskId = previousTaskInfo.get().getParentTaskId().toString();
        tempIndexName = ESUtils.extractTargetIndex(previousTaskInfo.get().getHeaders().get(ESUtils.OPAQUE_ID_HEADER));
      } else {
        // Create new index
        createIndex(tempIndexName, indexState);

        ReindexRequest reindexRequest = new ReindexRequest()
                .setSourceIndices(indexState.name())
                .setDestIndex(tempIndexName)
                .setMaxRetries(numRetries)
                .setAbortOnVersionConflict(false)
                .setTimeout(TimeValue.timeValueHours(maxReindexHours))
                .setSourceBatchSize(2500);

        RequestOptions requestOptions = ESUtils.buildReindexTaskRequestOptions(gitVersion.getVersion(), indexState.name(),
                tempIndexName);
        TaskSubmissionResponse reindexTask = searchClient.submitReindexTask(reindexRequest, requestOptions);
        parentTaskId = reindexTask.getTask();
      }

      boolean reindexTaskCompleted = false;
      int count = 0;

      while (System.currentTimeMillis() < timeoutAt) {

        log.info("Task: {} - Reindexing from {} to {} in progress...", parentTaskId, indexState.name(), tempIndexName);
        ListTasksRequest request = new ListTasksRequest().setParentTaskId(new TaskId(parentTaskId));
        Optional<TaskInfo> taskInfo = searchClient.tasks()
                .list(request, REQUEST_OPTIONS).getTasks().stream()
                .filter(task -> task.getTaskId().toString().equals(parentTaskId))
                .findFirst();

        if (taskInfo.isEmpty()) {
          log.info("Task: {} - Is not running. Checking document counts.", parentTaskId);

          // Either the reindex completed really fast or may not have started
          boolean completed = false;
          try {
            Pair<Long, Long> documentCounts = getDocumentCounts(indexState.name(), tempIndexName);
            if (documentCounts.getFirst().equals(documentCounts.getSecond())) {
              completed = true;
            } else {
              log.warn("Task: {} - Document counts do not match {} != {}.", parentTaskId, documentCounts.getFirst(),
                      documentCounts.getSecond());
            }
          } catch (Exception e) {
            log.warn("Task: {} - Error comparing document counts. Exception caught.", parentTaskId, e);
          }

          if (completed) {
            log.info("Task: {} - Reindexing {} to {} task has completed, will now check if reindex was successful",
                    parentTaskId, indexState.name(), tempIndexName);
            reindexTaskCompleted = true;
            break;
          }
        }

        try {
          count = count + 1;
          Thread.sleep(Math.min(finalCheckIntervalMilli, initialCheckIntervalMilli * count));
        } catch (InterruptedException e) {
          log.info("Trouble sleeping while reindexing {} to {}: Exception {}. Retrying...", indexState.name(), tempIndexName,
                  e.toString());
        }
      }
      if (!reindexTaskCompleted) {
        throw new RuntimeException(
                String.format("Reindex from %s to %s failed-- task exceeded time limit", indexState.name(), tempIndexName));
      }

    } catch (Exception e) {
      log.error("Failed to reindex {} to {}: Exception {}", indexState.name(), tempIndexName, e.toString());
      searchClient.indices().delete(new DeleteIndexRequest().indices(tempIndexName), RequestOptions.DEFAULT);
      throw e;
    }

    Pair<Long, Long> documentCounts = getDocumentCounts(indexState.name(), tempIndexName);

    if (!documentCounts.getFirst().equals(documentCounts.getSecond())) {
      if (elasticSearchConfiguration.getBuildIndices().isAllowDocCountMismatch()
              && elasticSearchConfiguration.getBuildIndices().isCloneIndices()) {
        log.warn("Index: {} - Post-reindex document count is different, source_doc_count: {} reindex_doc_count: {}\n"
                + "This condition is explicitly ALLOWED, please refer to latest clone if original index is required.",
                indexState.name(), documentCounts.getFirst(), documentCounts.getSecond());
      } else {
        log.error("Index: {} - Post-reindex document count is different, source_doc_count: {} reindex_doc_count: {}",
                indexState.name(), documentCounts.getFirst(), documentCounts.getSecond());
        diff(indexState.name(), tempIndexName, Math.max(documentCounts.getFirst(), documentCounts.getSecond()));
        searchClient.indices().delete(new DeleteIndexRequest().indices(tempIndexName), RequestOptions.DEFAULT);
        throw new RuntimeException(String.format("Reindex from %s to %s failed. Document count %s != %s", indexState.name(), tempIndexName,
                documentCounts.getFirst(), documentCounts.getSecond()));
      }
    }

    log.info("Reindex from {} to {} succeeded", indexState.name(), tempIndexName);
    String indexNamePattern = indexState.name() + "*";
    // Check if the original index is aliased or not
    GetAliasesResponse aliasesResponse = searchClient.indices().getAlias(
            new GetAliasesRequest(indexState.name()).indices(indexNamePattern), RequestOptions.DEFAULT);
    // If not aliased, delete the original index
    if (aliasesResponse.getAliases().isEmpty()) {
      searchClient.indices().delete(new DeleteIndexRequest().indices(indexState.name()), RequestOptions.DEFAULT);
    } else {
      searchClient.indices()
          .delete(new DeleteIndexRequest().indices(aliasesResponse.getAliases().keySet().toArray(new String[0])),
              RequestOptions.DEFAULT);
    }

    // Add alias for the new index
    AliasActions removeAction = AliasActions.remove().alias(indexState.name()).index(indexNamePattern);
    AliasActions addAction = AliasActions.add().alias(indexState.name()).index(tempIndexName);
    searchClient.indices()
        .updateAliases(new IndicesAliasesRequest().addAliasAction(removeAction).addAliasAction(addAction),
            RequestOptions.DEFAULT);
    log.info("Finished setting up {}", indexState.name());
  }

  private Pair<Long, Long> getDocumentCounts(String sourceIndex, String destinationIndex) throws IOException {
    // Check whether reindex succeeded by comparing document count
    // There can be some delay between the reindex finishing and count being fully up to date, so try multiple times
    long originalCount = 0;
    long reindexedCount = 0;
    for (int i = 0; i < this.numRetries; i++) {
      // Check if reindex succeeded by comparing document counts
      originalCount = getCount(sourceIndex);
      reindexedCount = getCount(destinationIndex);
      if (originalCount == reindexedCount) {
        break;
      }
      try {
        Thread.sleep(20 * 1000);
      } catch (InterruptedException e) {
        log.warn("Sleep interrupted");
      }
    }

    return Pair.of(originalCount, reindexedCount);
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
