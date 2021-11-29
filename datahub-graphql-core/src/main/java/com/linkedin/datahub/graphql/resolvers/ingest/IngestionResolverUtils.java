package com.linkedin.datahub.graphql.resolvers.ingest;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.ExecutionRequest;
import com.linkedin.datahub.graphql.generated.ExecutionRequestLogs;
import com.linkedin.datahub.graphql.generated.ExecutionRequestSourceType;
import com.linkedin.datahub.graphql.generated.IngestionConfig;
import com.linkedin.datahub.graphql.generated.IngestionRecipe;
import com.linkedin.datahub.graphql.generated.IngestionSchedule;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.IngestionSourceType;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceRecipe;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class IngestionResolverUtils {

  public static List<ExecutionRequest> mapExecutionRequests(final Collection<EntityResponse> requests) {
    List<ExecutionRequest> result = new ArrayList<>();
    for (final EntityResponse request : requests) {
      result.add(mapExecutionRequest(request));
    }
    return result;
  }

  public static ExecutionRequest mapExecutionRequest(final EntityResponse entityResponse) {
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    final ExecutionRequest result = new ExecutionRequest();
    result.setUrn(entityUrn.toString());

    // Map input aspect. Must be present.
    final EnvelopedAspect envelopedInput = aspects.get(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME);
    if (envelopedInput != null) {
      final ExecutionRequestInput executionRequestInput = new ExecutionRequestInput(envelopedInput.getValue().data());
      final com.linkedin.datahub.graphql.generated.ExecutionRequestInput inputResult = new com.linkedin.datahub.graphql.generated.ExecutionRequestInput();

      inputResult.setTask(executionRequestInput.getTask());
      if (executionRequestInput.hasSource()) {
        inputResult.setSource(mapExecutionRequestSource(executionRequestInput.getSource()));
      }
      if (executionRequestInput.hasArgs()) {
        inputResult.setArguments(StringMapMapper.map(executionRequestInput.getArgs()));
      }
    }

    // Map result aspect. Optional.
    final EnvelopedAspect envelopedResult = aspects.get(Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME);
    if (envelopedResult != null) {
      final ExecutionRequestResult executionRequestResult = new ExecutionRequestResult(envelopedResult.getValue().data());
      result.setResult(mapExecutionRequestResult(executionRequestResult));
    }

    return result;
  }

  public static com.linkedin.datahub.graphql.generated.ExecutionRequestSource mapExecutionRequestSource(final ExecutionRequestSource execRequestSource) {
    final com.linkedin.datahub.graphql.generated.ExecutionRequestSource result = new com.linkedin.datahub.graphql.generated.ExecutionRequestSource();
    result.setType(ExecutionRequestSourceType.valueOf(execRequestSource.getType()));
    return result;
  }

  public static com.linkedin.datahub.graphql.generated.ExecutionRequestResult mapExecutionRequestResult(final ExecutionRequestResult execRequestResult) {
    final com.linkedin.datahub.graphql.generated.ExecutionRequestResult result = new com.linkedin.datahub.graphql.generated.ExecutionRequestResult();
    result.setResult(execRequestResult.getResult());
    result.setStartTimeMs(execRequestResult.getStartTimeMs());
    result.setDurationMs(execRequestResult.getDurationMs());

    // Mock the logs for now.
    final ExecutionRequestLogs logs = new ExecutionRequestLogs();
    logs.setStdErr("Captured stdErr.\nyou know nothing");
    logs.setStdOut("Captured stdOut.\nyou know nothing");
    result.setLogs(logs);
    return result;
  }

  public static List<IngestionSource> mapIngestionSources(final Collection<EntityResponse> entities) {
    final List<IngestionSource> results = new ArrayList<>();
    for (EntityResponse response : entities) {
      results.add(mapIngestionSource(response));
    }
    return results;
  }

  public static IngestionSource mapIngestionSource(final EntityResponse ingestionSource) {
    final Urn entityUrn = ingestionSource.getUrn();
    final EnvelopedAspectMap aspects = ingestionSource.getAspects();

    // There should ALWAYS be an info aspect.
    final EnvelopedAspect envelopedInfo = aspects.get(Constants.INGESTION_INFO_ASPECT_NAME);

    // Bind into a strongly typed object.
    final DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo(envelopedInfo.getValue().data());

    return mapIngestionSourceInfo(entityUrn, ingestionSourceInfo);
  }

  public static IngestionSource mapIngestionSourceInfo(final Urn urn, final DataHubIngestionSourceInfo info) {
    final IngestionSource result = new IngestionSource();
    result.setUrn(urn.toString());
    result.setDisplayName(info.getDisplayName());
    result.setSourceType(IngestionSourceType.valueOf(info.getType()));
    result.setConfig(mapIngestionSourceConfig(info.getConfig()));
    if (info.hasSchedule()) {
      result.setSchedule(mapIngestionSourceSchedule(info.getSchedule()));
    }
    return result;
  }

  public static IngestionConfig mapIngestionSourceConfig(final DataHubIngestionSourceConfig config) {
    final IngestionConfig result = new IngestionConfig();
    if (config.hasRecipe()) {
      result.setRecipe(mapIngestionSourceRecipe(config.getRecipe()));
    }
    return result;
  }

  public static IngestionRecipe mapIngestionSourceRecipe(final DataHubIngestionSourceRecipe recipe) {
    final IngestionRecipe result = new IngestionRecipe();
    result.setJson(recipe.getJson());
    return result;
  }

  public static IngestionSchedule mapIngestionSourceSchedule(final DataHubIngestionSourceSchedule schedule) {
    final IngestionSchedule result = new IngestionSchedule();
    result.setInterval(schedule.getInterval());
    result.setStartTimeMs(schedule.getStartTimeMs());
    if (schedule.hasEndTimeMs()) {
      result.setEndTimeMs(schedule.getEndTimeMs());
    }
    return result;
  }

  private IngestionResolverUtils() { }
}
