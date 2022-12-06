package com.linkedin.datahub.upgrade.restoreindices;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.common.steps.ClearGraphServiceStep;
import com.linkedin.datahub.upgrade.common.steps.ClearSearchServiceStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import io.ebean.EbeanServer;
import java.util.ArrayList;
import java.util.List;


public class RestoreIndices implements Upgrade {
  public static final String BATCH_SIZE_ARG_NAME = "BATCH_SIZE";
  public static final String RESTORE_FROM_PARQUET = "RESTORE_FROM_PARQUET";
  public static final String DRY_RUN = "DRY_RUN";
  public static final String READER_POOL_SIZE = "READER_POOL_SIZE";
  public static final String WRITER_POOL_SIZE = "WRITER_POOL_SIZE";
  public static final String SQL_READER_POOL_SIZE = "SQL_READER_POOL_SIZE";
  public static final String ASPECT_NAME_ARG_NAME = "ASPECT_NAME";
  public static final String URN_LIKE_ARG_NAME = "URN_LIKE";
  public static final String URN_ARG_NAME = "URN";

  private final List<UpgradeStep> _steps;

  public RestoreIndices(final EbeanServer server, final EntityService entityService,
      final EntityRegistry entityRegistry, final EntitySearchService entitySearchService,
      final GraphService graphService) {
    _steps = buildSteps(server, entityService, entityRegistry, entitySearchService, graphService);
  }

  @Override
  public String id() {
    return "RestoreIndices";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(final EbeanServer server, final EntityService entityService,
      final EntityRegistry entityRegistry, final EntitySearchService entitySearchService,
      final GraphService graphService) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new ClearSearchServiceStep(entitySearchService, false));
    steps.add(new ClearGraphServiceStep(graphService, false));
    steps.add(new SendMAEStep(server, entityService, entityRegistry));
    steps.add(new RestoreFromParquetStep(entityService, entityRegistry));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}
