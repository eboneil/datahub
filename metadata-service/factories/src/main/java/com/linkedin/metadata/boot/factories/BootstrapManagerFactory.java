package com.linkedin.metadata.boot.factories;

import com.linkedin.gms.factory.assertions.AssertionServiceFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.incident.IncidentServiceFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.gms.factory.search.SearchDocumentTransformerFactory;
import com.linkedin.metadata.boot.BootstrapManager;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.boot.dependencies.BootstrapDependency;
<<<<<<< HEAD
import com.linkedin.metadata.boot.steps.MigrateAssertionsSummaryStep;
=======
import com.linkedin.metadata.boot.steps.BackfillBrowsePathsV2Step;
>>>>>>> oss_master
import com.linkedin.metadata.boot.steps.IndexDataPlatformsStep;
import com.linkedin.metadata.boot.steps.IngestDataPlatformInstancesStep;
import com.linkedin.metadata.boot.steps.IngestDataPlatformsStep;
import com.linkedin.metadata.boot.steps.IngestDefaultGlobalSettingsStep;
import com.linkedin.metadata.boot.steps.IngestOwnershipTypesStep;
import com.linkedin.metadata.boot.steps.IngestDefaultTagsStep;
import com.linkedin.metadata.boot.steps.IngestMetadataTestsStep;
import com.linkedin.metadata.boot.steps.IngestPoliciesStep;
import com.linkedin.metadata.boot.steps.IngestRetentionPoliciesStep;
import com.linkedin.metadata.boot.steps.IngestRolesStep;
import com.linkedin.metadata.boot.steps.IngestRootUserStep;
import com.linkedin.metadata.boot.steps.MigrateIncidentsSummaryStep;
import com.linkedin.metadata.boot.steps.RemoveClientIdAspectStep;
import com.linkedin.metadata.boot.steps.RestoreColumnLineageIndices;
import com.linkedin.metadata.boot.steps.RestoreDbtSiblingsIndices;
import com.linkedin.metadata.boot.steps.RestoreGlossaryIndices;
import com.linkedin.metadata.boot.steps.UpgradeDefaultBrowsePathsStep;
import com.linkedin.metadata.boot.steps.WaitForSystemUpdateStep;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.IncidentService;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;


@Configuration
@Import({EntityServiceFactory.class, EntityRegistryFactory.class, EntitySearchServiceFactory.class,
    SearchDocumentTransformerFactory.class, AssertionServiceFactory.class, IncidentServiceFactory.class})
public class BootstrapManagerFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry _entityRegistry;

  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService _entitySearchService;

  @Autowired
  @Qualifier("searchService")
  private SearchService _searchService;

  @Autowired
  @Qualifier("searchDocumentTransformer")
  private SearchDocumentTransformer _searchDocumentTransformer;

  @Autowired
  @Qualifier("assertionService")
  private AssertionService _assertionService;

  @Autowired
  @Qualifier("incidentService")
  private IncidentService _incidentService;

  @Autowired
  @Qualifier("timeseriesAspectService")
  private TimeseriesAspectService _timeseriesAspectService;

  @Autowired
  @Qualifier("entityAspectMigrationsDao")
  private AspectMigrationsDao _migrationsDao;

  @Autowired
  @Qualifier("ingestRetentionPoliciesStep")
  private IngestRetentionPoliciesStep _ingestRetentionPoliciesStep;

  @Autowired
  @Qualifier("dataHubUpgradeKafkaListener")
  private BootstrapDependency _dataHubUpgradeKafkaListener;

  @Autowired
  private ConfigurationProvider _configurationProvider;

  @Value("${bootstrap.upgradeDefaultBrowsePaths.enabled}")
  private Boolean _upgradeDefaultBrowsePathsEnabled;

<<<<<<< HEAD
  // Saas-only
  @Autowired
  @Qualifier("ingestMetadataTestsStep")
  private IngestMetadataTestsStep _ingestMetadataTestsStep;
=======
  @Value("${bootstrap.backfillBrowsePathsV2.enabled}")
  private Boolean _backfillBrowsePathsV2Enabled;
>>>>>>> oss_master

  @Bean(name = "bootstrapManager")
  @Scope("singleton")
  @Nonnull
  protected BootstrapManager createInstance() {
    final IngestRootUserStep ingestRootUserStep = new IngestRootUserStep(_entityService);
    final IngestPoliciesStep ingestPoliciesStep =
        new IngestPoliciesStep(_entityRegistry, _entityService, _entitySearchService, _searchDocumentTransformer);
    final IngestRolesStep ingestRolesStep = new IngestRolesStep(_entityService, _entityRegistry);
    final IngestDataPlatformsStep ingestDataPlatformsStep = new IngestDataPlatformsStep(_entityService);
    final IngestDataPlatformInstancesStep ingestDataPlatformInstancesStep =
        new IngestDataPlatformInstancesStep(_entityService, _migrationsDao);
    final RestoreGlossaryIndices restoreGlossaryIndicesStep =
        new RestoreGlossaryIndices(_entityService, _entitySearchService, _entityRegistry);
    final IndexDataPlatformsStep indexDataPlatformsStep =
        new IndexDataPlatformsStep(_entityService, _entitySearchService, _entityRegistry);
    final RestoreDbtSiblingsIndices restoreDbtSiblingsIndices =
        new RestoreDbtSiblingsIndices(_entityService, _entityRegistry);
    final RemoveClientIdAspectStep removeClientIdAspectStep = new RemoveClientIdAspectStep(_entityService);
    final RestoreColumnLineageIndices restoreColumnLineageIndices = new RestoreColumnLineageIndices(_entityService, _entityRegistry);
    final IngestDefaultGlobalSettingsStep ingestSettingsStep = new IngestDefaultGlobalSettingsStep(_entityService);
    final WaitForSystemUpdateStep waitForSystemUpdateStep = new WaitForSystemUpdateStep(_dataHubUpgradeKafkaListener,
        _configurationProvider);
    final IngestOwnershipTypesStep ingestOwnershipTypesStep = new IngestOwnershipTypesStep(_entityService);
    final IngestDefaultTagsStep ingestDefaultTagsStep = new IngestDefaultTagsStep(_entityService);

    final MigrateAssertionsSummaryStep assertionsSummaryStep =
        new MigrateAssertionsSummaryStep(_entityService, _entitySearchService, _assertionService, _timeseriesAspectService,
            _configurationProvider);
    final MigrateIncidentsSummaryStep incidentsSummaryStep =
        new MigrateIncidentsSummaryStep(_entityService, _entitySearchService, _incidentService);

    final List<BootstrapStep> finalSteps = Stream.of(
            waitForSystemUpdateStep,
            ingestRootUserStep,
            ingestPoliciesStep,
            ingestRolesStep,
            ingestDataPlatformsStep,
            ingestDataPlatformInstancesStep,
            _ingestRetentionPoliciesStep,
            ingestOwnershipTypesStep,
            ingestSettingsStep,
            restoreGlossaryIndicesStep,
            removeClientIdAspectStep,
            restoreDbtSiblingsIndices,
            indexDataPlatformsStep,
            restoreColumnLineageIndices,
            assertionsSummaryStep,
            incidentsSummaryStep,
            // Saas-only
            _ingestMetadataTestsStep,
            ingestDefaultTagsStep
        ).filter(Objects::nonNull)
        .collect(Collectors.toList());

    if (_upgradeDefaultBrowsePathsEnabled) {
      finalSteps.add(new UpgradeDefaultBrowsePathsStep(_entityService));
    }

    if (_backfillBrowsePathsV2Enabled) {
      finalSteps.add(new BackfillBrowsePathsV2Step(_entityService, _searchService));
    }

    return new BootstrapManager(finalSteps);
  }
}
