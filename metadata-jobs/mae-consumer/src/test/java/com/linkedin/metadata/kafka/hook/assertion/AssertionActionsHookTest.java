package com.linkedin.metadata.kafka.hook.assertion;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.anomaly.AnomalyInfo;
import com.linkedin.anomaly.AnomalySource;
import com.linkedin.anomaly.AnomalySourceProperties;
import com.linkedin.anomaly.AnomalySourceType;
import com.linkedin.anomaly.AnomalyState;
import com.linkedin.anomaly.AnomalyStatus;
import com.linkedin.anomaly.AnomalyType;
import com.linkedin.assertion.AssertionAction;
import com.linkedin.assertion.AssertionActionArray;
import com.linkedin.assertion.AssertionActionType;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;


public class AssertionActionsHookTest {
  private static final EntityRegistry ENTITY_REGISTRY = new ConfigEntityRegistry(
      AssertionsSummaryHookTest.class.getClassLoader().getResourceAsStream("test-entity-registry.yml"));
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_INCIDENT_URN = UrnUtils.getUrn("urn:li:incident:test-1");
  private static final Urn TEST_ANOMALY_URN = UrnUtils.getUrn("urn:li:anomaly:test-2");
  private static final Urn TEST_DATASET_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");

  @Test
  public void testInvokeNotEnabled() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        false);

    final MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        buildAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));
    hook.invoke(event);

    Mockito.verify(entityClient, Mockito.times(0)).getV2(
        Mockito.anyString(),
        Mockito.any(Urn.class),
        Mockito.anySet(),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testInvokeNotEligibleChange() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    // Case 1: Incorrect aspect --- Assertion Info
    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_INFO_ASPECT_NAME,
        ChangeType.UPSERT,
        new AssertionInfo());
    hook.invoke(event);
    Mockito.verify(entityClient, Mockito.times(0)).getV2(
        Mockito.anyString(),
        Mockito.any(Urn.class),
        Mockito.anySet(),
        Mockito.any(Authentication.class)
    );

    // Case 2: Run Event But Delete
    event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.DELETE,
        buildAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));
    hook.invoke(event);
    Mockito.verify(entityClient, Mockito.times(0)).getV2(
        Mockito.anyString(),
        Mockito.any(Urn.class),
        Mockito.anySet(),
        Mockito.any(Authentication.class)
    );

    // Case 3: Status aspect but for the wrong entity type
    event = buildMetadataChangeLog(
        TEST_DATASET_URN,
        STATUS_ASPECT_NAME,
        ChangeType.UPSERT,
        buildAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));
    hook.invoke(event);
    Mockito.verify(entityClient, Mockito.times(0)).getV2(
        Mockito.anyString(),
        Mockito.any(Urn.class),
        Mockito.anySet(),
        Mockito.any(Authentication.class)
    );

    // Case 4: Run event but not complete
    event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        buildAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.$UNKNOWN, AssertionResultType.SUCCESS));
    hook.invoke(event);
    Mockito.verify(entityClient, Mockito.times(0)).getV2(
        Mockito.anyString(),
        Mockito.any(Urn.class),
        Mockito.anySet(),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testInvokeAssertionRunEventSuccessNoActions() throws Exception {
    EntityClient entityClient = mockEntityClient(
        null,
        null,
        TEST_ASSERTION_URN,
        new AssertionInfo()
        .setType(AssertionType.DATASET)
        .setDatasetAssertion(new DatasetAssertionInfo()
            .setDataset(TEST_DATASET_URN)
            .setScope(DatasetAssertionScope.DATASET_COLUMN)
        ),
        new AssertionActions()
            .setOnFailure(new AssertionActionArray())
            .setOnSuccess(new AssertionActionArray())
    );
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        buildAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));

    hook.invoke(event);

    // Ensure that we looked up the assertion actions correctly.
    Mockito.verify(entityClient, Mockito.times(2)).getV2(
        Mockito.eq(ASSERTION_ENTITY_NAME),
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Ensure that we did not apply any actions or look up anything for incidents.
    Mockito.verify(entityClient, Mockito.times(0)).search(
        Mockito.eq(INCIDENT_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.any(SortCriterion.class),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    );

    // No ingestion to perform
    Mockito.verify(entityClient, Mockito.times(0)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(Authentication.class),
        Mockito.anyBoolean()
    );
  }

  @Test
  public void testInvokeAssertionRunEventSuccessActionsNoIncident() throws Exception {

    EntityClient entityClient = mockEntityClient(
        null,
        null,
        TEST_ASSERTION_URN,
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo()
                .setDataset(TEST_DATASET_URN)
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
            ),
        new AssertionActions()
            .setOnFailure(new AssertionActionArray())
            .setOnSuccess(new AssertionActionArray(
                ImmutableList.of(
                    new AssertionAction()
                      .setType(AssertionActionType.RESOLVE_INCIDENT)
                )
            ))
    );
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        buildAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));

    hook.invoke(event);

    // Ensure that we looked up the assertion actions correctly.
    Mockito.verify(entityClient, Mockito.times(2)).getV2(
        Mockito.eq(ASSERTION_ENTITY_NAME),
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Ensure that we searched for the active incidents associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1)).search(
        Mockito.eq(INCIDENT_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.eq(null),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    );

    // Verify that nothing was ingested in this case
    Mockito.verify(entityClient, Mockito.times(0)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(Authentication.class),
        Mockito.anyBoolean()
    );
  }

  @Test
  public void testInvokeAssertionRunEventSuccessActionsActiveIncident() throws Exception {
    IncidentInfo activeIncidentInfo = new IncidentInfo()
        .setType(IncidentType.DATASET_COLUMN)
        .setPriority(2)
        .setTitle("Test Title")
        .setDescription("Test description")
        .setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)))
        .setStatus(new IncidentStatus()
            .setState(IncidentState.ACTIVE)
            .setLastUpdated(new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(SYSTEM_ACTOR)))
        )
        .setSource(
            new IncidentSource()
                .setSourceUrn(TEST_ASSERTION_URN)
                .setType(IncidentSourceType.ASSERTION_FAILURE)

        )
        .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(0L));

    EntityClient entityClient = mockEntityClient(TEST_INCIDENT_URN,
        activeIncidentInfo,
        TEST_ASSERTION_URN,
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo()
                .setDataset(TEST_DATASET_URN)
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
            ),
        new AssertionActions()
            .setOnFailure(new AssertionActionArray())
            .setOnSuccess(new AssertionActionArray(
                ImmutableList.of(
                    new AssertionAction()
                        .setType(AssertionActionType.RESOLVE_INCIDENT)
                )
            ))
    );
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        buildAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));

    hook.invoke(event);

    // Ensure that we looked up the assertion info correctly.
    Mockito.verify(entityClient, Mockito.times(2)).getV2(
        Mockito.eq(ASSERTION_ENTITY_NAME),
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Ensure that we searched for the active incidents associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1)).search(
        Mockito.eq(INCIDENT_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.eq(null),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    );

    IncidentInfo expectedInfo = new IncidentInfo(activeIncidentInfo.data());
    expectedInfo.setStatus(new IncidentStatus()
        .setState(IncidentState.RESOLVED)
        .setMessage("Auto-Resolved: The failing assertion which generated this incident is now passing.")
        .setLastUpdated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(0L))
    );

    Mockito.verify(entityClient, Mockito.times(1)).ingestProposal(
        Mockito.argThat(new AssertionActionsHookIncidentInfoMatcher(
            AspectUtils.buildMetadataChangeProposal(
                TEST_INCIDENT_URN,
                INCIDENT_INFO_ASPECT_NAME,
                expectedInfo
            )
        )),
        Mockito.any(Authentication.class),
        Mockito.anyBoolean()
    );
  }

  @Test
  public void testInvokeInferredAssertionRunEventSuccessNoAnomaly() throws Exception {
    EntityClient entityClient = mockEntityClient(
        null,
        null,
        TEST_ASSERTION_URN,
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setSource(new AssertionSource().setType(AssertionSourceType.INFERRED)) // Inferred Assertion!
            .setDatasetAssertion(new DatasetAssertionInfo()
                .setDataset(TEST_DATASET_URN)
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
            )
    );
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        buildAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));

    hook.invoke(event);

    // Ensure that we looked up the assertion actions correctly.
    Mockito.verify(entityClient, Mockito.times(2)).getV2(
        Mockito.eq(ASSERTION_ENTITY_NAME),
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Ensure that we searched for the active anomalies associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1)).search(
        Mockito.eq(ANOMALY_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.eq(null),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    );

    // Verify that nothing was ingested in this case -- no anomalies to ingest since it was a success.
    Mockito.verify(entityClient, Mockito.times(0)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(Authentication.class),
        Mockito.anyBoolean()
    );
  }

  @Test
  public void testInvokeAssertionRunEventSuccessActionsActiveAnomaly() throws Exception {
    AnomalyInfo activeAnomalyInfo = new AnomalyInfo()
        .setEntity(TEST_DATASET_URN)
        .setStatus(new AnomalyStatus()
            .setState(AnomalyState.ACTIVE)
            .setLastUpdated(new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(SYSTEM_ACTOR)))
        )
        .setSource(
            new AnomalySource()
                .setSourceUrn(TEST_ASSERTION_URN)
                .setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE)
        )
        .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(0L))
        .setType(AnomalyType.DATASET_COLUMN);

    EntityClient entityClient = mockEntityClient(
        TEST_ANOMALY_URN,
        activeAnomalyInfo,
        TEST_ASSERTION_URN,
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setSource(new AssertionSource().setType(AssertionSourceType.INFERRED)) // Inferred Assertion!
            .setDatasetAssertion(new DatasetAssertionInfo()
                .setDataset(TEST_DATASET_URN)
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
            )
    );
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        buildAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));

    hook.invoke(event);

    // Ensure that we looked up the assertion info correctly.
    Mockito.verify(entityClient, Mockito.times(2)).getV2(
        Mockito.eq(ASSERTION_ENTITY_NAME),
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Ensure that we searched for the active anomalies associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1)).search(
        Mockito.eq(ANOMALY_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.eq(null),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    );

    AnomalyInfo expectedInfo = new AnomalyInfo(activeAnomalyInfo.data());
    expectedInfo.setStatus(new AnomalyStatus()
        .setState(AnomalyState.RESOLVED)
        .setLastUpdated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(0L))
    );

    Mockito.verify(entityClient, Mockito.times(1)).ingestProposal(
        Mockito.argThat(new AssertionActionsHookAnomalyInfoMatcher(
            AspectUtils.buildMetadataChangeProposal(
                TEST_ANOMALY_URN,
                ANOMALY_INFO_ASPECT_NAME,
                expectedInfo
            )
        )),
        Mockito.any(Authentication.class),
        Mockito.anyBoolean()
    );
  }

  @Test
  public void testInvokeAssertionRunEventFailureNoActions() throws Exception {
    EntityClient entityClient = mockEntityClient(
        null,
        null,
        TEST_ASSERTION_URN,
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo()
                .setDataset(TEST_DATASET_URN)
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
            ),
        new AssertionActions()
            .setOnFailure(new AssertionActionArray())
            .setOnSuccess(new AssertionActionArray())
    );
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        buildAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE));

    hook.invoke(event);

    // Ensure that we looked up the assertion actions correctly.
    Mockito.verify(entityClient, Mockito.times(2)).getV2(
        Mockito.eq(ASSERTION_ENTITY_NAME),
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Ensure that we did not apply any actions or look up anything for incidents.
    Mockito.verify(entityClient, Mockito.times(0)).search(
        Mockito.eq(INCIDENT_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.any(SortCriterion.class),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    );

    // No ingestion to perform
    Mockito.verify(entityClient, Mockito.times(0)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(Authentication.class),
        Mockito.anyBoolean()
    );
  }

  @Test
  public void testInvokeAssertionRunEventFailureActionsNoIncident() throws Exception {
    EntityClient entityClient = mockEntityClient(
        null,
        null,
        TEST_ASSERTION_URN,
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo()
                .setDataset(TEST_DATASET_URN)
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
            ),
        new AssertionActions()
            .setOnSuccess(new AssertionActionArray())
            .setOnFailure(new AssertionActionArray(
                ImmutableList.of(
                    new AssertionAction()
                        .setType(AssertionActionType.RAISE_INCIDENT)
                )
            ))
    );
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        buildAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE));

    hook.invoke(event);

    // Ensure that we looked up the assertion actions correctly.
    Mockito.verify(entityClient, Mockito.times(2)).getV2(
        Mockito.eq(ASSERTION_ENTITY_NAME),
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Ensure that we searched for the active incidents associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1)).search(
        Mockito.eq(INCIDENT_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.eq(null),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    );

    IncidentInfo expectedInfo = new IncidentInfo();
    expectedInfo.setType(IncidentType.DATASET_COLUMN);
    expectedInfo.setTitle("A critical Assertion is failing for this asset.");
    expectedInfo.setDescription("A critical Assertion has failed for this data asset. "
        + "This may indicate that the asset is unhealthy or unfit for consumption!");
    expectedInfo.setPriority(0);
    expectedInfo.setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)));
    expectedInfo.setSource(new IncidentSource()
      .setType(IncidentSourceType.ASSERTION_FAILURE)
      .setSourceUrn(TEST_ASSERTION_URN)
    );
    expectedInfo.setStatus(new IncidentStatus()
      .setState(IncidentState.ACTIVE)
      .setMessage("Auto-Raised: This incident was automatically generated by a failing assertion.")
      .setLastUpdated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(0L))
    );
    expectedInfo.setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(0L));

    // Verify that a new assertion was created
    Mockito.verify(entityClient, Mockito.times(1)).ingestProposal(
        Mockito.argThat(new AssertionActionsHookIncidentInfoMatcher(
            AspectUtils.buildMetadataChangeProposal(TEST_INCIDENT_URN,
                INCIDENT_INFO_ASPECT_NAME,
                expectedInfo
            )
        )),
        Mockito.any(Authentication.class),
        Mockito.anyBoolean()
    );
  }

  @Test
  public void testInvokeAssertionRunEventFailureActionsWithIncident() throws Exception {
    IncidentInfo activeIncidentInfo = new IncidentInfo()
        .setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)))
        .setStatus(new IncidentStatus()
            .setState(IncidentState.ACTIVE)
            .setLastUpdated(new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(SYSTEM_ACTOR)))
        )
        .setSource(
            new IncidentSource()
                .setSourceUrn(TEST_ASSERTION_URN)
                .setType(IncidentSourceType.ASSERTION_FAILURE)

        )
        .setType(IncidentType.DATASET_COLUMN);

    EntityClient entityClient = mockEntityClient(TEST_INCIDENT_URN,
        activeIncidentInfo,
        TEST_ASSERTION_URN,
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo()
                .setDataset(TEST_DATASET_URN)
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
            ),
        new AssertionActions()
            .setOnSuccess(new AssertionActionArray())
            .setOnFailure(new AssertionActionArray(
                ImmutableList.of(
                    new AssertionAction()
                        .setType(AssertionActionType.RAISE_INCIDENT)
                )
            ))
    );
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        buildAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE));

    hook.invoke(event);

    // Ensure that we looked up the assertion info correctly.
    Mockito.verify(entityClient, Mockito.times(2)).getV2(
        Mockito.eq(ASSERTION_ENTITY_NAME),
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Ensure that we searched for the active incidents associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1)).search(
        Mockito.eq(INCIDENT_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.eq(null),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    );

    // Verify that nothing was ingested due to existing active incident
    Mockito.verify(entityClient, Mockito.times(0)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(Authentication.class),
        Mockito.anyBoolean()
    );
  }

  @Test
  public void testInvokeInferredAssertionRunEventFailureNoAnomaly() throws Exception {

    EntityClient entityClient = mockEntityClient(
        null,
        null,
        TEST_ASSERTION_URN,
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setSource(new AssertionSource().setType(AssertionSourceType.INFERRED)) // Inferred Assertion!
            .setDatasetAssertion(new DatasetAssertionInfo()
                .setDataset(TEST_DATASET_URN)
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
            )
    );
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        buildAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE));

    hook.invoke(event);

    // Ensure that we looked up the assertion info correctly.
    Mockito.verify(entityClient, Mockito.times(2)).getV2(
        Mockito.eq(ASSERTION_ENTITY_NAME),
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Ensure that we searched for the active anomalies associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1)).search(
        Mockito.eq(ANOMALY_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.eq(null),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    );

    AnomalyInfo expectedInfo = new AnomalyInfo();
    expectedInfo.setType(AnomalyType.DATASET_COLUMN);
    expectedInfo.setEntity(TEST_DATASET_URN);
    expectedInfo.setSource(new AnomalySource()
        .setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE)
        .setSourceUrn(TEST_ASSERTION_URN)
        .setProperties(new AnomalySourceProperties().setAssertionRunEventTime(1L))
    );
    expectedInfo.setStatus(new AnomalyStatus()
        .setState(AnomalyState.ACTIVE)
        .setLastUpdated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(0L))
    );
    expectedInfo.setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(0L));

    // Verify that a new anomaly was created
    Mockito.verify(entityClient, Mockito.times(1)).ingestProposal(
        Mockito.argThat(new AssertionActionsHookAnomalyInfoMatcher(
            AspectUtils.buildMetadataChangeProposal(
                TEST_ANOMALY_URN,
                ANOMALY_INFO_ASPECT_NAME,
                expectedInfo
            )
        )),
        Mockito.any(Authentication.class),
        Mockito.anyBoolean()
    );
  }

  @Test
  public void testInvokeInferredAssertionRunEventFailureWithAnomaly() throws Exception {
    AnomalyInfo activeAnomalyInfo = new AnomalyInfo()
        .setEntity(TEST_DATASET_URN)
        .setStatus(new AnomalyStatus()
            .setState(AnomalyState.ACTIVE)
            .setLastUpdated(new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(SYSTEM_ACTOR)))
        )
        .setSource(
            new AnomalySource()
                .setSourceUrn(TEST_ASSERTION_URN)
                .setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE)
        )
        .setType(AnomalyType.DATASET_COLUMN);

    EntityClient entityClient = mockEntityClient(
        TEST_ANOMALY_URN,
        activeAnomalyInfo,
        TEST_ASSERTION_URN,
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setSource(new AssertionSource().setType(AssertionSourceType.INFERRED)) // Inferred Assertion!
            .setDatasetAssertion(new DatasetAssertionInfo()
                .setDataset(TEST_DATASET_URN)
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
            )
    );
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        buildAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE));

    hook.invoke(event);

    // Ensure that we looked up the assertion info correctly.
    Mockito.verify(entityClient, Mockito.times(2)).getV2(
        Mockito.eq(ASSERTION_ENTITY_NAME),
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Ensure that we searched for the active anomalies associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1)).search(
        Mockito.eq(ANOMALY_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.eq(null),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    );

    // Verify that no anomaly is raised since one already exists!
    Mockito.verify(entityClient, Mockito.times(0)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(Authentication.class),
        Mockito.anyBoolean()
    );
  }

  @Test
  public void testInvokeAssertionSoftDeletedActiveIncident() throws Exception {
    // If assertion is soft deleted, then any active incidents resulting from it should be removed.

    IncidentInfo activeIncidentInfo = new IncidentInfo()
        .setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)))
        .setStatus(new IncidentStatus()
            .setState(IncidentState.ACTIVE)
            .setLastUpdated(new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(SYSTEM_ACTOR)))
        )
        .setSource(
            new IncidentSource()
                .setSourceUrn(TEST_ASSERTION_URN)
                .setType(IncidentSourceType.ASSERTION_FAILURE)

        )
        .setType(IncidentType.DATASET_COLUMN);

    EntityClient entityClient = mockEntityClient(TEST_INCIDENT_URN,
        activeIncidentInfo,
        TEST_ASSERTION_URN,
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo()
                .setDataset(TEST_DATASET_URN)
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
            ),
        new AssertionActions()
            .setOnSuccess(new AssertionActionArray())
            .setOnFailure(new AssertionActionArray())
    );
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        STATUS_ASPECT_NAME,
        ChangeType.UPSERT,
        buildStatus(true));

    hook.invoke(event);

    Mockito.verify(entityClient, Mockito.times(0)).getV2(
        Mockito.eq(ASSERTION_ENTITY_NAME),
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Ensure that we searched for the active incidents associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1)).search(
        Mockito.eq(INCIDENT_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.eq(null),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    );

    // Verify that we've deleted the active incidents.
    Mockito.verify(entityClient, Mockito.times(1)).deleteEntity(
        Mockito.eq(TEST_INCIDENT_URN),
        Mockito.any(Authentication.class)
    );
    Mockito.verify(entityClient, Mockito.times(1)).deleteEntityReferences(
        Mockito.eq(TEST_INCIDENT_URN),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testInvokeAssertionHardDeletedActiveIncident() throws Exception {
    // If assertion is hard deleted, then any active incidents resulting from it should be removed.

    IncidentInfo activeIncidentInfo = new IncidentInfo()
        .setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)))
        .setStatus(new IncidentStatus()
            .setState(IncidentState.ACTIVE)
            .setLastUpdated(new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(SYSTEM_ACTOR)))
        )
        .setSource(
            new IncidentSource()
                .setSourceUrn(TEST_ASSERTION_URN)
                .setType(IncidentSourceType.ASSERTION_FAILURE)

        )
        .setType(IncidentType.DATASET_COLUMN);

    EntityClient entityClient = mockEntityClient(TEST_INCIDENT_URN,
        activeIncidentInfo,
        TEST_ASSERTION_URN,
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo()
                .setDataset(TEST_DATASET_URN)
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
            ),
        new AssertionActions()
            .setOnSuccess(new AssertionActionArray())
            .setOnFailure(new AssertionActionArray())
    );
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_KEY_ASPECT_NAME,
        ChangeType.DELETE,
        null);

    hook.invoke(event);

    Mockito.verify(entityClient, Mockito.times(0)).getV2(
        Mockito.eq(ASSERTION_ENTITY_NAME),
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Ensure that we searched for the active incidents associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1)).search(
        Mockito.eq(INCIDENT_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.eq(null),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    );

    // Verify that we've deleted the active incidents.
    Mockito.verify(entityClient, Mockito.times(1)).deleteEntity(
        Mockito.eq(TEST_INCIDENT_URN),
        Mockito.any(Authentication.class)
    );
    Mockito.verify(entityClient, Mockito.times(1)).deleteEntityReferences(
        Mockito.eq(TEST_INCIDENT_URN),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testInvokeAssertionSoftDeletedActiveAnomaly() throws Exception {
    // If assertion is soft deleted, then any active anomalies resulting from it should be removed.

    AnomalyInfo activeAnomalyInfo = new AnomalyInfo()
        .setEntity(TEST_DATASET_URN)
        .setStatus(new AnomalyStatus()
            .setState(AnomalyState.ACTIVE)
            .setLastUpdated(new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(SYSTEM_ACTOR)))
        )
        .setSource(
            new AnomalySource()
                .setSourceUrn(TEST_ASSERTION_URN)
                .setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE)
        )
        .setType(AnomalyType.DATASET_COLUMN);

    EntityClient entityClient = mockEntityClient(
        TEST_ANOMALY_URN,
        activeAnomalyInfo,
        TEST_ASSERTION_URN,
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo()
                .setDataset(TEST_DATASET_URN)
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
            )
    );
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        STATUS_ASPECT_NAME,
        ChangeType.UPSERT,
        buildStatus(true));

    hook.invoke(event);

    Mockito.verify(entityClient, Mockito.times(0)).getV2(
        Mockito.eq(ASSERTION_ENTITY_NAME),
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Ensure that we searched for the active anomalies associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1)).search(
        Mockito.eq(ANOMALY_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.eq(null),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    );

    // Verify that we've deleted the active anomalies.
    Mockito.verify(entityClient, Mockito.times(1)).deleteEntity(
        Mockito.eq(TEST_ANOMALY_URN),
        Mockito.any(Authentication.class)
    );
    Mockito.verify(entityClient, Mockito.times(1)).deleteEntityReferences(
        Mockito.eq(TEST_ANOMALY_URN),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testInvokeAssertionHardDeletedActiveAnomaly() throws Exception {
    // If assertion is hard deleted, then any active anomalies resulting from it should be removed.

    AnomalyInfo activeAnomalyInfo = new AnomalyInfo()
        .setEntity(TEST_DATASET_URN)
        .setStatus(new AnomalyStatus()
            .setState(AnomalyState.ACTIVE)
            .setLastUpdated(new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(SYSTEM_ACTOR)))
        )
        .setSource(
            new AnomalySource()
                .setSourceUrn(TEST_ASSERTION_URN)
                .setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE)
        )
        .setType(AnomalyType.DATASET_COLUMN);

    EntityClient entityClient = mockEntityClient(
        TEST_ANOMALY_URN,
        activeAnomalyInfo,
        TEST_ASSERTION_URN,
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo()
                .setDataset(TEST_DATASET_URN)
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
            )
    );
    Authentication authentication = Mockito.mock(Authentication.class);

    final AssertionActionsHook hook = new AssertionActionsHook(
        ENTITY_REGISTRY,
        entityClient,
        authentication,
        true);

    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_KEY_ASPECT_NAME,
        ChangeType.DELETE,
        null);

    hook.invoke(event);

    // Ensure that we looked up the assertion info correctly.
    Mockito.verify(entityClient, Mockito.times(0)).getV2(
        Mockito.eq(ASSERTION_ENTITY_NAME),
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Ensure that we searched for the active anomalies associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1)).search(
        Mockito.eq(ANOMALY_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.eq(null),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    );

    // Verify that we've deleted the active anomalies.
    Mockito.verify(entityClient, Mockito.times(1)).deleteEntity(
        Mockito.eq(TEST_ANOMALY_URN),
        Mockito.any(Authentication.class)
    );
    Mockito.verify(entityClient, Mockito.times(1)).deleteEntityReferences(
        Mockito.eq(TEST_ANOMALY_URN),
        Mockito.any(Authentication.class)
    );
  }

  private EntityClient mockEntityClient(
      Urn incidentUrn,
      IncidentInfo incidentInfo,
      Urn assertionUrn,
      AssertionInfo assertionInfo,
      AssertionActions assertionActions) throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    if (incidentUrn != null) {
      Mockito.when(mockClient.getV2(
          Mockito.eq(INCIDENT_ENTITY_NAME),
          Mockito.eq(incidentUrn),
          Mockito.eq(ImmutableSet.of(INCIDENT_INFO_ASPECT_NAME)),
          Mockito.any(Authentication.class)
      ))
          .thenReturn(
              new EntityResponse()
                  .setUrn(incidentUrn)
                  .setEntityName(INCIDENT_ENTITY_NAME)
                  .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                      INCIDENT_INFO_ASPECT_NAME,
                      new EnvelopedAspect()
                          .setName(INCIDENT_INFO_ASPECT_NAME)
                          .setValue(new Aspect(incidentInfo.data()))
                  )))
          );
    }

    if (assertionUrn != null) {
      Mockito.when(mockClient.getV2(
          Mockito.eq(ASSERTION_ENTITY_NAME),
          Mockito.eq(assertionUrn),
          Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
          Mockito.any(Authentication.class)
      ))
          .thenReturn(
              new EntityResponse()
                  .setUrn(assertionUrn)
                  .setEntityName(ASSERTION_ENTITY_NAME)
                  .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                      ASSERTION_INFO_ASPECT_NAME,
                      new EnvelopedAspect()
                          .setName(ASSERTION_INFO_ASPECT_NAME)
                          .setValue(new Aspect(assertionInfo.data())),
                      ASSERTION_ACTIONS_ASPECT_NAME,
                      new EnvelopedAspect()
                          .setName(ASSERTION_ACTIONS_ASPECT_NAME)
                          .setValue(new Aspect(assertionActions.data()))
                  )))
          );
    }

    SearchEntityArray searchEntities = incidentUrn == null
        ? new SearchEntityArray(Collections.emptyList())
        : new SearchEntityArray(ImmutableList.of(
            new SearchEntity().setEntity(incidentUrn)
        ));

    Mockito.when(mockClient.search(
        Mockito.eq(INCIDENT_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.eq(null),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    ))
        .thenReturn(
            new SearchResult()
                .setNumEntities(1)
                .setPageSize(1)
                .setFrom(0)
                .setEntities(searchEntities)
        );

    return mockClient;
  }

  private EntityClient mockEntityClient(
      Urn anomalyUrn,
      AnomalyInfo anomalyInfo,
      Urn assertionUrn,
      AssertionInfo assertionInfo) throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    if (anomalyUrn != null) {
      Mockito.when(mockClient.getV2(
          Mockito.eq(ANOMALY_ENTITY_NAME),
          Mockito.eq(anomalyUrn),
          Mockito.eq(ImmutableSet.of(ANOMALY_INFO_ASPECT_NAME)),
          Mockito.any(Authentication.class)
      ))
          .thenReturn(
              new EntityResponse()
                  .setUrn(anomalyUrn)
                  .setEntityName(ANOMALY_ENTITY_NAME)
                  .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                      ANOMALY_INFO_ASPECT_NAME,
                      new EnvelopedAspect()
                          .setName(ANOMALY_INFO_ASPECT_NAME)
                          .setValue(new Aspect(anomalyInfo.data()))
                  )))
          );
    }

    if (assertionUrn != null) {
      Mockito.when(mockClient.getV2(
          Mockito.eq(ASSERTION_ENTITY_NAME),
          Mockito.eq(assertionUrn),
          Mockito.eq(ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_ACTIONS_ASPECT_NAME)),
          Mockito.any(Authentication.class)
      ))
          .thenReturn(
              new EntityResponse()
                  .setUrn(assertionUrn)
                  .setEntityName(ASSERTION_ENTITY_NAME)
                  .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                      ASSERTION_INFO_ASPECT_NAME,
                      new EnvelopedAspect()
                          .setName(ASSERTION_INFO_ASPECT_NAME)
                          .setValue(new Aspect(assertionInfo.data()))
                  )))
          );
    }

    SearchEntityArray searchEntities = anomalyUrn == null
        ? new SearchEntityArray(Collections.emptyList())
        : new SearchEntityArray(ImmutableList.of(
            new SearchEntity().setEntity(anomalyUrn)
        ));

    Mockito.when(mockClient.search(
        Mockito.eq(ANOMALY_ENTITY_NAME),
        Mockito.eq("*"),
        Mockito.any(Filter.class),
        Mockito.eq(null),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class),
        Mockito.any(SearchFlags.class)
    ))
        .thenReturn(
            new SearchResult()
                .setNumEntities(1)
                .setPageSize(1)
                .setFrom(0)
                .setEntities(searchEntities)
        );

    return mockClient;
  }

  @Nonnull
  private Status buildStatus(final boolean removed) {
    final Status result = new Status();
    result.setRemoved(removed);
    return result;
  }

  private AssertionRunEvent buildAssertionRunEvent(final Urn urn, final AssertionRunStatus status, final AssertionResultType resultType) {
    AssertionRunEvent event = new AssertionRunEvent();
    event.setTimestampMillis(1L);
    event.setAssertionUrn(urn);
    event.setStatus(status);
    event.setResult(new AssertionResult()
        .setType(resultType)
        .setRowCount(0L)
    );
    return event;
  }

  private MetadataChangeLog buildMetadataChangeLog(Urn urn, String aspectName, ChangeType changeType, RecordTemplate aspect) throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(urn);
    event.setEntityType(urn.getEntityType());
    event.setAspectName(aspectName);
    event.setChangeType(changeType);
    if (aspect != null) {
      event.setAspect(GenericRecordUtils.serializeAspect(aspect));
    }
    return event;
  }
}
