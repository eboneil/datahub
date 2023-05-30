package com.linkedin.metadata.kafka.hook.assertion;

import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.common.AssertionSummaryDetails;
import com.linkedin.common.AssertionSummaryDetailsArray;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;


public class AssertionsSummaryHookTest {

  private static EntityRegistry entityRegistry;

  static {
    AssertionsSummaryHookTest.class.getClassLoader().setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    entityRegistry = new ConfigEntityRegistry(
        AssertionsSummaryHookTest.class.getClassLoader().getResourceAsStream("test-entity-registry.yml"));
  }

  private static final Urn TEST_EXISTING_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:existing-test");
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_DATASET_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  private static final String TEST_ASSERTION_TYPE = AssertionType.DATASET.toString();

  @Test
  public void testInvokeNotEnabled() throws Exception {
    AssertionService service = mockAssertionService(new AssertionsSummary());
    AssertionsSummaryHook hook = new AssertionsSummaryHook(entityRegistry, service, false);
    final MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        mockAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(0)).getAssertionInfo(Mockito.any());
  }

  @Test
  public void testInvokeNotEligibleChange() throws Exception {
    AssertionService service = mockAssertionService(new AssertionsSummary());
    AssertionsSummaryHook hook = new AssertionsSummaryHook(entityRegistry, service, true);

    // Case 1: Incorrect aspect
    MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_INFO_ASPECT_NAME,
        ChangeType.UPSERT,
        new AssertionInfo());
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(0)).getAssertionInfo(Mockito.any());

    // Case 2: Run Event But Delete
    event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.DELETE,
        mockAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(0)).getAssertionInfo(Mockito.any());
  }

  @DataProvider(name = "assertionsSummaryProvider")
  static Object[][] assertionsSummaryProvider() {
    return new Object[][] {
        new Object[] {
            null
        },
        new Object[] {
            new AssertionsSummary()
                .setPassingAssertionDetails(new AssertionSummaryDetailsArray())
                .setFailingAssertionDetails(new AssertionSummaryDetailsArray())
        },
        new Object[] {
            new AssertionsSummary()
                .setPassingAssertionDetails(new AssertionSummaryDetailsArray(ImmutableList.of(
                    new AssertionSummaryDetails()
                        .setUrn(TEST_EXISTING_ASSERTION_URN)
                        .setType(TEST_ASSERTION_TYPE)
                        .setLastResultAt(0L)
                        .setSource(AssertionSourceType.EXTERNAL.toString())
                )))
                .setFailingAssertionDetails(new AssertionSummaryDetailsArray())
        },
        new Object[] {
            new AssertionsSummary()
                .setPassingAssertionDetails(new AssertionSummaryDetailsArray())
                .setFailingAssertionDetails(new AssertionSummaryDetailsArray(ImmutableList.of(
                  new AssertionSummaryDetails()
                      .setUrn(TEST_EXISTING_ASSERTION_URN)
                      .setType(TEST_ASSERTION_TYPE)
                      .setLastResultAt(0L)
                      .setSource(AssertionSourceType.EXTERNAL.toString())
            )))
        },
        new Object[] {
            new AssertionsSummary()
                .setPassingAssertionDetails(new AssertionSummaryDetailsArray(ImmutableList.of(
                    new AssertionSummaryDetails()
                        .setUrn(TEST_EXISTING_ASSERTION_URN)
                        .setType(TEST_ASSERTION_TYPE)
                        .setLastResultAt(0L)
                        .setSource(AssertionSourceType.EXTERNAL.toString())
                )))
                .setFailingAssertionDetails(new AssertionSummaryDetailsArray(ImmutableList.of(
                  new AssertionSummaryDetails()
                      .setUrn(TEST_EXISTING_ASSERTION_URN)
                      .setType(TEST_ASSERTION_TYPE)
                      .setLastResultAt(0L)
                      .setSource(AssertionSourceType.EXTERNAL.toString())
            )))
        }
    };
  }

  @Test(dataProvider = "assertionsSummaryProvider")
  public void testInvokeAssertionRunEventSuccess(AssertionsSummary summary) throws Exception {
    AssertionService service = mockAssertionService(summary);
    AssertionsSummaryHook hook = new AssertionsSummaryHook(entityRegistry, service, true);
    final AssertionInfo info = new AssertionInfo().setType(AssertionType.DATASET).setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL));
    final AssertionRunEvent runEvent = mockAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS);
    final MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        runEvent);
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(1)).getAssertionInfo(Mockito.eq(TEST_ASSERTION_URN));
    Mockito.verify(service, Mockito.times(1)).getAssertionsSummary(Mockito.eq(TEST_DATASET_URN));

    if (summary == null) {
      summary = new AssertionsSummary();
    }
    AssertionsSummary expectedSummary = new AssertionsSummary(summary.data());
    expectedSummary.setPassingAssertionDetails(
        new AssertionSummaryDetailsArray(expectedSummary.getPassingAssertionDetails()
            .stream()
            .filter(details -> !details.getUrn().equals(TEST_ASSERTION_URN))
            .collect(Collectors.toList())));
    expectedSummary.setFailingAssertionDetails(
        new AssertionSummaryDetailsArray(expectedSummary.getFailingAssertionDetails()
            .stream()
            .filter(details -> !details.getUrn().equals(TEST_ASSERTION_URN))
            .collect(Collectors.toList())));
    expectedSummary.getPassingAssertionDetails().add(buildAssertionSummaryDetails(TEST_ASSERTION_URN, info, runEvent));

    // Ensure we ingested a new aspect.
    Mockito.verify(service, Mockito.times(1)).updateAssertionsSummary(
        Mockito.eq(TEST_DATASET_URN),
        Mockito.eq(expectedSummary));
  }


  @Test(dataProvider = "assertionsSummaryProvider")
  public void testInvokeAssertionRunEventFailure(AssertionsSummary summary) throws Exception {
    AssertionService service = mockAssertionService(summary);
    AssertionsSummaryHook hook = new AssertionsSummaryHook(entityRegistry, service, true);
    final AssertionInfo info = new AssertionInfo().setType(AssertionType.DATASET).setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL));
    final AssertionRunEvent runEvent = mockAssertionRunEvent(TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE);
    final MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        ASSERTION_RUN_EVENT_ASPECT_NAME,
        ChangeType.UPSERT,
        runEvent);
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(1)).getAssertionInfo(Mockito.eq(TEST_ASSERTION_URN));
    Mockito.verify(service, Mockito.times(1)).getAssertionsSummary(Mockito.eq(TEST_DATASET_URN));

    if (summary == null) {
      summary = new AssertionsSummary();
    }
    AssertionsSummary expectedSummary = new AssertionsSummary(summary.data());
    expectedSummary.setPassingAssertionDetails(
        new AssertionSummaryDetailsArray(expectedSummary.getPassingAssertionDetails()
            .stream()
            .filter(details -> !details.getUrn().equals(TEST_ASSERTION_URN))
            .collect(Collectors.toList())));
    expectedSummary.setFailingAssertionDetails(
        new AssertionSummaryDetailsArray(expectedSummary.getFailingAssertionDetails()
            .stream()
            .filter(details -> !details.getUrn().equals(TEST_ASSERTION_URN))
            .collect(Collectors.toList())));
    expectedSummary.getFailingAssertionDetails().add(buildAssertionSummaryDetails(TEST_ASSERTION_URN, info, runEvent));

    // Ensure we ingested a new aspect.
    Mockito.verify(service, Mockito.times(1)).updateAssertionsSummary(
        Mockito.eq(TEST_DATASET_URN),
        Mockito.eq(expectedSummary));
  }


  @Test(dataProvider = "assertionsSummaryProvider")
  public void testInvokeAssertionSoftDeleted(AssertionsSummary summary) throws Exception {
    AssertionService service = mockAssertionService(summary);
    AssertionsSummaryHook hook = new AssertionsSummaryHook(entityRegistry, service, true);
    final MetadataChangeLog event = buildMetadataChangeLog(
        TEST_ASSERTION_URN,
        STATUS_ASPECT_NAME,
        ChangeType.UPSERT,
        mockAssertionSoftDeleted());
    hook.invoke(event);

    Mockito.verify(service, Mockito.times(1)).getAssertionInfo(Mockito.eq(TEST_ASSERTION_URN));
    Mockito.verify(service, Mockito.times(1)).getAssertionsSummary(Mockito.eq(TEST_DATASET_URN));

    if (summary == null) {
      summary = new AssertionsSummary();
    }
    AssertionsSummary expectedSummary = new AssertionsSummary(summary.data());
    expectedSummary.setPassingAssertionDetails(
        new AssertionSummaryDetailsArray(expectedSummary.getPassingAssertionDetails()
            .stream()
            .filter(details -> !details.getUrn().equals(TEST_ASSERTION_URN))
            .collect(Collectors.toList())));
    expectedSummary.setFailingAssertionDetails(
        new AssertionSummaryDetailsArray(expectedSummary.getFailingAssertionDetails()
            .stream()
            .filter(details -> !details.getUrn().equals(TEST_ASSERTION_URN))
            .collect(Collectors.toList())));
    // Ensure we ingested a new aspect.
    Mockito.verify(service, Mockito.times(1)).updateAssertionsSummary(
        Mockito.eq(TEST_DATASET_URN),
        Mockito.eq(expectedSummary));
  }

  private AssertionRunEvent mockAssertionRunEvent(final Urn urn, final AssertionRunStatus status, final AssertionResultType resultType) {
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

  private Status mockAssertionSoftDeleted() {
    Status status = new Status();
    status.setRemoved(true);
    return status;
  }

  private AssertionService mockAssertionService(AssertionsSummary summary) {
    AssertionService mockService = Mockito.mock(AssertionService.class);

    Mockito.when(mockService.getAssertionInfo(TEST_ASSERTION_URN))
        .thenReturn(new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL))
            .setDatasetAssertion(new DatasetAssertionInfo()
                .setDataset(TEST_DATASET_URN)
        )
    );

    Mockito.when(mockService.getAssertionsSummary(TEST_DATASET_URN)).thenReturn(summary);

    return mockService;
  }

  @Nonnull
  private AssertionSummaryDetails buildAssertionSummaryDetails(
      @Nonnull final Urn urn,
      @Nonnull final AssertionInfo info,
      @Nonnull final AssertionRunEvent event) {
    AssertionSummaryDetails assertionSummaryDetails = new AssertionSummaryDetails();
    assertionSummaryDetails.setUrn(urn);
    assertionSummaryDetails.setType(info.getType().toString());
    assertionSummaryDetails.setLastResultAt(event.getTimestampMillis());
    if (info.hasSource()) {
      assertionSummaryDetails.setSource(info.getSource().getType().toString());
    }
    return assertionSummaryDetails;
  }

  private MetadataChangeLog buildMetadataChangeLog(Urn urn, String aspectName, ChangeType changeType, RecordTemplate aspect) throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(urn);
    event.setEntityType(ASSERTION_ENTITY_NAME);
    event.setAspectName(aspectName);
    event.setChangeType(changeType);
    event.setAspect(GenericRecordUtils.serializeAspect(aspect));
    return event;
  }
}