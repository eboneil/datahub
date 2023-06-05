package com.linkedin.datahub.graphql.types.monitor;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.AuditLogSpec;
import com.linkedin.monitor.DatasetSlaAssertionParameters;
import com.linkedin.monitor.DatasetSlaSourceType;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.schema.SchemaFieldSpec;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MonitorMapperTest {

  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_ENTITY_URN = UrnUtils.getUrn("urn:li:dataset:test");
  private static final String TEST_MONITOR_ID = "test";

  @Test
  public void testMapAssertionMonitor() {
    MonitorKey key = new MonitorKey();
    key.setEntity(TEST_ENTITY_URN);
    key.setId(TEST_MONITOR_ID);

    // Case 1: Without nullable fields
    MonitorInfo info = createAssertionMonitorInfoWithoutNullableFields();
    EntityResponse monitorEntityResponse = createMonitorEntityResponse(key, info);
    Monitor output = MonitorMapper.map(monitorEntityResponse);
    verifyMonitor(key, info, output);

    // Case 2: With nullable fields
    info = createAssertionMonitorInfoWithNullableFields();
    EntityResponse monitorEntityResponseWithNullables = createMonitorEntityResponse(key, info);
    output = MonitorMapper.map(monitorEntityResponseWithNullables);
    verifyMonitor(key, info, output);
  }

  private void verifyMonitor(MonitorKey key, MonitorInfo info, Monitor output) {
    Assert.assertNotNull(output);
    Assert.assertNotNull(output.getInfo());
    Assert.assertEquals(output.getEntity().getUrn(), key.getEntity().toString());
    Assert.assertEquals(info.getType().toString(), output.getInfo().getType().toString());
    Assert.assertEquals(info.getStatus().getMode().toString(), output.getInfo().getStatus().getMode().toString());
    if (info.hasAssertionMonitor()) {
      verifyAssertionMonitor(info.getAssertionMonitor(), output.getInfo().getAssertion());
    }
  }

  private void verifyAssertionMonitor(AssertionMonitor input, com.linkedin.datahub.graphql.generated.AssertionMonitor output) {
    Assert.assertEquals(output.getAssertions().get(0).getAssertion().getUrn(), input.getAssertions().get(0).getAssertion().toString());
    Assert.assertEquals(output.getAssertions().get(0).getSchedule().getCron(), input.getAssertions().get(0).getSchedule().getCron());
    Assert.assertEquals(output.getAssertions().get(0).getSchedule().getTimezone(), input.getAssertions().get(0).getSchedule().getTimezone());
    Assert.assertEquals(output.getAssertions().get(0).getParameters().getType().toString(),
        input.getAssertions().get(0).getParameters().getType().toString());

    if (input.getAssertions().get(0).getParameters().hasDatasetSlaParameters()) {
      // Verify the dataset SLA parameters.
      DatasetSlaAssertionParameters inputParams = input.getAssertions().get(0).getParameters().getDatasetSlaParameters();
      com.linkedin.datahub.graphql.generated.DatasetSlaAssertionParameters outputParams =
          output.getAssertions().get(0).getParameters().getDatasetSlaParameters();
      Assert.assertEquals(outputParams.getSourceType().toString(), inputParams.getSourceType().toString());

      if (inputParams.hasField()) {
        SchemaFieldSpec inputFieldSpec = inputParams.getField();
        com.linkedin.datahub.graphql.generated.SchemaFieldSpec outputFieldSpec = outputParams.getField();
        Assert.assertEquals(outputFieldSpec.getNativeType(), inputFieldSpec.getNativeType());
        Assert.assertEquals(outputFieldSpec.getType(), inputFieldSpec.getType());
        Assert.assertEquals(outputFieldSpec.getPath(), inputFieldSpec.getPath());
      }

      if (inputParams.hasAuditLog()) {
        AuditLogSpec inputAuditLogSpec = inputParams.getAuditLog();
        com.linkedin.datahub.graphql.generated.AuditLogSpec outputAuditLogSpec = outputParams.getAuditLog();
        if (inputAuditLogSpec.hasOperationTypes()) {
          Assert.assertEquals(outputAuditLogSpec.getOperationTypes(), inputAuditLogSpec.getOperationTypes());
        }
        if (inputAuditLogSpec.hasUserName()) {
          Assert.assertEquals(outputAuditLogSpec.getUserName(), inputAuditLogSpec.getUserName());
        }
      }
    }

  }

  private EntityResponse createMonitorEntityResponse(final MonitorKey key, final MonitorInfo info) {
    EnvelopedAspect envelopedMonitorKey = createEnvelopedAspect(key.data());
    EnvelopedAspect envelopedMonitorInfo = createEnvelopedAspect(info.data());
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:monitor:1"));
    entityResponse.setAspects(new EnvelopedAspectMap(new HashMap<>()));
    entityResponse.getAspects().put(Constants.MONITOR_KEY_ASPECT_NAME, envelopedMonitorKey);
    entityResponse.getAspects().put(Constants.MONITOR_INFO_ASPECT_NAME, envelopedMonitorInfo);
    return entityResponse;
  }

  private EnvelopedAspect createEnvelopedAspect(DataMap dataMap) {
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(dataMap));
    return envelopedAspect;
  }

  private MonitorInfo createAssertionMonitorInfoWithoutNullableFields() {
    MonitorInfo info = new MonitorInfo();
    info.setType(MonitorType.ASSERTION);
    info.setAssertionMonitor(new AssertionMonitor()
      .setAssertions(new AssertionEvaluationSpecArray(
        ImmutableList.of(
          new AssertionEvaluationSpec()
            .setAssertion(TEST_ASSERTION_URN)
            .setSchedule(new CronSchedule().setCron("1 * * * *").setTimezone("America/Los_Angeles"))
            .setParameters(new AssertionEvaluationParameters()
                .setType(com.linkedin.monitor.AssertionEvaluationParametersType.DATASET_SLA)
            )
        )
      )));
    info.setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));
    return info;
  }

  private MonitorInfo createAssertionMonitorInfoWithNullableFields() {
    MonitorInfo info = new MonitorInfo();
    info.setType(MonitorType.ASSERTION);
    info.setAssertionMonitor(new AssertionMonitor()
      .setAssertions(new AssertionEvaluationSpecArray(
          ImmutableList.of(
              new AssertionEvaluationSpec()
                .setAssertion(TEST_ASSERTION_URN)
                .setSchedule(new CronSchedule().setCron("1 * * * *").setTimezone("America/Los_Angeles"))
                .setParameters(new AssertionEvaluationParameters()
                    .setType(com.linkedin.monitor.AssertionEvaluationParametersType.DATASET_SLA)
                    .setDatasetSlaParameters(new DatasetSlaAssertionParameters()
                        .setSourceType(DatasetSlaSourceType.FIELD_VALUE)
                        .setField(new SchemaFieldSpec().setNativeType("varchar").setType("STRING").setPath("name"))
                    )
                )
          )
      )));
    info.setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));
    return info;
  }
}
