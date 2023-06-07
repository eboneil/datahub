package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.SlaCronSchedule;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.SlaAssertionInfo;
import com.linkedin.assertion.SlaAssertionSchedule;
import com.linkedin.assertion.SlaAssertionScheduleType;
import com.linkedin.assertion.SlaAssertionType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.FixedIntervalSchedule;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AssertionMapperTest {

  @Test
  public void testMapDatasetAssertion() {
    // Case 1: Without nullable fields
    AssertionInfo input = createSlaAssertionInfoWithoutNullableFields();
    EntityResponse datasetAssertionEntityResponse = createAssertionInfoEntityResponse(input);
    Assertion output = AssertionMapper.map(datasetAssertionEntityResponse);
    verifyAssertion(input, output);

    // Case 2: With nullable fields
    input = createSlaAssertionInfoWithNullableFields();
    EntityResponse datasetAssertionEntityResponseWithNullables = createAssertionInfoEntityResponse(input);
    output = AssertionMapper.map(datasetAssertionEntityResponseWithNullables);
    verifyAssertion(input, output);
  }

  @Test
  public void testMapSlaAssertion() {
    // Case 1: Without nullable fields
    AssertionInfo input = createSlaAssertionInfoWithoutNullableFields();
    EntityResponse slaAssertionEntityResponse = createAssertionInfoEntityResponse(input);
    Assertion output = AssertionMapper.map(slaAssertionEntityResponse);
    verifyAssertion(input, output);

    // Case 2: With nullable fields
    input = createDatasetAssertionInfoWithNullableFields();
    EntityResponse slaAssertionEntityResponseWithNullables = createAssertionInfoEntityResponse(input);
    output = AssertionMapper.map(slaAssertionEntityResponseWithNullables);
    verifyAssertion(input, output);
  }

  private void verifyAssertion(AssertionInfo input, Assertion output) {
    Assert.assertNotNull(output);
    Assert.assertNotNull(output.getInfo());
    Assert.assertEquals(output.getInfo().getType().toString(), output.getInfo().getType().toString());

    if (input.hasDatasetAssertion()) {
      verifyDatasetAssertion(input.getDatasetAssertion(), output.getInfo().getDatasetAssertion());
    }

    if (input.hasSlaAssertion()) {
      verifySlaAssertion(input.getSlaAssertion(), output.getInfo().getSlaAssertion());
    }

    if (input.hasSource()) {
      verifySource(input.getSource(), output.getInfo().getSource());
    }
  }

  private void verifyDatasetAssertion(DatasetAssertionInfo input, com.linkedin.datahub.graphql.generated.DatasetAssertionInfo output) {
    Assert.assertEquals(output.getOperator().toString(), input.getOperator().toString());
    Assert.assertEquals(output.getOperator().toString(), input.getOperator().toString());
    Assert.assertEquals(output.getScope().toString(), input.getScope().toString());
    Assert.assertEquals(output.getDatasetUrn(), input.getDataset().toString());
    if (input.hasAggregation()) {
      Assert.assertEquals(output.getAggregation().toString(), input.getAggregation().toString());
    }
    if (input.hasNativeType()) {
      Assert.assertEquals(output.getNativeType(), input.getNativeType().toString());
    }
    if (input.hasLogic()) {
      Assert.assertEquals(output.getLogic(), input.getLogic());
    }
    if (input.hasFields()) {
      Assert.assertTrue(input.getFields().stream()
          .allMatch(field -> output.getFields().stream().anyMatch(outField -> field.toString().equals(outField.getUrn()))));
    }
  }

  private void verifySlaAssertion(SlaAssertionInfo input, com.linkedin.datahub.graphql.generated.SlaAssertionInfo output) {
    Assert.assertEquals(output.getType().toString(), input.getType().toString());
    Assert.assertEquals(output.getEntityUrn(), input.getEntity().toString());
    if (input.hasSchedule()) {
      verifySlaSchedule(input.getSchedule(), output.getSchedule());
    }
  }

  private void verifyCronSchedule(SlaCronSchedule input, com.linkedin.datahub.graphql.generated.SlaCronSchedule output) {
    Assert.assertEquals(output.getCron(), input.getCron());
    Assert.assertEquals(output.getTimezone(), input.getTimezone());
    if (input.hasWindowStartOffsetMs()) {
      Assert.assertEquals(output.getWindowStartOffsetMs(), input.getWindowStartOffsetMs());
    }
  }

  private void verifySlaSchedule(SlaAssertionSchedule input, com.linkedin.datahub.graphql.generated.SlaAssertionSchedule output) {
    Assert.assertEquals(output.getType().toString(), input.getType().toString());
    if (input.hasCron()) {
      verifyCronSchedule(input.getCron(), output.getCron());
    }
    if (input.hasFixedInterval()) {
      verifyFixedIntervalSchedule(input.getFixedInterval(), output.getFixedInterval());
    }
  }

  private void verifyFixedIntervalSchedule(com.linkedin.assertion.FixedIntervalSchedule input, FixedIntervalSchedule output) {
    Assert.assertEquals(output.getMultiple(), (int) input.getMultiple());
    Assert.assertEquals(output.getUnit().toString(), input.getUnit().toString());
  }

  private void verifySource(AssertionSource input, com.linkedin.datahub.graphql.generated.AssertionSource output) {
    Assert.assertEquals(output.getType().toString(), input.getType().toString());
  }

  private EntityResponse createAssertionInfoEntityResponse(final AssertionInfo info) {
    EnvelopedAspect envelopedAssertionInfo = createEnvelopedAspect(info.data());
    return createEntityResponse(Constants.ASSERTION_INFO_ASPECT_NAME, envelopedAssertionInfo);
  }

  private EntityResponse createEntityResponse(String aspectName, EnvelopedAspect envelopedAspect) {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:assertion:1"));
    entityResponse.setAspects(new EnvelopedAspectMap(new HashMap<>()));
    entityResponse.getAspects().put(aspectName, envelopedAspect);
    return entityResponse;
  }

  private EnvelopedAspect createEnvelopedAspect(DataMap dataMap) {
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(dataMap));
    return envelopedAspect;
  }

  private AssertionInfo createDatasetAssertionInfoWithoutNullableFields() {
    AssertionInfo info = new AssertionInfo();
    info.setType(com.linkedin.assertion.AssertionType.DATASET);
    DatasetAssertionInfo datasetAssertionInfo = new DatasetAssertionInfo();
    datasetAssertionInfo.setDataset(UrnUtils.getUrn("urn:li:dataset:1"));
    datasetAssertionInfo.setScope(DatasetAssertionScope.DATASET_COLUMN);
    datasetAssertionInfo.setOperator(AssertionStdOperator.GREATER_THAN);
    info.setDatasetAssertion(datasetAssertionInfo);
    return info;
  }

  private AssertionInfo createDatasetAssertionInfoWithNullableFields() {
    AssertionInfo infoWithoutNullables = createDatasetAssertionInfoWithoutNullableFields();
    DatasetAssertionInfo baseInfo = infoWithoutNullables.getDatasetAssertion();
    baseInfo.setFields(new UrnArray(Arrays.asList(UrnUtils.getUrn("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD),field)"))));
    baseInfo.setAggregation(AssertionStdAggregation.SUM);
    baseInfo.setParameters(createAssertionStdParameters());
    baseInfo.setNativeType("native_type");
    baseInfo.setNativeParameters(new StringMap(Collections.singletonMap("key", "value")));
    baseInfo.setLogic("sample_logic");
    infoWithoutNullables.setSource(new AssertionSource()
        .setType(com.linkedin.assertion.AssertionSourceType.INFERRED)
    );
    return infoWithoutNullables;
  }

  private AssertionInfo createSlaAssertionInfoWithoutNullableFields() {
    AssertionInfo info = new AssertionInfo();
    info.setType(AssertionType.SLA);
    SlaAssertionInfo slaAssertionInfo = new SlaAssertionInfo();
    slaAssertionInfo.setEntity(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)"));
    slaAssertionInfo.setType(SlaAssertionType.DATASET_CHANGE);
    info.setSlaAssertion(slaAssertionInfo);
    return info;
  }

  private AssertionInfo createSlaAssertionInfoWithNullableFields() {
    AssertionInfo infoWithoutNullables = createSlaAssertionInfoWithoutNullableFields();
    SlaAssertionInfo baseInfo = infoWithoutNullables.getSlaAssertion();
    baseInfo.setSchedule(createSlaAssertionSchedule());
    infoWithoutNullables.setSource(new AssertionSource()
      .setType(com.linkedin.assertion.AssertionSourceType.INFERRED)
    );
    return infoWithoutNullables;
  }

  private AssertionStdParameters createAssertionStdParameters() {
    AssertionStdParameters parameters = new AssertionStdParameters();
    parameters.setValue(createAssertionStdParameter());
    parameters.setMinValue(createAssertionStdParameter());
    parameters.setMaxValue(createAssertionStdParameter());
    return parameters;
  }

  private AssertionStdParameter createAssertionStdParameter() {
    AssertionStdParameter parameter = new AssertionStdParameter();
    parameter.setType(AssertionStdParameterType.NUMBER);
    parameter.setValue("100");
    return parameter;
  }

  private SlaAssertionSchedule createSlaAssertionSchedule() {
    SlaAssertionSchedule schedule = new SlaAssertionSchedule();
    schedule.setType(SlaAssertionScheduleType.CRON);
    schedule.setCron(createCronSchedule());
    return schedule;
  }

  private SlaCronSchedule createCronSchedule() {
    SlaCronSchedule cronSchedule = new SlaCronSchedule();
    cronSchedule.setCron("0 0 * * *");
    cronSchedule.setTimezone("UTC");
    return cronSchedule;
  }
}
