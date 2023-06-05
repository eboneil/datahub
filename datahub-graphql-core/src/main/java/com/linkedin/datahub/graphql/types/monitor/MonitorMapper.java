package com.linkedin.datahub.graphql.types.monitor;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationParameters;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationParametersType;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationSpec;
import com.linkedin.datahub.graphql.generated.AssertionMonitor;
import com.linkedin.datahub.graphql.generated.AuditLogSpec;
import com.linkedin.datahub.graphql.generated.CronSchedule;
import com.linkedin.datahub.graphql.generated.DatasetSlaAssertionParameters;
import com.linkedin.datahub.graphql.generated.DatasetSlaSourceType;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.generated.MonitorInfo;
import com.linkedin.datahub.graphql.generated.MonitorType;
import com.linkedin.datahub.graphql.generated.MonitorStatus;
import com.linkedin.datahub.graphql.generated.MonitorMode;
import com.linkedin.datahub.graphql.generated.SchemaFieldSpec;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.MonitorKey;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class MonitorMapper {

  public static Monitor map(@Nonnull final EntityResponse entityResponse) {
    final Monitor result = new Monitor();

    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(com.linkedin.datahub.graphql.generated.EntityType.MONITOR);

    final EnvelopedAspect envelopedMonitorKey = aspects.get(Constants.MONITOR_KEY_ASPECT_NAME);
    if (envelopedMonitorKey != null) {
      MonitorKey key = new MonitorKey(envelopedMonitorKey.getValue().data());
      result.setEntity(UrnToEntityMapper.map(key.getEntity()));
    }

    final EnvelopedAspect envelopedMonitorInfo = aspects.get(Constants.MONITOR_INFO_ASPECT_NAME);
    if (envelopedMonitorInfo != null) {
      result.setInfo(mapMonitorInfo(new com.linkedin.monitor.MonitorInfo(envelopedMonitorInfo.getValue().data())));
    }

    return result;
  }

  private static MonitorInfo mapMonitorInfo(com.linkedin.monitor.MonitorInfo backendMonitorInfo) {
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.valueOf(backendMonitorInfo.getType().name()));
    if (backendMonitorInfo.hasAssertionMonitor()) {
      monitorInfo.setAssertion(mapAssertionMonitor(backendMonitorInfo.getAssertionMonitor()));
    }
    monitorInfo.setStatus(mapMonitorStatus(backendMonitorInfo.getStatus()));
    return monitorInfo;
  }

  private static AssertionMonitor mapAssertionMonitor(com.linkedin.monitor.AssertionMonitor backendAssertionMonitor) {
    AssertionMonitor assertionMonitor = new AssertionMonitor();
    List<AssertionEvaluationSpec> assertionEvaluationSpecs = backendAssertionMonitor.getAssertions().stream()
        .map(MonitorMapper::mapAssertionEvaluationSpec)
        .collect(Collectors.toList());
    assertionMonitor.setAssertions(assertionEvaluationSpecs);
    return assertionMonitor;
  }

  private static AssertionEvaluationSpec mapAssertionEvaluationSpec(com.linkedin.monitor.AssertionEvaluationSpec backendAssertionEvaluationSpec) {
    final AssertionEvaluationSpec assertionEvaluationSpec = new AssertionEvaluationSpec();
    final Assertion partialAssertion = new Assertion();
    partialAssertion.setUrn(backendAssertionEvaluationSpec.getAssertion().toString());
    partialAssertion.setType(EntityType.ASSERTION);
    assertionEvaluationSpec.setAssertion(partialAssertion);
    assertionEvaluationSpec.setSchedule(mapCronSchedule(backendAssertionEvaluationSpec.getSchedule()));
    if (backendAssertionEvaluationSpec.hasParameters()) {
      assertionEvaluationSpec.setParameters(mapAssertionEvaluationParameters(backendAssertionEvaluationSpec.getParameters()));
    }
    return assertionEvaluationSpec;
  }

  private static CronSchedule mapCronSchedule(com.linkedin.common.CronSchedule backendSchedule) {
    final CronSchedule result = new CronSchedule();
    result.setCron(backendSchedule.getCron());
    result.setTimezone(backendSchedule.getTimezone());
    return result;
  }

  private static AssertionEvaluationParameters mapAssertionEvaluationParameters(
      com.linkedin.monitor.AssertionEvaluationParameters backendAssertionEvaluationParameters) {
    final AssertionEvaluationParameters assertionEvaluationParameters = new AssertionEvaluationParameters();
    assertionEvaluationParameters.setType(
        AssertionEvaluationParametersType.valueOf(backendAssertionEvaluationParameters.getType().name()));
    if (backendAssertionEvaluationParameters.getDatasetSlaParameters() != null) {
      assertionEvaluationParameters.setDatasetSlaParameters(mapDatasetSlaAssertionParameters(
          backendAssertionEvaluationParameters.getDatasetSlaParameters()));
    }
    return assertionEvaluationParameters;
  }

  private static DatasetSlaAssertionParameters mapDatasetSlaAssertionParameters(
      com.linkedin.monitor.DatasetSlaAssertionParameters backendDatasetSlaAssertionParameters) {
    final DatasetSlaAssertionParameters datasetSlaAssertionParameters = new DatasetSlaAssertionParameters();
    datasetSlaAssertionParameters.setSourceType(
        DatasetSlaSourceType.valueOf(backendDatasetSlaAssertionParameters.getSourceType().name()));
    if (backendDatasetSlaAssertionParameters.hasField()) {
      datasetSlaAssertionParameters.setField(mapSchemaFieldSpec(backendDatasetSlaAssertionParameters.getField()));
    }
    if (backendDatasetSlaAssertionParameters.hasAuditLog()) {
      datasetSlaAssertionParameters.setAuditLog(mapAuditLogSpec(backendDatasetSlaAssertionParameters.getAuditLog()));
    }
    return datasetSlaAssertionParameters;
  }

  private static SchemaFieldSpec mapSchemaFieldSpec(com.linkedin.schema.SchemaFieldSpec backendSchemaFieldSpec) {
    SchemaFieldSpec schemaFieldSpec = new SchemaFieldSpec();
    schemaFieldSpec.setPath(backendSchemaFieldSpec.getPath());
    schemaFieldSpec.setType(backendSchemaFieldSpec.getType());
    schemaFieldSpec.setNativeType(backendSchemaFieldSpec.getNativeType());
    return schemaFieldSpec;
  }

  private static AuditLogSpec mapAuditLogSpec(com.linkedin.monitor.AuditLogSpec backendAuditLogSpec) {
    AuditLogSpec auditLogSpec = new AuditLogSpec();
    if (backendAuditLogSpec.hasOperationTypes()) {
      auditLogSpec.setOperationTypes(new ArrayList<>(backendAuditLogSpec.getOperationTypes()));
    }
    if (backendAuditLogSpec.hasUserName()) {
      auditLogSpec.setUserName(backendAuditLogSpec.getUserName());
    }
    return auditLogSpec;
  }

  private static MonitorStatus mapMonitorStatus(com.linkedin.monitor.MonitorStatus backendStatus) {
    MonitorStatus monitorStatus = new MonitorStatus();
    monitorStatus.setMode(MonitorMode.valueOf(backendStatus.getMode().toString()));
    return monitorStatus;
  }

  private MonitorMapper() { }
}