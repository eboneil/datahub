package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.assertion.AssertionAction;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionActionType;
import com.linkedin.datahub.graphql.generated.AssertionSource;
import com.linkedin.datahub.graphql.generated.AssertionStdAggregation;
import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionStdParameter;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterType;
import com.linkedin.datahub.graphql.generated.AssertionStdParameters;
import com.linkedin.datahub.graphql.generated.AssertionType;
import com.linkedin.datahub.graphql.generated.AssertionSourceType;
import com.linkedin.datahub.graphql.generated.SlaAssertionInfo;
import com.linkedin.datahub.graphql.generated.SlaAssertionSchedule;
import com.linkedin.datahub.graphql.generated.SlaAssertionScheduleType;
import com.linkedin.datahub.graphql.generated.FixedIntervalSchedule;
import com.linkedin.datahub.graphql.generated.DateInterval;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.DatasetAssertionInfo;
import com.linkedin.datahub.graphql.generated.DatasetAssertionScope;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SchemaFieldRef;
import com.linkedin.datahub.graphql.generated.SlaAssertionType;
import com.linkedin.datahub.graphql.generated.SlaCronSchedule;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import java.util.Collections;
import java.util.stream.Collectors;


public class AssertionMapper {

  public static Assertion map(final EntityResponse entityResponse) {
    final Assertion result = new Assertion();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.ASSERTION);

    final EnvelopedAspect envelopedAssertionInfo = aspects.get(Constants.ASSERTION_INFO_ASPECT_NAME);
    if (envelopedAssertionInfo != null) {
      result.setInfo(mapAssertionInfo(new AssertionInfo(envelopedAssertionInfo.getValue().data())));
    }

    final EnvelopedAspect envelopedAssertionActions = aspects.get(Constants.ASSERTION_ACTIONS_ASPECT_NAME);
    if (envelopedAssertionActions != null) {
      result.setActions(mapAssertionActions(new AssertionActions(envelopedAssertionActions.getValue().data())));
    }

    final EnvelopedAspect envelopedPlatformInstance = aspects.get(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    if (envelopedPlatformInstance != null) {
      final DataMap data = envelopedPlatformInstance.getValue().data();
      result.setPlatform(mapPlatform(new DataPlatformInstance(data)));
      result.setDataPlatformInstance(DataPlatformInstanceAspectMapper.map(new DataPlatformInstance(data)));
    } else {
      final DataPlatform unknownPlatform = new DataPlatform();
      unknownPlatform.setUrn(Constants.UNKNOWN_DATA_PLATFORM);
      result.setPlatform(unknownPlatform);
    }

    return result;
  }

  private static com.linkedin.datahub.graphql.generated.AssertionInfo mapAssertionInfo(
      final AssertionInfo gmsAssertionInfo) {
    final com.linkedin.datahub.graphql.generated.AssertionInfo assertionInfo =
        new com.linkedin.datahub.graphql.generated.AssertionInfo();
    assertionInfo.setType(AssertionType.valueOf(gmsAssertionInfo.getType().name()));
    if (gmsAssertionInfo.hasDatasetAssertion()) {
      DatasetAssertionInfo datasetAssertion = mapDatasetAssertionInfo(gmsAssertionInfo.getDatasetAssertion());
      assertionInfo.setDatasetAssertion(datasetAssertion);
    }
    // SLA Assertions
    if (gmsAssertionInfo.hasSlaAssertion()) {
      SlaAssertionInfo slaAssertionInfo = mapSlaAssertionInfo(gmsAssertionInfo.getSlaAssertion());
      assertionInfo.setSlaAssertion(slaAssertionInfo);
    }
    // Source Type
    if (gmsAssertionInfo.hasSource()) {
      assertionInfo.setSource(mapSource(gmsAssertionInfo.getSource()));
    }
    return assertionInfo;
  }

  private static com.linkedin.datahub.graphql.generated.AssertionActions mapAssertionActions(final AssertionActions gmsAssertionActions) {
    final com.linkedin.datahub.graphql.generated.AssertionActions result =
        new com.linkedin.datahub.graphql.generated.AssertionActions();
    if (gmsAssertionActions.hasOnFailure()) {
      result.setOnFailure(
          gmsAssertionActions.getOnFailure().stream()
              .map(AssertionMapper::mapAssertionAction)
              .collect(Collectors.toList()));
    }
    if (gmsAssertionActions.hasOnSuccess()) {
      result.setOnSuccess(
          gmsAssertionActions.getOnSuccess().stream()
              .map(AssertionMapper::mapAssertionAction)
              .collect(Collectors.toList()));
    }
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.AssertionAction mapAssertionAction(final AssertionAction gmsAssertionAction) {
    final com.linkedin.datahub.graphql.generated.AssertionAction result =
        new com.linkedin.datahub.graphql.generated.AssertionAction();
    result.setType(AssertionActionType.valueOf(gmsAssertionAction.getType().toString()));
    return result;
  }

  private static DatasetAssertionInfo mapDatasetAssertionInfo(
      final com.linkedin.assertion.DatasetAssertionInfo gmsDatasetAssertion) {
    DatasetAssertionInfo datasetAssertion = new DatasetAssertionInfo();
    datasetAssertion.setDatasetUrn(
        gmsDatasetAssertion.getDataset().toString());
    datasetAssertion.setScope(
        DatasetAssertionScope.valueOf(gmsDatasetAssertion.getScope().name()));
    if (gmsDatasetAssertion.hasFields()) {
      datasetAssertion.setFields(gmsDatasetAssertion.getFields()
          .stream()
          .map(AssertionMapper::mapDatasetSchemaField)
          .collect(Collectors.toList()));
    } else {
      datasetAssertion.setFields(Collections.emptyList());
    }
    // Agg
    if (gmsDatasetAssertion.hasAggregation()) {
      datasetAssertion.setAggregation(AssertionStdAggregation.valueOf(gmsDatasetAssertion.getAggregation().name()));
    }

    // Op
    datasetAssertion.setOperator(AssertionStdOperator.valueOf(gmsDatasetAssertion.getOperator().name()));

    // Params
    if (gmsDatasetAssertion.hasParameters()) {
      datasetAssertion.setParameters(mapParameters(gmsDatasetAssertion.getParameters()));
    }

    if (gmsDatasetAssertion.hasNativeType()) {
      datasetAssertion.setNativeType(gmsDatasetAssertion.getNativeType());
    }
    if (gmsDatasetAssertion.hasNativeParameters()) {
      datasetAssertion.setNativeParameters(StringMapMapper.map(gmsDatasetAssertion.getNativeParameters()));
    } else {
      datasetAssertion.setNativeParameters(Collections.emptyList());
    }
    if (gmsDatasetAssertion.hasLogic()) {
      datasetAssertion.setLogic(gmsDatasetAssertion.getLogic());
    }
    return datasetAssertion;
  }

  private static DataPlatform mapPlatform(final DataPlatformInstance platformInstance) {
    // Set dummy platform to be resolved.
    final DataPlatform partialPlatform = new DataPlatform();
    partialPlatform.setUrn(platformInstance.getPlatform().toString());
    return partialPlatform;
  }

  private static SchemaFieldRef mapDatasetSchemaField(final Urn schemaFieldUrn) {
    return new SchemaFieldRef(schemaFieldUrn.toString(), schemaFieldUrn.getEntityKey().get(1));
  }

  private static AssertionStdParameters mapParameters(final com.linkedin.assertion.AssertionStdParameters params) {
    final AssertionStdParameters result = new AssertionStdParameters();
    if (params.hasValue()) {
      result.setValue(mapParameter(params.getValue()));
    }
    if (params.hasMinValue()) {
      result.setMinValue(mapParameter(params.getMinValue()));
    }
    if (params.hasMaxValue()) {
      result.setMaxValue(mapParameter(params.getMaxValue()));
    }
    return result;
  }

  private static AssertionStdParameter mapParameter(final com.linkedin.assertion.AssertionStdParameter param) {
    final AssertionStdParameter result = new AssertionStdParameter();
    result.setType(AssertionStdParameterType.valueOf(param.getType().name()));
    result.setValue(param.getValue());
    return result;
  }

  private static SlaAssertionInfo mapSlaAssertionInfo(
      final com.linkedin.assertion.SlaAssertionInfo gmsSlaAssertionInfo) {
    SlaAssertionInfo slaAssertionInfo = new SlaAssertionInfo();
    slaAssertionInfo.setEntityUrn(gmsSlaAssertionInfo.getEntity().toString());
    slaAssertionInfo.setType(SlaAssertionType.valueOf(gmsSlaAssertionInfo.getType().name()));
    if (gmsSlaAssertionInfo.hasSchedule()) {
      slaAssertionInfo.setSchedule(mapSlaAssertionSchedule(gmsSlaAssertionInfo.getSchedule()));
    }
    return slaAssertionInfo;
  }

  private static SlaAssertionSchedule mapSlaAssertionSchedule(
      final com.linkedin.assertion.SlaAssertionSchedule gmsSlaAssertionSchedule) {
    SlaAssertionSchedule slaAssertionSchedule = new SlaAssertionSchedule();
    slaAssertionSchedule.setType(SlaAssertionScheduleType.valueOf(gmsSlaAssertionSchedule.getType().name()));
    if (gmsSlaAssertionSchedule.hasCron()) {
      slaAssertionSchedule.setCron(mapCronSchedule(gmsSlaAssertionSchedule.getCron()));
    }
    if (gmsSlaAssertionSchedule.hasFixedInterval()) {
      slaAssertionSchedule.setFixedInterval(mapFixedIntervalSchedule(gmsSlaAssertionSchedule.getFixedInterval()));
    }
    return slaAssertionSchedule;
  }

  private static FixedIntervalSchedule mapFixedIntervalSchedule(com.linkedin.assertion.FixedIntervalSchedule gmsFixedIntervalSchedule) {
    FixedIntervalSchedule fixedIntervalSchedule = new FixedIntervalSchedule();
    fixedIntervalSchedule.setUnit(DateInterval.valueOf(gmsFixedIntervalSchedule.getUnit().name()));
    fixedIntervalSchedule.setMultiple(gmsFixedIntervalSchedule.getMultiple());
    return fixedIntervalSchedule;
  }

  private static SlaCronSchedule mapCronSchedule(
      final com.linkedin.assertion.CronSchedule gmsCronSchedule) {
    SlaCronSchedule cronSchedule = new SlaCronSchedule();
    cronSchedule.setCron(gmsCronSchedule.getCron());
    cronSchedule.setTimezone(gmsCronSchedule.getTimezone());
    cronSchedule.setWindowStartOffsetMs(gmsCronSchedule.getWindowStartOffsetMs(GetMode.NULL));
    return cronSchedule;
  }

  private static AssertionSource mapSource(
      final com.linkedin.assertion.AssertionSource gmsSource) {
    AssertionSource result = new AssertionSource();
    result.setType(AssertionSourceType.valueOf(gmsSource.getType().toString()));
    return result;
  }

  private AssertionMapper() {
  }
}
