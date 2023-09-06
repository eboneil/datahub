package com.linkedin.datahub.graphql.types.extendedproperty;

import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ExtendedPropertyDefinition;
import com.linkedin.datahub.graphql.generated.ExtendedPropertyEntity;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class ExtendedPropertyMapper implements ModelMapper<EntityResponse, ExtendedPropertyEntity> {

  public static final ExtendedPropertyMapper INSTANCE = new ExtendedPropertyMapper();

  public static ExtendedPropertyEntity map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  @Override
  public ExtendedPropertyEntity apply(@Nonnull final EntityResponse entityResponse) {
    final ExtendedPropertyEntity result = new ExtendedPropertyEntity();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.EXTENDED_PROPERTY);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<ExtendedPropertyEntity> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(EXTENDED_PROPERTY_DEFINITION_ASPECT_NAME, (this::mapExtendedPropertyDefinition));
    return mappingHelper.getResult();
  }

  private void mapExtendedPropertyDefinition(@Nonnull ExtendedPropertyEntity extendedProperty, @Nonnull DataMap dataMap) {
    com.linkedin.common.ExtendedPropertyDefinition gmsDefinition = new com.linkedin.common.ExtendedPropertyDefinition(dataMap);
    ExtendedPropertyDefinition definition = new ExtendedPropertyDefinition();
    definition.setDisplayName(gmsDefinition.getDisplayName());
    extendedProperty.setDefinition(definition);
  }
}
