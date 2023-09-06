package com.linkedin.datahub.graphql.types.extendedproperty;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ExtendedPropertyEntity;
import com.linkedin.datahub.graphql.generated.QueryEntity;
import com.linkedin.datahub.graphql.types.query.QueryMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

import static com.linkedin.metadata.Constants.*;


@RequiredArgsConstructor
public class ExtendedPropertyType implements com.linkedin.datahub.graphql.types.EntityType<ExtendedPropertyEntity, String> {
  public static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of(EXTENDED_PROPERTY_DEFINITION_ASPECT_NAME);
  private final EntityClient _entityClient;

  @Override
  public EntityType type() {
    return EntityType.EXTENDED_PROPERTY;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<ExtendedPropertyEntity> objectClass() {
    return ExtendedPropertyEntity.class;
  }

  @Override
  public List<DataFetcherResult<ExtendedPropertyEntity>> batchLoad(@Nonnull List<String> urns, @Nonnull QueryContext context)
      throws Exception {
    final List<Urn> extendedPropertyUrns = urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(EXTENDED_PROPERTY_ENTITY_NAME, new HashSet<>(extendedPropertyUrns), ASPECTS_TO_FETCH,
              context.getAuthentication());

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : extendedPropertyUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(gmsResult -> gmsResult == null ? null
              : DataFetcherResult.<ExtendedPropertyEntity>newResult().data(ExtendedPropertyMapper.map(gmsResult)).build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Queries", e);
    }
  }
}