package com.linkedin.datahub.graphql.resolvers.glossary;

import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateGlossaryEntityInput;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.GlossaryNodeKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;


@Slf4j
@RequiredArgsConstructor
public class CreateGlossaryNodeResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;
  private final EntityService _entityService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final CreateGlossaryEntityInput input = bindArgument(environment.getArgument("input"), CreateGlossaryEntityInput.class);
    final Urn parentNode = input.getParentNode() != null ? UrnUtils.getUrn(input.getParentNode()) : null;

    return CompletableFuture.supplyAsync(() -> {
      if (GlossaryUtils.canManageChildrenEntities(context, parentNode, _entityClient)) {
        try {
          final GlossaryNodeKey key = new GlossaryNodeKey();

          final String id = input.getId() != null ? input.getId() : UUID.randomUUID().toString();
          key.setName(id);

          if (_entityClient.exists(EntityKeyUtils.convertEntityKeyToUrn(key, GLOSSARY_NODE_ENTITY_NAME), context.getAuthentication())) {
            throw new IllegalArgumentException("This Glossary Node already exists!");
          }

          final MetadataChangeProposal proposal = buildMetadataChangeProposalWithKey(key, GLOSSARY_NODE_ENTITY_NAME,
              GLOSSARY_NODE_INFO_ASPECT_NAME, mapGlossaryNodeInfo(input));

          String glossaryNodeUrn = _entityClient.ingestProposal(proposal, context.getAuthentication(), false);
          OwnerUtils.addCreatorAsOwner(context, glossaryNodeUrn, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER, _entityService);
          return glossaryNodeUrn;
        } catch (Exception e) {
          log.error("Failed to create GlossaryNode with id: {}, name: {}: {}", input.getId(), input.getName(), e.getMessage());
          throw new RuntimeException(String.format("Failed to create GlossaryNode with id: %s, name: %s", input.getId(), input.getName()), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  private GlossaryNodeInfo mapGlossaryNodeInfo(final CreateGlossaryEntityInput input) {
    final GlossaryNodeInfo result = new GlossaryNodeInfo();
    result.setName(input.getName());
    final String description = input.getDescription() != null ? input.getDescription() : "";
    result.setDefinition(description);
    if (input.getParentNode() != null) {
      try {
        final GlossaryNodeUrn parentNode = GlossaryNodeUrn.createFromString(input.getParentNode());
        result.setParentNode(parentNode, SetMode.IGNORE_NULL);
      } catch (URISyntaxException e) {
        throw new RuntimeException(String.format("Failed to create GlossaryNodeUrn from string: %s", input.getParentNode()), e);
      }
    }
    return result;
  }
}

