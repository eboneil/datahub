package com.linkedin.datahub.graphql.resolvers.test;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateTestInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.key.TestKey;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.TestInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.test.TestUtils.*;
import static com.linkedin.metadata.Constants.*;


/**
 * Creates or updates a Test. Requires the MANAGE_TESTS privilege.
 */
@RequiredArgsConstructor
public class CreateTestResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;
  private final TestEngine _testEngine;

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final Actor actor = authentication.getActor();
    final CreateTestInput input = bindArgument(environment.getArgument("input"), CreateTestInput.class);

    return CompletableFuture.supplyAsync(() -> {

      if (canManageTests(context)) {

        try {

          // Create new test
          // Since we are creating a new Test, we need to generate a unique UUID.
          final UUID uuid = UUID.randomUUID();
          final String uuidStr = input.getId() == null ? uuid.toString() : input.getId();

          // Create the Ingestion source key
          final TestKey key = new TestKey();
          key.setId(uuidStr);

          if (_entityClient.exists(EntityKeyUtils.convertEntityKeyToUrn(key, TEST_ENTITY_NAME),
              authentication)) {
            throw new IllegalArgumentException("This Test already exists!");
          }

          // Create the Test info.
          final TestInfo info = mapCreateTestInput(input, actor);

          // Validate test info
          ValidationResult validationResult = _testEngine.validateJson(info.getDefinition().getJson());
          if (!validationResult.isValid()) {
            throw new RuntimeException(
                "Failed to validate test definition: \n" + String.join("\n", validationResult.getMessages()));
          }

          final MetadataChangeProposal proposal = buildMetadataChangeProposalWithKey(key, TEST_ENTITY_NAME, TEST_INFO_ASPECT_NAME, info);
          String ingestResult;
          try {
            ingestResult = _entityClient.ingestProposal(proposal, context.getAuthentication(), false);
          } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to create test with urn %s", input), e);
          }

          _testEngine.invalidateCache();
          return ingestResult;

        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to create test with urn %s", input), e);
        }
      }
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  private static TestInfo mapCreateTestInput(final CreateTestInput input, final Actor actor) {
    final TestInfo result = new TestInfo();
    result.setName(input.getName());
    result.setCategory(input.getCategory());
    result.setDescription(input.getDescription(), SetMode.IGNORE_NULL);
    result.setDefinition(mapDefinition(input.getDefinition()));

    long currentTimeMillis = System.currentTimeMillis();
    result.setLastUpdatedTimestamp(currentTimeMillis);
    final AuditStamp auditStamp =
        new AuditStamp().setTime(currentTimeMillis).setActor(UrnUtils.getUrn(actor.toUrnStr()));
    result.setCreated(auditStamp);
    result.setLastUpdated(auditStamp);
    return result;
  }
}
