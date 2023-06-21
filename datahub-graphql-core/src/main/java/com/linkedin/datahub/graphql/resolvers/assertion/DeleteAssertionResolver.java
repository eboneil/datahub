package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;


/**
 * GraphQL Resolver that deletes an Assertion.
 */
@Slf4j
public class DeleteAssertionResolver implements DataFetcher<CompletableFuture<Boolean>>  {

  private final EntityClient _entityClient;
  private final EntityService _entityService;

  public DeleteAssertionResolver(final EntityClient entityClient, final EntityService entityService) {
    _entityClient = entityClient;
    _entityService = entityService;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Urn assertionUrn = Urn.createFromString(environment.getArgument("urn"));
    return CompletableFuture.supplyAsync(() -> {

      // 1. check the entity exists. If not, return false.
      if (!_entityService.exists(assertionUrn)) {
        return true;
      }

      if (isAuthorizedToDeleteAssertion(context, assertionUrn)) {
          try {
            _entityClient.deleteEntity(assertionUrn, context.getAuthentication());

            // Asynchronously Delete all references to the entity (to return quickly)
            // TODO: Actually delete any monitors associated with the assertion.
            CompletableFuture.runAsync(() -> {
              try {
                _entityClient.deleteEntityReferences(assertionUrn, context.getAuthentication());
              } catch (Exception e) {
                log.error(String.format("Caught exception while attempting to clear all entity references for assertion with urn %s", assertionUrn), e);
              }
            });

            return true;
          } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to perform delete against assertion with urn %s", assertionUrn), e);
          }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  /**
   * Determine whether the current user is allowed to remove an assertion.
   */
  private boolean isAuthorizedToDeleteAssertion(final QueryContext context, final Urn assertionUrn) {

    // 2. fetch the assertion info
    AssertionInfo info =
        (AssertionInfo) MutationUtils.getAspectFromEntity(
            assertionUrn.toString(), Constants.ASSERTION_INFO_ASPECT_NAME, _entityService, null);

    if (info != null) {
      // 3. check whether the actor has permission to edit the assertions on the assertee
      final Urn asserteeUrn = AssertionUtils.getAsserteeUrnFromInfo(info);
      return AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, asserteeUrn);
    }

    return true;
  }
}