package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.CreateFreshnessAssertionInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.assertion.AssertionMapper;
import com.linkedin.metadata.service.AssertionService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class CreateFreshnessAssertionResolver implements DataFetcher<CompletableFuture<Assertion>> {

  private final AssertionService _assertionService;

  public CreateFreshnessAssertionResolver(@Nonnull final AssertionService assertionService) {
    _assertionService = Objects.requireNonNull(assertionService, "assertionService is required");
  }

  @Override
  public CompletableFuture<Assertion> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final CreateFreshnessAssertionInput
        input = ResolverUtils.bindArgument(environment.getArgument("input"), CreateFreshnessAssertionInput.class);
    final Urn asserteeUrn = UrnUtils.getUrn(input.getEntityUrn());

    return CompletableFuture.supplyAsync(() -> {

      if (AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, asserteeUrn)) {

        // First create the new assertion.
        final Urn assertionUrn = _assertionService.createFreshnessAssertion(
            asserteeUrn,
            FreshnessAssertionType.valueOf(input.getType().toString()),
            AssertionUtils.createFreshnessAssertionSchedule(input.getSchedule()),
            input.getActions() != null ? AssertionUtils.createAssertionActions(input.getActions()) : null,
            context.getAuthentication()
        );

        // Then, return the new assertion
        return AssertionMapper.map(_assertionService.getAssertionEntityResponse(assertionUrn, context.getAuthentication()));
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }
}