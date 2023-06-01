package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.UpdateDatasetAssertionInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.assertion.AssertionMapper;
import com.linkedin.metadata.service.AssertionService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class UpdateDatasetAssertionResolver implements DataFetcher<CompletableFuture<Assertion>> {

  private final AssertionService _assertionService;

  public UpdateDatasetAssertionResolver(@Nonnull final AssertionService assertionService) {
    _assertionService = Objects.requireNonNull(assertionService, "assertionService is required");
  }

  @Override
  public CompletableFuture<Assertion> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    final Urn assertionUrn = UrnUtils.getUrn(environment.getArgument("urn"));
    final UpdateDatasetAssertionInput input = ResolverUtils.bindArgument(environment.getArgument("input"), UpdateDatasetAssertionInput.class);

    return CompletableFuture.supplyAsync(() -> {
      // Check whether the current user is allowed to update the assertion.
      final AssertionInfo info = _assertionService.getAssertionInfo(assertionUrn);

      if (info == null) {
        throw new IllegalArgumentException(String.format("Failed to update Assertion. Assertion with urn %s does not exist.", assertionUrn));
      }

      final Urn asserteeUrn = AssertionUtils.getAsserteeUrnFromInfo(info);

      if (AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, asserteeUrn)) {

        // First update the existing assertion.
        _assertionService.updateDatasetAssertion(
            assertionUrn,
            DatasetAssertionScope.valueOf(input.getScope().toString()),
            input.getFieldUrns() != null ? input.getFieldUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList()) : null,
            input.getAggregation() != null ? AssertionStdAggregation.valueOf(input.getAggregation().toString()) : null,
            AssertionStdOperator.valueOf(input.getOperator().toString()),
            input.getParameters() != null ? AssertionUtils.createDatasetAssertionParameters(input.getParameters()) : null,
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