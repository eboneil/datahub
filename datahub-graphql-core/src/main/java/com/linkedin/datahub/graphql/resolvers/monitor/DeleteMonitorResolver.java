package com.linkedin.datahub.graphql.resolvers.monitor;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.AuthUtils.*;


/**
 * GraphQL Resolver that deletes an Monitor.
 */
@Slf4j
public class DeleteMonitorResolver implements DataFetcher<CompletableFuture<Boolean>>  {

  private final EntityClient _entityClient;
  private final EntityService _entityService;

  public DeleteMonitorResolver(
      final EntityClient entityClient,
      final EntityService entityService) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _entityService = Objects.requireNonNull(entityService, "entityService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Urn monitorUrn = UrnUtils.getUrn(environment.getArgument("urn"));
    final Urn entityUrn = UrnUtils.getUrn(monitorUrn.getEntityKey().get(0));
    return CompletableFuture.supplyAsync(() -> {

      // 1. check the entity exists. If not, return false.
      if (!_entityService.exists(monitorUrn)) {
        return true;
      }

      if (isAuthorizedToDeleteMonitor(entityUrn, context)) {
        try {
          _entityClient.deleteEntity(monitorUrn, context.getAuthentication());

          // Asynchronously Delete all references to the entity (to return quickly)
          CompletableFuture.runAsync(() -> {
            try {
              _entityClient.deleteEntityReferences(monitorUrn, context.getAuthentication());
            } catch (RemoteInvocationException e) {
              log.error(String.format("Caught exception while attempting to clear all entity references for monitor with urn %s", monitorUrn), e);
            }
          });

          return true;
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform delete against monitor with urn %s", monitorUrn), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  /**
   * Determine whether the current user is allowed to remove an monitor.
   *
   * Since monitors are not currently tied to an entity directly, this simply requires a global platform
   * privilege called Manage Monitors. In the future, we may extend this to be more granular, allowing
   * users to manage monitors on a per-entity basis.
   */
  private boolean isAuthorizedToDeleteMonitor(final Urn entityUrn, final QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(
        ImmutableList.of(ALL_PRIVILEGES_GROUP,
            new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_MONITORS_PRIVILEGE.getType()))));
    return AuthorizationUtils.isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_MONITORS)
        || AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        entityUrn.getEntityType(),
        entityUrn.toString(),
        orPrivilegeGroups);  }
}