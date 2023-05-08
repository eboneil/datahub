package com.linkedin.datahub.graphql.resolvers.connection;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.connection.DataHubConnectionDetailsType;
import com.linkedin.connection.DataHubJsonConnection;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DataHubConnection;
import com.linkedin.datahub.graphql.generated.UpsertDataHubConnectionInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.secret.SecretService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@Slf4j
public class UpsertConnectionResolver implements DataFetcher<CompletableFuture<DataHubConnection>> {

  private final ConnectionService _connectionService;
  private final SecretService _secretService;

  public UpsertConnectionResolver(@Nonnull final EntityClient entityClient, @Nonnull final SecretService secretService) {
    _connectionService = new ConnectionService(Objects.requireNonNull(entityClient, "entityClient cannot be null"));
    _secretService = Objects.requireNonNull(secretService, "secretService cannot be null");
  }

  @Override
  public CompletableFuture<DataHubConnection> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final UpsertDataHubConnectionInput input = bindArgument(environment.getArgument("input"), UpsertDataHubConnectionInput.class);
    final Authentication authentication = context.getAuthentication();

    return CompletableFuture.supplyAsync(() -> {

      if (!ConnectionUtils.canManageConnections(context)) {
        throw new AuthorizationException(
            "Unauthorized to upsert Connection. Please contact your DataHub administrator for more information.");
      }

      try {
        final Urn connectionUrn = _connectionService.upsertConnection(
            input.getId(),
            UrnUtils.getUrn(input.getPlatformUrn()),
            DataHubConnectionDetailsType.valueOf(input.getType().toString()),
            input.getJson() != null
                // Encrypt payload
                ? new DataHubJsonConnection().setEncryptedBlob(_secretService.encrypt(input.getJson().getBlob()))
                : null,
            authentication
        );

        final EntityResponse connectionResponse = _connectionService.getConnectionEntityResponse(
            connectionUrn,
            context.getAuthentication()
        );
        return ConnectionMapper.map(connectionResponse, _secretService);
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to upsert a Connection from input %s", input), e);
      }
    });
  }
}
