package com.linkedin.metadata.connection;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.connection.DataHubConnectionDetails;
import com.linkedin.connection.DataHubConnectionDetailsType;
import com.linkedin.connection.DataHubJsonConnection;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.DataHubConnectionKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ConnectionService {

  private final EntityClient _entityClient;
  private final Authentication _systemAuthentication;

  /**
   * Upserts a DataHub connection. If the connection with the provided ID already exists,
   * then it will be overwritten.
   *
   * This method assumes that authorization has already been verified at the calling layer.
   *
   * @return the URN of the new connection.
   */
  public Urn upsertConnection(
      @Nullable final String id,
      @Nonnull final Urn platformUrn,
      @Nonnull final DataHubConnectionDetailsType type,
      @Nullable final DataHubJsonConnection json,
      @Nonnull final Authentication authentication
  ) {
    Objects.requireNonNull(platformUrn, "platformUrn must not be null");
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");

    // 1. Optionally generate new connection id
    final String connectionId = id != null ? id : UUID.randomUUID().toString();
    final DataHubConnectionKey key = new DataHubConnectionKey().setId(connectionId);
    final Urn connectionUrn = EntityKeyUtils.convertEntityKeyToUrn(key, Constants.DATAHUB_CONNECTION_ENTITY_NAME);

    // 2. Build Connection Details
    final DataHubConnectionDetails details = new DataHubConnectionDetails();
    details.setType(type);
    details.setName(id, SetMode.IGNORE_NULL);

    if (DataHubConnectionDetailsType.JSON.equals(details.getType())) {
      if (json != null) {
        details.setJson(json);
      } else {
        throw new IllegalArgumentException("Connections with type JSON must provide the field 'json'.");
      }
    }

    // 3. Build platform instance
    final DataPlatformInstance platformInstance = new DataPlatformInstance();
    platformInstance.setPlatform(platformUrn);

    // 4. Write changes to GMS
    try {
      final List<MetadataChangeProposal> aspectsToIngest = new ArrayList<>();
      aspectsToIngest.add(
          AspectUtils.buildMetadataChangeProposal(connectionUrn, Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME, details));
      aspectsToIngest.add(
          AspectUtils.buildMetadataChangeProposal(connectionUrn, Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME, platformInstance));
      _entityClient.batchIngestProposals(aspectsToIngest, authentication, false);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to upsert Connection with urn %s", connectionUrn), e);
    }
    return connectionUrn;
  }

  @Nullable
  public DataHubConnectionDetails getConnectionDetails(@Nonnull final Urn connectionUrn) {
    Objects.requireNonNull(connectionUrn, "connectionUrn must not be null");
    final EntityResponse response = getConnectionEntityResponse(connectionUrn, this._systemAuthentication);
    if (response != null && response.getAspects().containsKey(Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME)) {
      return new DataHubConnectionDetails(response.getAspects().get(Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  @Nullable
  public EntityResponse getConnectionEntityResponse(@Nonnull final Urn connectionUrn, @Nonnull final Authentication authentication) {
    try {
      return _entityClient.getV2(
          Constants.DATAHUB_CONNECTION_ENTITY_NAME,
          connectionUrn,
          ImmutableSet.of(Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME, Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME),
          authentication
      );
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to retrieve Connection with urn %s", connectionUrn), e);
    }
  }
}
