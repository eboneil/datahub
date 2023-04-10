package com.linkedin.metadata.kafka.hook.incident;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.IncidentsSummary;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.incident.IncidentServiceFactory;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.metadata.kafka.hook.HookUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.IncidentService;
import com.linkedin.metadata.service.IncidentsSummaryUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import static com.linkedin.metadata.Constants.*;


/**
 * This hook is responsible for maintaining the IncidentsSummary.pdl aspect of entities
 * on which Incidents may be raised. It handles both incident updates and incident soft deletions
 * to ensure that this aspect reflects the latest state of the incident.
 *
 * Hard deletes of incidents are not handled within this hook because the expectation is that deleteReferences will be invoked
 * to clean up references.
 */
@Slf4j
@Component
@Singleton
@Import({EntityRegistryFactory.class, IncidentServiceFactory.class, SystemAuthenticationFactory.class})
public class IncidentsSummaryHook implements MetadataChangeLogHook {

  private static final Set<ChangeType> SUPPORTED_UPDATE_TYPES = ImmutableSet.of(ChangeType.UPSERT, ChangeType.CREATE, ChangeType.RESTATE);
  private static final Set<String> SUPPORTED_UPDATE_ASPECTS = ImmutableSet.of(INCIDENT_INFO_ASPECT_NAME, STATUS_ASPECT_NAME);

  private final EntityRegistry _entityRegistry;
  private final IncidentService _incidentService;
  private final boolean _isEnabled;

  @Autowired
  public IncidentsSummaryHook(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final IncidentService incidentService,
      @Nonnull @Value("${incidents.hook.enabled:true}") Boolean isEnabled
  ) {
    _entityRegistry = Objects.requireNonNull(entityRegistry, "entityRegistry is required");
    _incidentService = Objects.requireNonNull(incidentService, "incidentService is required");
    _isEnabled = isEnabled;
  }

  @Override
  public void init() {
  }

  @Override
  public boolean isEnabled() {
    return _isEnabled;
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog event) {
    if (_isEnabled && isEligibleForProcessing(event)) {
      log.debug("Urn {} received by Incident Summary Hook.", event.getEntityUrn());
      final Urn urn = HookUtils.getUrnFromEvent(event, _entityRegistry);
      // Handle the deletion case.
      if (isIncidentSoftDeleted(event)) {
        handleIncidentSoftDeleted(urn);
      } else if (isIncidentUpdate(event)) {
        handleIncidentUpdated(urn);
      }
    }
  }

  /**
   * Handles an incident deletion by removing the incident from either resolved or active incidents.
   */
  private void handleIncidentSoftDeleted(@Nonnull final Urn incidentUrn) {
    // 1. Fetch incident info.
    IncidentInfo incidentInfo = _incidentService.getIncidentInfo(incidentUrn);

    // 2. Retrieve associated urns.
    if (incidentInfo != null) {
      final List<Urn> incidentEntities = incidentInfo.getEntities();

      // 3. For each urn, resolve the entity incidents aspect and remove from active and resolved incidents.
      for (Urn entityUrn : incidentEntities) {
        removeIncidentFromSummary(incidentUrn, entityUrn);
      }
    } else {
      log.warn(
          String.format(
              "Failed to find incidentInfo aspect for incident with urn %s. Skipping updating incident summary for related incidents!",
              incidentUrn));
    }
  }

  /**
   * Handle an incident update by adding to either resolved or active incidents for an entity.
   */
  private void handleIncidentUpdated(@Nonnull final Urn incidentUrn) {
    // 1. Fetch incident info.
    IncidentInfo incidentInfo = _incidentService.getIncidentInfo(incidentUrn);

    // 2. Retrieve associated urns.
    if (incidentInfo != null) {
      final List<Urn> incidentEntities = incidentInfo.getEntities();

      // 3. For each urn, resolve the entity incidents aspect and add to active or resolved incidents.
      for (Urn entityUrn : incidentEntities) {
        addIncidentToSummary(incidentUrn, entityUrn, incidentInfo.getStatus());
      }
    } else {
      log.warn(
          String.format(
              "Failed to find incidentInfo aspect for incident with urn %s. Skipping updating incident summary for related incidents!",
              incidentUrn));
    }
  }

  /**
   * Removes an incident to the IncidentSummary aspect for a related entity.
   */
  private void removeIncidentFromSummary(@Nonnull final Urn incidentUrn, @Nonnull final Urn entityUrn) {
    // 1. Fetch the latest incident summary for the entity
    IncidentsSummary summary = getIncidentsSummary(entityUrn);

    // 2. Remove the incident from active and resolved incidents
    IncidentsSummaryUtils.removeIncidentFromResolvedSummary(incidentUrn, summary);
    IncidentsSummaryUtils.removeIncidentFromActiveSummary(incidentUrn, summary);


    // 3. Emit the change back!
    updateIncidentSummary(entityUrn, summary);
  }

  /**
   * Adds an incident to the IncidentSummary aspect for a related entity.
   * This is used to search for entity by active and resolved incidents.
   */
  private void addIncidentToSummary(@Nonnull final Urn incidentUrn, @Nonnull final Urn entityUrn, @Nonnull final IncidentStatus status) {
    // 1. Fetch the latest incident summary for the entity
    IncidentsSummary summary = getIncidentsSummary(entityUrn);

    // 2. Add the incident to active or resolved incidents
    if (IncidentState.ACTIVE.equals(status.getState())) {
      // First, ensure this isn't in resolved anymore.
      IncidentsSummaryUtils.removeIncidentFromResolvedSummary(incidentUrn, summary);
      // Then, add to active.
      IncidentsSummaryUtils.addIncidentToActiveSummary(incidentUrn, summary);

    } else if (IncidentState.RESOLVED.equals(status.getState())) {
      // First, ensure this isn't in active anymore.
      IncidentsSummaryUtils.removeIncidentFromActiveSummary(incidentUrn, summary);
      // Then, add to resolved.
      IncidentsSummaryUtils.addIncidentToResolvedSummary(incidentUrn, summary);
    }

    // 3. Emit the change back!
    updateIncidentSummary(entityUrn, summary);
  }

  @Nonnull
  private IncidentsSummary getIncidentsSummary(@Nonnull final Urn entityUrn) {
    IncidentsSummary maybeIncidentsSummary = _incidentService.getIncidentsSummary(entityUrn);
    return maybeIncidentsSummary == null ? new IncidentsSummary() : maybeIncidentsSummary;
  }

  /**
   * Returns true if the event should be processed, which is only true if the change is on the incident status aspect
   */
  private boolean isEligibleForProcessing(@Nonnull final MetadataChangeLog event) {
    return isIncidentSoftDeleted(event) || isIncidentUpdate(event);
  }

  /**
   * Returns true if an incident is being soft-deleted.
   */
  private boolean isIncidentSoftDeleted(@Nonnull final MetadataChangeLog event) {
    return SUPPORTED_UPDATE_TYPES.contains(event.getChangeType()) && isSoftDeletionEvent(event);
  }

  private boolean isSoftDeletionEvent(@Nonnull final MetadataChangeLog event) {
    if (STATUS_ASPECT_NAME.equals(event.getAspectName()) && event.getAspect() != null) {
      final Status status = GenericRecordUtils.deserializeAspect(
          event.getAspect().getValue(),
          event.getAspect().getContentType(),
          Status.class);
      return status.hasRemoved() && status.isRemoved();
    }
    return false;
  }

  /**
   * Returns true if the event represents an incident deletion event.
   */
  private boolean isIncidentUpdate(@Nonnull final MetadataChangeLog event) {
    return INCIDENT_ENTITY_NAME.equals(event.getEntityType())
        && SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && SUPPORTED_UPDATE_ASPECTS.contains(event.getAspectName());
  }

  /**
   * Updates the incidents summary for a given entity
   */
  private void updateIncidentSummary(@Nonnull final Urn entityUrn, @Nonnull final IncidentsSummary newSummary) {
    try {
      _incidentService.updateIncidentsSummary(entityUrn, newSummary);
    } catch (Exception e) {
      log.error(
          String.format("Failed to updated incidents summary for entity with urn %s! Skipping updating the summary", entityUrn), e);
    }
  }
}
