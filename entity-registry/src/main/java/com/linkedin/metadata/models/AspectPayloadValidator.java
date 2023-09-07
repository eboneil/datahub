package com.linkedin.metadata.models;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.registry.AspectRetriever;

import static com.linkedin.events.metadata.ChangeType.*;


public interface AspectPayloadValidator {

  default ChangeType[] operationsSupported() {
    return new ChangeType[]{UPSERT};
  }

  default boolean validateAspectUpsert(Urn entityUrn, RecordTemplate aspectPayload, String aspectName, AspectRetriever aspectRetriever)
      throws AspectValidationException {
      return true;
  }

  default boolean validatePreCommit(RecordTemplate previousAspect, RecordTemplate proposedAspect) throws AspectValidationException {
    return true;
  };

}
