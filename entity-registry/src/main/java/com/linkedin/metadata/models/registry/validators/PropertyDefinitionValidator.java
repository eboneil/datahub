package com.linkedin.metadata.models.registry.validators;

import com.linkedin.common.ExtendedPropertyDefinition;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectPayloadValidator;
import com.linkedin.metadata.models.AspectValidationException;
import com.linkedin.metadata.models.registry.AspectRetriever;

import static com.linkedin.common.PropertyCardinality.*;


public class PropertyDefinitionValidator implements AspectPayloadValidator {

  public boolean validatePreCommit(RecordTemplate previousAspect, RecordTemplate proposedAspect) throws AspectValidationException {
    if (previousAspect != null) {
      ExtendedPropertyDefinition previousDefinition = (ExtendedPropertyDefinition) previousAspect;
      ExtendedPropertyDefinition newDefinition = (ExtendedPropertyDefinition) proposedAspect;
      if (!newDefinition.getValueType().equals(previousDefinition.getValueType())) {
        throw new AspectValidationException("Value type cannot be changed as this is a backwards incompatible change");
      }
      if (newDefinition.getCardinality().equals(SINGLE) && previousDefinition.getCardinality().equals(MULTIPLE)) {
        throw new AspectValidationException("Property definition cardinality cannot be changed from MULTI to SINGLE");
      }
    }
    return true;
  };

}
