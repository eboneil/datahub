package com.linkedin.metadata.models.registry.validators;

import com.linkedin.common.ExtendedProperties;
import com.linkedin.common.ExtendedPropertyDefinition;
import com.linkedin.common.ExtendedPropertyValueAssignment;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectPayloadValidator;
import com.linkedin.metadata.models.AspectValidationException;
import com.linkedin.metadata.models.registry.AspectRetriever;
import java.net.URISyntaxException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ExtendedPropertiesValidator implements AspectPayloadValidator {

  @Override
  public boolean validateAspectUpsert(Urn entityUrn, RecordTemplate aspectPayload, String aspectName, AspectRetriever aspectRetriever)
      throws AspectValidationException {
    ExtendedProperties extendedProperties = (ExtendedProperties) aspectPayload;
    log.warn("Validator called with {}", extendedProperties.toString());
    for (Map.Entry<String, ExtendedPropertyValueAssignment> entry: extendedProperties.getProperties().entrySet()) {
      String property = entry.getKey();
      try {
        Urn propertyUrn = Urn.createFromString(property);
        assert propertyUrn.getEntityType().equals("extendedProperty");
        ExtendedPropertyDefinition extendedPropertyDefinition = (ExtendedPropertyDefinition) aspectRetriever.getLatestAspect(propertyUrn,
            "propertyDefinition");
        log.warn("Retrieved property definition for {}. {}", propertyUrn, extendedPropertyDefinition);
        if (extendedPropertyDefinition != null) {
          ExtendedPropertyValueAssignment value = entry.getValue();
          switch (extendedPropertyDefinition.getValueType()) {
            case STRING: case MARKDOWN: {
              log.debug("Property definition demands a string value. {}, {}", value.getValue().isString(), value.getValue().isDouble());
              if (!value.getValue().isString()) {
                throw new AspectValidationException("Property: " + property + ", value: " + value.getValue().toString() + " should be a string");
              }
              break;
            }
            case NUMBER: {
              log.debug("Property definition demands a numberic value. {}, {}", value.getValue().isString(), value.getValue().isDouble());
              if (!value.getValue().isDouble()) {
                throw new AspectValidationException("Property: " + property + ", value: " + value.getValue().toString() + " should be a number");
              }
              break;
            }
            default: {
              throw new AspectValidationException("Validation support for type " + extendedPropertyDefinition.getValueType() + " is not yet implemented.");
            }
          }
        }
      } catch (URISyntaxException e) {
        throw new AspectValidationException("Property " + property + " should be an urn", e);
      }

    }
    return true;
  }
}
