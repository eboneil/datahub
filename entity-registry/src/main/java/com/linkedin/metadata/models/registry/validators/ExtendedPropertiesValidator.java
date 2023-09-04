package com.linkedin.metadata.models.registry.validators;

import com.linkedin.common.ExtendedProperties;
import com.linkedin.common.ExtendedPropertyDefinition;
import com.linkedin.common.ExtendedPropertyValueAssignment;
import com.linkedin.common.PropertyCardinality;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectPayloadValidator;
import com.linkedin.metadata.models.AspectValidationException;
import com.linkedin.metadata.models.registry.AspectRetriever;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ExtendedPropertiesValidator implements AspectPayloadValidator {

  @Override
  public boolean validateAspectUpsert(Urn entityUrn, RecordTemplate aspectPayload, String aspectName, AspectRetriever aspectRetriever)
      throws AspectValidationException {
    ExtendedProperties extendedProperties = (ExtendedProperties) aspectPayload;
    log.warn("Validator called with {}", extendedProperties.toString());
    Map<Urn, List<ExtendedPropertyValueAssignment>> extendedPropertiesMap = extendedProperties.getProperties()
        .stream()
        .collect(Collectors.groupingBy(x -> x.getPropertyUrn(),
            HashMap::new,
            Collectors.toCollection(
        ArrayList::new)));

    for (Map.Entry<Urn, List<ExtendedPropertyValueAssignment>> entry: extendedPropertiesMap.entrySet()) {
      Urn propertyUrn = entry.getKey();
      String property = propertyUrn.toString();
      assert propertyUrn.getEntityType().equals("extendedProperty");
      ExtendedPropertyDefinition extendedPropertyDefinition = (ExtendedPropertyDefinition) aspectRetriever.getLatestAspect(propertyUrn,
          "propertyDefinition");
      log.warn("Retrieved property definition for {}. {}", propertyUrn, extendedPropertyDefinition);
      if (extendedPropertyDefinition != null) {
        List<ExtendedPropertyValueAssignment> values = entry.getValue();
        // Check cardinality
        if (extendedPropertyDefinition.getCardinality() == PropertyCardinality.SINGLE) {
          if (values.size() > 1) {
            throw new AspectValidationException(
                "Property: " + property + " has cardinality 1, but multiple values were assigned: " + values.toString());
          }
        }
        // Check types
        for (ExtendedPropertyValueAssignment value: values) {
          switch (extendedPropertyDefinition.getValueType()) {
            case STRING:
            case MARKDOWN: {
              log.debug("Property definition demands a string value. {}, {}", value.getValue().isString(),
                  value.getValue().isDouble());
              if (!value.getValue().isString()) {
                throw new AspectValidationException(
                    "Property: " + property + ", value: " + value.getValue().toString() + " should be a string");
              }
              break;
            }
            case NUMBER: {
              log.debug("Property definition demands a numeric value. {}, {}", value.getValue().isString(),
                  value.getValue().isDouble());
              if (!value.getValue().isDouble()) {
                throw new AspectValidationException(
                    "Property: " + property + ", value: " + value.getValue().toString() + " should be a number");
              }
              break;
            }
            default: {
              throw new AspectValidationException(
                  "Validation support for type " + extendedPropertyDefinition.getValueType() + " is not yet implemented.");
            }
          }
        }
      }
    }
    return true;
  }
}
