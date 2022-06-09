package com.linkedin.metadata.test.eval.operation;

import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class EqualsEvaluator implements BaseOperationEvaluator {
  private static final String VALUES = "values";
  private static final String VALUE = "value";

  @Override
  public String getOperation() {
    return "equals";
  }

  @Override
  public void validate(OperationParams params) throws IllegalArgumentException {
    if (params.getParams().containsKey(VALUES) && params.getParams().get(VALUES) instanceof List) {
      return;
    }
    if (params.getParams().containsKey(VALUE)) {
      return;
    }
    throw new IllegalArgumentException(
        "Invalid params for the EQUALS operation: Need to have either values or value fields set");
  }

  @Override
  public boolean evaluate(TestQueryResponse queryResponse, OperationParams params) {
    Set<String> compareAgainst;
    if (params.getParams().containsKey(VALUES) && params.getParams().get(VALUES) instanceof List) {
      compareAgainst =
          ((List<Object>) params.getParams().get(VALUES)).stream().map(Object::toString).collect(Collectors.toSet());
    } else if (params.getParams().containsKey(VALUE)) {
      compareAgainst = Collections.singleton(params.getParams().get(VALUE).toString());
    } else {
      throw new IllegalArgumentException(
          "Invalid params for the EQUALS operation: Need to have either values or value fields set");
    }

    return queryResponse.getValues().stream().anyMatch(compareAgainst::contains);
  }
}
