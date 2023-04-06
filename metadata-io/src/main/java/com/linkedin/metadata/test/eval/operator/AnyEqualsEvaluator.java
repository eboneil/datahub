package com.linkedin.metadata.test.eval.operator;

import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.test.definition.expression.Expression;
import com.linkedin.metadata.test.definition.operator.Operands;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.value.ListType;
import com.linkedin.metadata.test.definition.value.StringType;
import com.linkedin.metadata.test.definition.value.ValueType;
import com.linkedin.metadata.test.eval.ResolvedOperand;
import com.linkedin.metadata.test.eval.ResolvedOperands;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Any equals operation evaluator.
 *
 * Accepts strings, or string lists as operands and returns whether there are any
 * equivalence matches between the two sets as a boolean.
 */
public class AnyEqualsEvaluator extends BaseOperatorEvaluator {

  private static final Set<ValueType> SUPPORTED_OPERAND_TYPES = ImmutableSet.of(
      new StringType(),
      new ListType(new StringType())
  );

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.ANY_EQUALS;
  }

  @Override
  public void validate(Operands operands) throws InvalidOperandException {
    if (operands.size() != 2) {
      throw new InvalidOperandException(
          "Invalid params for the equals operation: Requires 2 input operands");
    }
    // Validate that both input types are string lists.
    if (!isSupportedOperandType(operands.get(0).getExpression()) || !isSupportedOperandType(operands.get(1).getExpression())) {
      throw new InvalidOperandException(
          "Invalid params for the equals operation: Requires 2 string list operands");
    }
  }

  @Override
  public Object evaluate(ResolvedOperands resolvedOperands) throws InvalidOperandException {

    ResolvedOperand operand1 = resolvedOperands.get(0); // Query response -> This will be list of string.
    ResolvedOperand operand2 = resolvedOperands.get(1); // -> This will be user provided or list.

    Set<String> operand1Values = toStringSet(operand1);
    Set<String> operand2Values = toStringSet(operand2);

    return operand1Values.stream().anyMatch(operand2Values::contains);
  }

  private Set<String> toStringSet(ResolvedOperand operand) {
    if (operand.getExpression().getValue() instanceof String) {
      return Collections.singleton((String) operand.getExpression().getValue());
    } else if (operand.getExpression().getValue() instanceof List) {
      return new HashSet<>((List<String>) operand.getExpression().getValue());
    } else {
      throw new IllegalArgumentException(
          String.format("Failed to evaluate AnyEquals operator against operand of type %s. Expected string or string list.",
              operand.getExpression().getValue().getClass()));
    }
  }

  private boolean isSupportedOperandType(Expression exp) {
    return SUPPORTED_OPERAND_TYPES.contains(exp.valueType());
  }
}