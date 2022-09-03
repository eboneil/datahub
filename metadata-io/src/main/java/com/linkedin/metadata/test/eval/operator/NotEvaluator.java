package com.linkedin.metadata.test.eval.operator;

import com.linkedin.metadata.test.definition.operator.Operands;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.value.BooleanType;
import com.linkedin.metadata.test.eval.ResolvedOperand;
import com.linkedin.metadata.test.eval.ResolvedOperands;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import lombok.extern.slf4j.Slf4j;


/**
 * Or operation evaluator. Checks whether any of input predicates returns true
 */
@Slf4j
public class NotEvaluator extends BaseOperatorEvaluator {

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.NOT;
  }

  @Override
  public void validate(Operands operands) throws InvalidOperandException {
    if (!(operands.size() == 1) || !operands.get()
        .stream()
        .allMatch(operand -> BooleanType.get().equals(operand.getExpression().valueType()))) {
      throw new InvalidOperandException(
          "NOT operator requires 1 boolean input operand.");
    }
  }

  @Override
  public Object evaluate(ResolvedOperands resolvedOperands) throws InvalidOperandException {

    ResolvedOperand operand = resolvedOperands.get(0); // Query response -> This will be list of string.

    log.debug(String.format("Invoking 'not' operator with operands %s", operand.getExpression().getValue()));

    return !((boolean) operand.getExpression().getValue());
  }
}
