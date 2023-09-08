package com.linkedin.datahub.graphql.resolvers.type;

import com.linkedin.datahub.graphql.generated.FloatValue;
import com.linkedin.datahub.graphql.generated.StringValue;
import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;

public class PropertyValueResolver implements TypeResolver {

  public static final String STRING_VALUE = "StringValue";
  public static final String FLOAT_VALUE = "FloatValue";

  @Override
  public GraphQLObjectType getType(TypeResolutionEnvironment env) {
    if (env.getObject() instanceof StringValue) {
      return env.getSchema().getObjectType(STRING_VALUE);
    } else if (env.getObject() instanceof FloatValue) {
      return env.getSchema().getObjectType(FLOAT_VALUE);
    } else {
      throw new RuntimeException("Unrecognized object type provided to type resolver, Type:" + env.getObject().toString());
    }
  }
}
