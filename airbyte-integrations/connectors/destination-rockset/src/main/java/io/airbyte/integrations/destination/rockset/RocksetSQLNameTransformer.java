package io.airbyte.integrations.destination.rockset;

import io.airbyte.integrations.destination.ExtendedNameTransformer;

public class RocksetSQLNameTransformer extends ExtendedNameTransformer {

  @Override
  protected String applyDefaultCase(String input) {
    return input.toLowerCase();
  }

}

