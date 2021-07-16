import { SourceDefinition } from "core/resources/SourceDefinition";
import { ConnectorDefinition } from "./connector";

export function isSourceDefinition(
  connector: ConnectorDefinition
): connector is SourceDefinition {
  return (connector as SourceDefinition).sourceDefinitionId !== undefined;
}

// eslint-disable-next-line no-template-curly-in-string
export const SOURCE_NAMESPACE_TAG = "${SOURCE_NAMESPACE}";
