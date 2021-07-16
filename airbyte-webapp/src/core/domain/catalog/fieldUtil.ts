import { JSONSchema7Definition } from "json-schema";
import Status from "core/statuses";
import { CommonRequestError } from "core/request/CommonRequestError";

import { SourceDiscoverSchemaRead } from "./api";
import { SyncSchemaField } from "./models";
import { ConnectionNamespaceDefinition } from "../connection";
import { SOURCE_NAMESPACE_TAG } from "../connector/source";

function toInnerModel(
  result: SourceDiscoverSchemaRead
): SourceDiscoverSchemaRead {
  if (result.jobInfo?.status === Status.FAILED || !result.catalog) {
    // @ts-ignore address this case
    const e = new CommonRequestError(result);
    // Generate error with failed status and received logs
    e._status = 400;
    // @ts-ignore address this case
    e.response = result.jobInfo;
    throw e;
  }

  return result;
}

const traverseSchemaToField = (
  jsonSchema: JSONSchema7Definition,
  key: string
): SyncSchemaField[] => {
  // For the top level we should not insert an extra object
  return traverseJsonSchemaProperties(jsonSchema, key)[0].fields ?? [];
};

const traverseJsonSchemaProperties = (
  jsonSchema: JSONSchema7Definition,
  key: string,
  path: string = key,
  depth = 0
): SyncSchemaField[] => {
  if (typeof jsonSchema === "boolean") {
    return [];
  }

  let fields: SyncSchemaField[] | undefined;
  if (jsonSchema.properties) {
    fields = Object.entries(jsonSchema.properties)
      .flatMap(([k, schema]) =>
        traverseJsonSchemaProperties(
          schema,
          k,
          depth === 0 ? k : `${path}.${k}`,
          depth + 1
        )
      )
      .flat(2);
  }

  return [
    {
      cleanedName: key,
      name: path,
      key,
      fields,
      type:
        (Array.isArray(jsonSchema.type)
          ? jsonSchema.type.find((t) => t !== "null") ?? jsonSchema.type[0]
          : jsonSchema.type) ?? "null",
    },
  ];
};

type NamespaceOptions =
  | {
      namespaceDefinition:
        | ConnectionNamespaceDefinition.Source
        | ConnectionNamespaceDefinition.Destination;
      sourceNamespace?: string;
    }
  | {
      namespaceDefinition: ConnectionNamespaceDefinition.CustomFormat;
      namespaceFormat: string;
      sourceNamespace?: string;
    };

function getDestinationNamespace(opt: NamespaceOptions): string {
  const destinationSetting = "<destination schema>";
  switch (opt.namespaceDefinition) {
    case ConnectionNamespaceDefinition.Source:
      return opt.sourceNamespace ?? destinationSetting;
    case ConnectionNamespaceDefinition.Destination:
      return destinationSetting;
    case ConnectionNamespaceDefinition.CustomFormat:
      if (!opt.sourceNamespace?.trim()) {
        return destinationSetting;
      }

      return opt.namespaceFormat.replace(
        SOURCE_NAMESPACE_TAG,
        opt.sourceNamespace
      );
  }
}

export { getDestinationNamespace, traverseSchemaToField, toInnerModel };
