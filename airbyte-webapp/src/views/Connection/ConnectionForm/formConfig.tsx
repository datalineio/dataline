import { useMemo } from "react";
import { useIntl } from "react-intl";
import * as yup from "yup";

import {
  AirbyteStreamConfiguration,
  DestinationSyncMode,
  SyncMode,
  SyncSchema,
  SyncSchemaStream,
} from "core/domain/catalog";
import { ValuesProps } from "components/hooks/services/useConnectionHook";
import {
  Normalization,
  NormalizationType,
  Operation,
  OperatorType,
  Transformation,
} from "core/domain/connection/operation";
import { DropDownRow } from "components";
import FrequencyConfig from "config/FrequencyConfig.json";
import { DestinationDefinitionSpecification } from "core/resources/DestinationDefinitionSpecification";
import { Connection, ScheduleProperties } from "core/resources/Connection";
import { ConnectionNamespaceDefinition } from "core/domain/connection";
import { SOURCE_NAMESPACE_TAG } from "core/domain/connector/source";

type FormikConnectionFormValues = {
  schedule?: ScheduleProperties | null;
  prefix: string;
  syncCatalog: SyncSchema;
  namespaceDefinition: ConnectionNamespaceDefinition;
  namespaceFormat: string;
  transformations?: Transformation[];
  normalization?: NormalizationType;
};

type ConnectionFormValues = ValuesProps;

const SUPPORTED_MODES: [SyncMode, DestinationSyncMode][] = [
  [SyncMode.FullRefresh, DestinationSyncMode.Overwrite],
  [SyncMode.FullRefresh, DestinationSyncMode.Append],
  [SyncMode.Incremental, DestinationSyncMode.Append],
  [SyncMode.Incremental, DestinationSyncMode.Dedupted],
];

const DEFAULT_TRANSFORMATION: Transformation = {
  name: "My dbt transformations",
  operatorConfiguration: {
    operatorType: OperatorType.Dbt,
    dbt: {
      dockerImage: "fishtownanalytics/dbt:0.19.1",
      dbtArguments: "run",
    },
  },
};

const connectionValidationSchema = yup
  .object({
    schedule: yup
      .object({
        units: yup.number().required("form.empty.error"),
        timeUnit: yup.string().required("form.empty.error"),
      })
      .nullable()
      .defined("form.empty.error"),
    namespaceDefinition: yup
      .string()
      .oneOf([
        ConnectionNamespaceDefinition.Source,
        ConnectionNamespaceDefinition.Destination,
        ConnectionNamespaceDefinition.CustomFormat,
      ])
      .required("form.empty.error"),
    namespaceFormat: yup.string().when("namespaceDefinition", {
      is: ConnectionNamespaceDefinition.CustomFormat,
      then: yup.string().required("form.empty.error"),
    }),
    prefix: yup.string(),
    syncCatalog: yup.object({
      streams: yup.array().of(
        yup.object({
          id: yup
            .string()
            // This is required to get rid of id fields we are using to detect stream for edition
            .when(
              "$isRequest",
              (isRequest: boolean, schema: yup.StringSchema) =>
                isRequest ? schema.strip(true) : schema
            ),
          stream: yup.object(),
          // @ts-ignore
          config: yup.object().test({
            name: "connectionSchema.config.validator",
            // eslint-disable-next-line no-template-curly-in-string
            message: "${path} is wrong",
            test: function (value: AirbyteStreamConfiguration) {
              if (!value.selected) {
                return true;
              }
              if (DestinationSyncMode.Dedupted === value.destinationSyncMode) {
                if (value.primaryKey.length === 0) {
                  return this.createError({
                    message: "connectionForm.primaryKey.required",
                    path: `schema.streams[${this.parent.id}].config.primaryKey`,
                  });
                }
              }

              if (SyncMode.Incremental === value.syncMode) {
                if (
                  !this.parent.stream.sourceDefinedCursor &&
                  value.cursorField.length === 0
                ) {
                  return this.createError({
                    message: "connectionForm.cursorField.required",
                    path: `schema.streams[${this.parent.id}].config.cursorField`,
                  });
                }
              }
              return true;
            },
          }),
        })
      ),
    }),
  })
  .noUnknown();

/**
 * Returns {@link Operation}[]
 *
 * Maps UI representation of Transformation and Normalization
 * into API's {@link Operation} representation.
 *
 * Always puts normalization as first operation
 * @param values
 * @param initialOperations
 */
function mapFormPropsToOperation(
  values: {
    transformations?: Transformation[];
    normalization?: NormalizationType;
  },
  initialOperations: Operation[] = []
): Operation[] {
  const newOperations: Operation[] = [];

  if (values.normalization) {
    if (values.normalization !== NormalizationType.RAW) {
      const normalizationOperation = initialOperations.find(
        (op) =>
          op.operatorConfiguration.operatorType === OperatorType.Normalization
      );

      if (normalizationOperation) {
        newOperations.push(normalizationOperation);
      } else {
        newOperations.push({
          name: "Normalization",
          operatorConfiguration: {
            operatorType: OperatorType.Normalization,
            normalization: {
              option: values.normalization,
            },
          },
        });
      }
    }
  }

  if (values.transformations) {
    newOperations.push(...values.transformations);
  }

  return newOperations;
}

function getDefaultCursorField(streamNode: SyncSchemaStream): string[] {
  if (streamNode.stream.defaultCursorField.length) {
    return streamNode.stream.defaultCursorField;
  }
  return streamNode.config.cursorField;
}

// If the value in supportedSyncModes is empty assume the only supported sync mode is FULL_REFRESH.
// Otherwise it supports whatever sync modes are present.
const useInitialSchema = (schema: SyncSchema): SyncSchema =>
  useMemo<SyncSchema>(
    () => ({
      streams: schema.streams.map<SyncSchemaStream>((apiNode, id) => {
        const streamNode: SyncSchemaStream = { ...apiNode, id: id.toString() };
        const node = !streamNode.stream.supportedSyncModes?.length
          ? {
              ...streamNode,
              stream: {
                ...streamNode.stream,
                supportedSyncModes: [SyncMode.FullRefresh],
              },
            }
          : streamNode;

        // If syncMode isn't null - don't change item
        if (node.config.syncMode) {
          return node;
        }

        const updateStream = (
          config: Partial<AirbyteStreamConfiguration>
        ): SyncSchemaStream => ({
          ...node,
          config: { ...node.config, ...config },
        });

        const supportedSyncModes = node.stream.supportedSyncModes;

        // If syncMode is null, FULL_REFRESH should be selected by default (if it support FULL_REFRESH).
        if (supportedSyncModes.includes(SyncMode.FullRefresh)) {
          return updateStream({
            syncMode: SyncMode.FullRefresh,
          });
        }

        // If source support INCREMENTAL and not FULL_REFRESH. Set INCREMENTAL
        if (supportedSyncModes.includes(SyncMode.Incremental)) {
          return updateStream({
            cursorField: streamNode.config.cursorField.length
              ? streamNode.config.cursorField
              : getDefaultCursorField(streamNode),
            syncMode: SyncMode.Incremental,
          });
        }

        // If source don't support INCREMENTAL and FULL_REFRESH - set first value from supportedSyncModes list
        return updateStream({
          syncMode: streamNode.stream.supportedSyncModes[0],
        });
      }),
    }),
    [schema.streams]
  );

const useInitialValues = (
  connection:
    | Connection
    | (Partial<Connection> &
        Pick<Connection, "syncCatalog" | "source" | "destination">),
  destDefinition: DestinationDefinitionSpecification,
  isEditMode?: boolean
) => {
  const initialSchema = useInitialSchema(connection.syncCatalog);

  return useMemo<FormikConnectionFormValues>(() => {
    const initialValues: FormikConnectionFormValues = {
      syncCatalog: initialSchema,
      schedule: connection.schedule,
      prefix: connection.prefix || "",
      namespaceDefinition:
        connection.namespaceDefinition ?? ConnectionNamespaceDefinition.Source,
      // eslint-disable-next-line no-template-curly-in-string
      namespaceFormat: connection.namespaceFormat ?? SOURCE_NAMESPACE_TAG,
    };

    const { operations = [] } = connection;

    if (destDefinition.supportsDbt) {
      initialValues.transformations =
        (operations.filter(
          (op) => op.operatorConfiguration.operatorType === OperatorType.Dbt
        ) as Transformation[]) ?? [];
    }

    if (destDefinition.supportsNormalization) {
      let initialNormalization = (operations.find(
        (op) =>
          op.operatorConfiguration.operatorType === OperatorType.Normalization
      ) as Normalization)?.operatorConfiguration?.normalization?.option;

      // If no normalization was selected for already present normalization -> Raw is select
      if (!initialNormalization && isEditMode) {
        initialNormalization = NormalizationType.RAW;
      }

      initialValues.normalization =
        initialNormalization ?? NormalizationType.BASIC;
    }

    return initialValues;
  }, [initialSchema, connection, isEditMode, destDefinition]);
};
const useFrequencyDropdownData = (): DropDownRow.IDataItem[] => {
  const formatMessage = useIntl().formatMessage;

  return useMemo(
    () =>
      FrequencyConfig.map((item) => ({
        value: item.config,
        label:
          item.config === null
            ? item.text
            : formatMessage(
                {
                  id: "form.every",
                },
                {
                  value: item.simpleText || item.text,
                }
              ),
      })),
    []
  );
};

export type { ConnectionFormValues, FormikConnectionFormValues };
export {
  connectionValidationSchema,
  useInitialValues,
  useFrequencyDropdownData,
  mapFormPropsToOperation,
  SUPPORTED_MODES,
  DEFAULT_TRANSFORMATION,
};
