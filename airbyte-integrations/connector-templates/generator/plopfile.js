"use strict";
const path = require("path");
const uuid = require("uuid");

const getSuccessMessage = function (
  connectorName,
  outputPath,
  additionalMessage
) {
  return `
🚀 🚀 🚀 🚀 🚀 🚀

Success! 

Your ${connectorName} connector has been created at ${path.resolve(outputPath)}.

Follow the TODOs in the generated module to implement your connector. 

Questions, comments, or concerns? Let us know at:
Slack: https://slack.airbyte.io
Github: https://github.com/airbytehq/airbyte

We're always happy to provide any support!

${additionalMessage || ""}
`;
};

module.exports = function (plop) {
  const docRoot = "../../../docs";
  const definitionRoot = "../../../airbyte-config/init/src/main/resources";

  const baseSourceInputRoot = "../source-base";
  const baseDestinationInputRoot = "../destination-base";

  const pythonSourceInputRoot = "../source-python";
  const singerSourceInputRoot = "../source-singer";
  const genericSourceInputRoot = "../source-generic";
  const httpApiInputRoot = "../source-python-http-api";
  const javaDestinationInput = "../destination-java";

  const outputDir = "../../connectors";
  const pythonSourceOutputRoot = `${outputDir}/source-{{dashCase name}}`;
  const singerSourceOutputRoot = `${outputDir}/source-{{dashCase name}}-singer`;
  const genericSourceOutputRoot = `${outputDir}/source-{{dashCase name}}`;
  const httpApiOutputRoot = `${outputDir}/source-{{dashCase name}}`;
  const javaDestinationOutputRoot = `${outputDir}/destination-{{dashCase name}}`;

  plop.setActionType("emitSuccess", function (answers, config, plopApi) {
    console.log(
      getSuccessMessage(
        answers.name,
        plopApi.renderString(config.outputPath, answers),
        config.message
      )
    );
  });

  plop.setGenerator("Python HTTP API Source", {
    description:
      "Generate a Source that pulls data from a synchronous HTTP API.",
    prompts: [
      {
        type: "input",
        name: "name",
        message: 'Source name e.g: "google-analytics"',
      },
      {
        type: "input",
        name: "uuid",
        default: () => {
          return uuid.v4();
        },
        message:
          "Connector UUID (v4). Press enter to use the auto generated one.",
      },
    ],
    actions: [
      {
        abortOnFail: true,
        type: "addMany",
        destination: httpApiOutputRoot,
        base: httpApiInputRoot,
        templateFiles: `${httpApiInputRoot}/**/**`,
      },
      // plop doesn't add dotfiles by default so we manually add them
      {
        type: "add",
        abortOnFail: true,
        templateFile: `${httpApiInputRoot}/.dockerignore.hbs`,
        path: `${httpApiOutputRoot}/.dockerignore`,
      },
      {
        type: "emitSuccess",
        outputPath: httpApiOutputRoot,
      },
    ],
  });

  plop.setGenerator("Python Singer Source", {
    description: "Generate a Singer-tap-based Airbyte Source.",
    prompts: [
      {
        type: "input",
        name: "name",
        message:
          'Source name, without the "source-" prefix e.g: "google-analytics"',
        filter: function (name) {
          return name.endsWith("-singer") ? name.replace(/-singer$/, "") : name;
        },
      },
      {
        type: "input",
        name: "tap_name",
        message: 'Singer tap package e.g "tap-mixpanel"',
      },
      {
        type: "input",
        name: "uuid",
        default: () => {
          return uuid.v4();
        },
        message:
          "Connector UUID (v4). Press enter to use the auto generated one.",
      },
    ],
    actions: [
      {
        abortOnFail: true,
        type: "addMany",
        destination: singerSourceOutputRoot,
        base: singerSourceInputRoot,
        templateFiles: `${singerSourceInputRoot}/**/**`,
      },
      {
        type: "add",
        abortOnFail: true,
        templateFile: `${singerSourceInputRoot}/.gitignore.hbs`,
        path: `${singerSourceOutputRoot}/.gitignore`,
      },
      {
        type: "add",
        abortOnFail: true,
        templateFile: `${singerSourceInputRoot}/.dockerignore.hbs`,
        path: `${singerSourceOutputRoot}/.dockerignore`,
      },
      { type: "emitSuccess", outputPath: singerSourceOutputRoot },
    ],
  });

  plop.setGenerator("Python Source", {
    description:
      "Generate a minimal Python Airbyte Source Connector that works with any kind of data source. Use this if none of the other Python templates serve your use case.",
    prompts: [
      {
        type: "input",
        name: "name",
        message:
          'Source name, without the "source-" prefix e.g: "google-analytics"',
      },
      {
        type: "input",
        name: "uuid",
        default: () => {
          return uuid.v4();
        },
        message:
          "Connector UUID (v4). Press enter to use the auto generated one.",
      },
    ],
    actions: [
      {
        abortOnFail: true,
        type: "addMany",
        destination: pythonSourceOutputRoot,
        base: pythonSourceInputRoot,
        templateFiles: `${pythonSourceInputRoot}/**/**`,
      },
      {
        type: "add",
        abortOnFail: true,
        templateFile: `${pythonSourceInputRoot}/.gitignore.hbs`,
        path: `${pythonSourceOutputRoot}/.gitignore`,
      },
      {
        type: "add",
        abortOnFail: true,
        templateFile: `${pythonSourceInputRoot}/.dockerignore.hbs`,
        path: `${pythonSourceOutputRoot}/.dockerignore`,
      },
      {
        type: "emitSuccess",
        outputPath: pythonSourceOutputRoot,
        message:
          "For a checklist of what to do next go to https://docs.airbyte.io/tutorials/building-a-python-source",
      },
    ],
  });

  plop.setGenerator("Generic Source", {
    description: "Use if none of the other templates apply to your use case.",
    prompts: [
      {
        type: "input",
        name: "name",
        message:
          'Source name, without the "source-" prefix e.g: "google-analytics"',
      },
      {
        type: "input",
        name: "uuid",
        default: () => {
          return uuid.v4();
        },
        message:
          "Connector UUID (v4). Press enter to use the auto generated one.",
      },
    ],
    actions: [
      {
        abortOnFail: true,
        type: "addMany",
        destination: genericSourceOutputRoot,
        base: genericSourceInputRoot,
        templateFiles: `${genericSourceInputRoot}/**/**`,
      },
      {
        type: "add",
        abortOnFail: true,
        templateFile: `${genericSourceInputRoot}/.gitignore.hbs`,
        path: `${genericSourceOutputRoot}/.gitignore`,
      },
      { type: "emitSuccess", outputPath: genericSourceOutputRoot },
    ],
  });

  plop.setGenerator("Java Destination", {
    description:
      "Generate a minimal Java Airbyte Destination Connector that works with any kind of data source. Use this if none of the other templates serve your use case.",
    prompts: [
      {
        type: "input",
        name: "name",
        message:
          'Destination name, without the "destination-" prefix e.g: "google-analytics"',
      },
      {
        type: "input",
        name: "uuid",
        default: () => {
          return uuid.v4();
        },
        message:
          "Connector UUID (v4). Press enter to use the auto generated one.",
      },
    ],
    actions: [
      {
        abortOnFail: true,
        type: "addMany",
        base: javaDestinationInput,
        templateFiles: `${javaDestinationInput}/**/**`,
        destination: javaDestinationOutputRoot,
      },
      // Plop doesn't add dotfiles by default so we manually add them
      {
        type: "add",
        abortOnFail: true,
        templateFile: `${javaDestinationInput}/.dockerignore.hbs`,
        path: `${javaDestinationOutputRoot}/.dockerignore`,
      },
      // Doc
      {
        type: "add",
        abortOnFail: true,
        templateFile: `${baseDestinationInputRoot}/doc-file.md.hbs`,
        path: `${docRoot}/integrations/destinations/{{dashCase name}}.md`,
      },
      {
        type: "append",
        abortOnFail: true,
        templateFile: `${baseDestinationInputRoot}/doc-link.md.hbs`,
        path: `${docRoot}/SUMMARY.md`,
        pattern: "  * [Destinations](integrations/destinations/README.md)",
      },
      // Definition
      {
        type: "add",
        abortOnFail: true,
        templateFile: `${baseDestinationInputRoot}/definition-config.json.hbs`,
        path: `${definitionRoot}/config/STANDARD_DESTINATION_DEFINITION/{{uuid}}.json`,
      },
      {
        type: "append",
        abortOnFail: true,
        templateFile: `${baseDestinationInputRoot}/definition-seed.yaml.hbs`,
        path: `${definitionRoot}/seed/destination_definitions.yaml`,
        pattern: "# DESTINATION DEFINITION BY CODE GENERATOR",
      },
      {
        type: "emitSuccess",
        outputPath: javaDestinationOutputRoot,
      },
    ],
  });
};
