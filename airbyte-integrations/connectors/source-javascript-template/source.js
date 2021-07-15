const fs = require('fs');
const axios = require("axios");
const path = require('path');
const {ArgumentParser} = require('argparse');

// todo - not the right way to do this.
const dateFns = require('date-fns');
const parse = dateFns.parse;
const getMilliseconds = dateFns.getMilliseconds;
const isAfter = dateFns.isAfter;
// const parse = require('date-fns/parse');
// const getMilliseconds = require('date-fns/get_milliseconds')
// const isAfter = require('date-fns/is_after')

async function read(config, catalog) {
    if (!config['api_key']) {
        log("Input config must contain the properties 'api_key' and 'stock_ticker'");
        process.exit(1);
    }

    let stockPricesStream = null;
    for (const configuredStreamIndex in catalog["streams"]) {
        const configuredStream = catalog["streams"][configuredStreamIndex];
        if (configuredStream["stream"]["name"] === "stock_prices") {
            stockPricesStream = configuredStream;
        }
    }

    if (stockPricesStream === null) {
        log("No streams selected");
        return;
    }
    // We only support full_refresh at the moment, so verify the user didn't ask for another sync mode
    if (stockPricesStream["sync_mode"] !== "full_refresh") {
        log("This connector only supports full refresh syncs! (for now)");
        process.exit(1);
    }

    // If we've made it this far, all the configuration is good and we can pull the last 7 days of market data
    const apiKey = config["api_key"];
    const stockTicker = config["stock_ticker"];
    const now = new Date();
    const response = await _callApi(`/stock/${stockTicker}/chart/7d`, apiKey);

    if (response.status !== 200) {
        log("Failure occurred when calling IEX API");
        process.exit(1);
    } else {
        const prices = response.data
            .sort((record1, record2) => {
                const dateFormat = "yyyy-MM-dd";
                const record1Date = parse(record1["data"], dateFormat, new Date());
                const record2Date = parse(record2["data"], dateFormat, new Date());
                return isAfter(record1Date, record2Date)
            })
            .map(price => {
                const data = {
                    "date": price["date"],
                    "stock_ticker": price["symbol"],
                    "price": price["close"]
                };
                const record = {
                    "stream": "stock_prices",
                    "data": data,
                    "emitted_at": getMilliseconds(now)
                };
                return {"type": "RECORD", "record": record};
            })
            .forEach(record => console.log(JSON.stringify(record)));
    }
}

function readJson(filePath) {
    return JSON.parse(fs.readFileSync(filePath));
}

async function _callApi(endpoint, token) {
    return axios.get("https://cloud.iexapis.com/v1/" + endpoint + "?token=" + token)
        .then(function (response) {
            return response;
        })
        .catch(function (response) {
            return response;
        })
}

async function check(config) {
    if (!config['api_key']) {
        log("Input config must contain the properties 'api_key' and 'stock_ticker'");
        process.exit(1);
    }
    // Validate input configuration by attempting to get the price of the input stock ticker for the previous day
    const response = await _callApi("stock/" + config["stock_ticker"] + "/previous", config["api_key"]);
    let result;
    if (response.status === 200) {
        result = {"status": "SUCCEEDED"};
    } else if (response.status_code === 403) {
        // HTTP code 403 means authorization failed so the API key is incorrect
        result = {"status": "FAILED", "message": "API Key is incorrect."};
    } else {
        // Consider any other code a "generic" failure and tell the user to make sure their config is correct.
        result = {
            "status": "FAILED",
            "message": "Input configuration is incorrect. Please verify the input stock ticker and API key."
        };
    }

    // Format the result of the check operation according to the Airbyte Specification
    const outputMessage = {"type": "CONNECTION_STATUS", "connectionStatus": result}
    console.log(JSON.stringify(outputMessage));

}

function log(message) {
    const logJson = {"type": "LOG", "log": message}
    console.log(logJson)
}

function discover() {
    const catalog = {
        "streams": [{
            "name": "stock_prices",
            "supported_sync_modes": ["full_refresh"],
            "json_schema": {
                "properties": {
                    "date": {
                        "type": "string"
                    },
                    "price": {
                        "type": "number"
                    },
                    "stock_ticker": {
                        "type": "string"
                    }
                }
            }
        }]
    };
    const airbyte_message = {"type": "CATALOG", "catalog": catalog};
    console.log(JSON.stringify(airbyte_message));
}

function getInputFilePath(path1) {
    if (path.isAbsolute(path1)) {
        return path1;
    } else {
        return path.join(process.cwd(), path1);
    }
}

function spec() {
    // Read the file named spec.json from the module directory as a JSON file
    const specPath = path.join(path.dirname(__filename), "spec.json");
    const specification = readJson(specPath);

    // form an Airbyte Message containing the spec and print it to stdout
    const airbyteMessage = {"type": "SPEC", "spec": specification};

    console.log(JSON.stringify(airbyteMessage));
}

async function run(args) {
    const parentParser = new ArgumentParser({add_help: false});
    const mainParser = new ArgumentParser({add_help: false});
    const subparsers = mainParser.add_subparsers({title: "commands", dest: "command"});

    // Accept the spec command
    subparsers.add_parser("spec", {
        help: "outputs the json configuration specification",
        parents: [parentParser]
    });

    // Accept the check command
    const checkParser = subparsers.add_parser("check", {
        help: "checks the config used to connect",
        parents: [parentParser]
    });
    const requiredCheckParser = checkParser.add_argument_group("required named arguments")
    requiredCheckParser.add_argument("--config", {
        type: "str",
        required: true,
        help: "path to the json configuration file"
    });

    // Accept the discover command
    const discover_parser = subparsers.add_parser("discover", {
        help: "outputs a catalog describing the source's schema",
        parents: [parentParser]
    });
    const requiredDiscoverParser = discover_parser.add_argument_group("required named arguments");
    requiredDiscoverParser.add_argument("--config", {
        type: "str",
        required: true,
        help: "path to the json configuration file"
    });

    // Accept the read command
    const readParser = subparsers.add_parser("read", {
        help: "reads the source and outputs messages to STDOUT",
        parents: [parentParser]
    });
    readParser.add_argument("--state", {
        type: "str",
        required: false,
        help: "path to the json-encoded state file"
    });
    const requiredReadParser = readParser.add_argument_group("required named arguments");
    requiredReadParser.add_argument("--config", {
        type: "str",
        required: true,
        help: "path to the json configuration file"
    });
    requiredReadParser.add_argument("--catalog", {
        type: "str",
        required: true,
        help: "path to the catalog used to determine which data to read"
    });

    const parsedArgs = mainParser.parse_args(args);
    const command = parsedArgs.command;

    if (command === "spec") {
        spec();
    } else if (command === "check") {
        const config = readJson(getInputFilePath(parsedArgs.config))
        await check(config);
    } else if (command === "discover") {
        discover();
    } else if (command === "read") {
        const config = readJson(getInputFilePath(parsedArgs.config))
        const configuredCatalog = readJson(getInputFilePath(parsedArgs.catalog))
        await read(config, configuredCatalog);
    } else {
        // If we don't recognize the command log the problem and exit with an error code greater than 0 to indicate the process
        // had a failure
        log("Invalid command. Allowable commands: [spec, check, discover, read]")
        process.exit(1)
    }

    // A zero exit code means the process successfully completed
    process.exit(0)
}

(async function () {
    await run(process.argv.slice(2)).catch(reason => console.log(reason));
}());
