# Best Practices

In order to guarantee the highest quality for connectors, we've compiled the following best practices for connector development. Connectors which follow these best practices will be labelled as "Airbyte Certified" to indicate they've passed a high quality bar and will perform reliably in all production use cases. Following these guidelines is **not required** for your contribution to Airbyte to be accepted, as they add a barrier to entry for contribution \(though adopting them certainly doesn't hurt!\).

## Principles of developing connectors

1. **Reliability + usability &gt; more features.** It is better to support 1 feature that works reliably and has a great UX than 2 that are unreliable or hard to use. One solid connector is better than 2 finicky ones.
2. **Fail fast.** A user should not be able to configure something that will not work. 
3. **Fail actionably.** If a failure is actionable by the user, clearly let them know what they can do. Otherwise, make it very easy for them to give us necessary debugging information \(logs etc.\)

From these principles we extrapolate the following goals for connectors, in descending priority order:

1. **Correct user input should result in a successful sync.** If there is an issue, it should be extremely easy for the user to see and report. 
2. **Issues arising from bad user input should print an actionable error message.** "Invalid credentials" is not an actionable message. "Please verify your username/password is correct" is better. 
3. **Wherever possible, a connector should support incremental sync.** This prevents excessive load on the underlying data source.  _\*\*_
4. **When running a sync, a connector should communicate its status frequently to provide clear feedback that it is working.** Output a log message at least every 5 minutes. 
5. **A connector should allow reading or writing as many entities as is feasible.** Supporting syncing all entities from an API is preferred to only supporting a small subset which would satisfy narrow use cases. Similarly, a database should support as many data types as is feasible. 

Note that in the above list, the _least_ important is the number of features it has \(e.g: whether an API connector supports all entities in the API\). The most important thing is that for its declared features, it is reliable and usable. The only exception are “minimum viability” features e.g: for some sources, it’s not feasible to pull data without incremental due to rate limiting issues. In this case, those are considered usability issues.

## Quality certification checklist

When reviewing connectors, we'll use the following "checklist" to verify whether the connector is considered "Airbyte certified" or closer to beta or alpha:

### Integration Testing

**As much as possible, prove functionality via testing**. This means slightly different things depending on the type of connector:

* **All connectors** must test all the sync modes they support during integration tests
* **Database connectors** should test that they can replicate **all** supported data types in both `read` and `discover` operations
* **API connectors** should validate records that every stream outputs data
  * If this causes rate limiting problems, there should be a periodic CI build which tests this on a less frequent cadence to avoid rate limiting

**Thoroughly test edge cases.** While Airbyte provides a [Standard Test Suite](testing-connectors.md#airbytes-standard-tests) that all connectors must pass, it's not possible for the standard test suite to cover all edge cases. When in doubt about whether the standard tests provide sufficient evidence of functionality, write a custom test case for your connector.

### Check Connection

* **Verify permissions upfront**. The "check connection" operation should verify any necessary permissions upfront e.g: the provided API token has read access to the API entities. 
  * In some cases it's not possible to verify permissions without knowing which streams the user wants to replicate. For example, a provided API token only needs read access to the "Employees" entity if the user wants to replicate the "Employees" stream. In this case, the CheckConnection operation should verify the minimum needed requirements \(e.g: the API token exists\), and the "read" or "write" operation should verify all needed permissions based on the provided catalog, failing if a required permission is not granted.
* **Provide actionable feedback for incorrect input.** 
  * Examples of non actionable error messages
    * "Can't connect". The only recourse this gives the user is to guess whether they need to dig through logs or guess which field of their input configuration is incorrect. 
  * Examples of actionable error messages
    * "Your username/password combination is incorrect"
    * "Unable to reach Database host: please verify that there are no firewall rules preventing Airbyte from connecting to the database"
    * etc...

### Rate Limiting

Most APIs enforce rate limits. Your connector should gracefully handle those \(i.e: without failing the connector process\). The most common way to handle rate limits is to implement backoff.

### Spec.json
The spec should attempt to give a user as much guidance as possible when configuring a connector. It should take reasonable steps to reduce errors due to bad user input. Things to consider:
1. Add a useful description for any field that is not super obvious.
2. Add a default value for any field wherever possible.
3. Add a regex to constrain what a valid input looks like for each field.
4. Use an enum where possible. For any text field, where the valid values can be enumerated, even if the list is somewhat long (e.g. currency types), use an enum, so that the user is forced to put in valid inputs.
5. Specify the order of the fields so that the user has to input data in a sequence that is intuitive.
