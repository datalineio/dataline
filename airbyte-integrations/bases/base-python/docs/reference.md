<a name="client"></a>
# client

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="client.package_name_from_class"></a>
#### package\_name\_from\_class

```python
package_name_from_class(cls: object) -> str
```

Find the package name given a class name

<a name="client.StreamStateMixin"></a>
## StreamStateMixin Objects

```python
class StreamStateMixin()
```

<a name="client.StreamStateMixin.get_stream_state"></a>
#### get\_stream\_state

```python
 | get_stream_state(name: str) -> Any
```

Get state of stream with corresponding name

<a name="client.StreamStateMixin.set_stream_state"></a>
#### set\_stream\_state

```python
 | set_stream_state(name: str, state: Any)
```

Set state of stream with corresponding name

<a name="client.StreamStateMixin.stream_has_state"></a>
#### stream\_has\_state

```python
 | stream_has_state(name: str) -> bool
```

Tell if stream supports incremental sync

<a name="client.BaseClient"></a>
## BaseClient Objects

```python
class BaseClient(StreamStateMixin,  ABC)
```

Base client for API

<a name="client.BaseClient.read_stream"></a>
#### read\_stream

```python
 | read_stream(stream: AirbyteStream) -> Generator[Dict[str, Any], None, None]
```

Yield records from stream

<a name="client.BaseClient.streams"></a>
#### streams

```python
 | @property
 | streams() -> Generator[AirbyteStream, None, None]
```

List of available streams

<a name="client.BaseClient.health_check"></a>
#### health\_check

```python
 | @abstractmethod
 | health_check() -> Tuple[bool, str]
```

Check if service is up and running

<a name="client.configured_catalog_from_client"></a>
#### configured\_catalog\_from\_client

```python
configured_catalog_from_client(client: BaseClient) -> ConfiguredAirbyteCatalog
```

Helper to generate configured catalog for testing

<a name="__init__"></a>
# \_\_init\_\_

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="entrypoint"></a>
# entrypoint

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="logger"></a>
# logger

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="integration"></a>
# integration

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="integration.Integration"></a>
## Integration Objects

```python
class Integration(object)
```

<a name="integration.Integration.configure"></a>
#### configure

```python
 | configure(config: json, temp_dir: str) -> json
```

Persist config in temporary directory to run the Source job

<a name="integration.Integration.spec"></a>
#### spec

```python
 | spec(logger: AirbyteLogger) -> ConnectorSpecification
```

Returns the spec for this integration. The spec is a JSON-Schema object describing the required configurations (e.g: username and password)
required to run this integration.

<a name="integration.Integration.check"></a>
#### check

```python
 | check(logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus
```

Tests if the input configuration can be used to successfully connect to the integration e.g: if a provided Stripe API token can be used to connect
to the Stripe API.

<a name="integration.Integration.discover"></a>
#### discover

```python
 | discover(logger: AirbyteLogger, config: json) -> AirbyteCatalog
```

Returns an AirbyteCatalog representing the available streams and fields in this integration. For example, given valid credentials to a
Postgres database, returns an Airbyte catalog where each postgres table is a stream, and each table column is a field.

<a name="integration.Source"></a>
## Source Objects

```python
class Source(Integration)
```

<a name="integration.Source.read"></a>
#### read

```python
 | read(logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state_path: Dict[str, any]) -> Generator[AirbyteMessage, None, None]
```

Returns a generator of the AirbyteMessages generated by reading the source with the given configuration, catalog, and state.

<a name="catalog_helpers"></a>
# catalog\_helpers

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="catalog_helpers.CatalogHelper"></a>
## CatalogHelper Objects

```python
class CatalogHelper()
```

<a name="catalog_helpers.CatalogHelper.coerce_catalog_as_full_refresh"></a>
#### coerce\_catalog\_as\_full\_refresh

```python
 | @staticmethod
 | coerce_catalog_as_full_refresh(catalog: AirbyteCatalog) -> AirbyteCatalog
```

Updates the sync mode on all streams in this catalog to be full refresh

<a name="schema_helpers"></a>
# schema\_helpers

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="schema_helpers.JsonSchemaResolver"></a>
## JsonSchemaResolver Objects

```python
class JsonSchemaResolver()
```

Helper class to expand $ref items in json schema

<a name="schema_helpers.JsonSchemaResolver.resolve"></a>
#### resolve

```python
 | resolve(schema: dict, refs: Dict[str, dict] = None) -> dict
```

Resolves and replaces json-schema $refs with the appropriate dict.
Recursively walks the given schema dict, converting every instance
of $ref in a 'properties' structure with a resolved dict.
This modifies the input schema and also returns it.

**Arguments**:

  schema:
  the schema dict
  refs:
  a dict of <string, dict> which forms a store of referenced schemata

**Returns**:

  schema

<a name="schema_helpers.ResourceSchemaLoader"></a>
## ResourceSchemaLoader Objects

```python
class ResourceSchemaLoader()
```

JSONSchema loader from package resources

<a name="schema_helpers.ResourceSchemaLoader.get_schema"></a>
#### get\_schema

```python
 | get_schema(name: str) -> dict
```

This method retrieves a JSON schema from the schemas/ folder.


The expected file structure is to have all top-level schemas (corresponding to streams) in the "schemas/" folder, with any shared $refs
living inside the "schemas/shared/" folder. For example:

schemas/shared/<shared_definition>.json
schemas/<name>.json # contains a $ref to shared_definition
schemas/<name2>.json # contains a $ref to shared_definition

<a name="source"></a>
# source

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="source.BaseSource"></a>
## BaseSource Objects

```python
class BaseSource(Source)
```

Base source that designed to work with clients derived from BaseClient

<a name="source.BaseSource.name"></a>
#### name

```python
 | @property
 | name() -> str
```

Source name

<a name="source.BaseSource.discover"></a>
#### discover

```python
 | discover(logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteCatalog
```

Discover streams

<a name="source.BaseSource.check"></a>
#### check

```python
 | check(logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus
```

Check connection

<a name="cdk"></a>
# cdk

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="cdk.streams"></a>
# cdk.streams

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="cdk.streams.rate_limiting"></a>
# cdk.streams.rate\_limiting

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="cdk.streams.auth"></a>
# cdk.streams.auth

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="cdk.streams.auth.token"></a>
# cdk.streams.auth.token

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="cdk.streams.auth.core"></a>
# cdk.streams.auth.core

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="cdk.streams.auth.core.HttpAuthenticator"></a>
## HttpAuthenticator Objects

```python
class HttpAuthenticator(ABC)
```

Base abstract class for various HTTP Authentication strategies. Authentication strategies are generally
expected to provide security credentials via HTTP headers.

<a name="cdk.streams.auth.core.HttpAuthenticator.get_auth_header"></a>
#### get\_auth\_header

```python
 | @abstractmethod
 | get_auth_header() -> Mapping[str, Any]
```

**Returns**:

A dictionary containing all the necessary headers to authenticate.

<a name="cdk.streams.auth.oauth"></a>
# cdk.streams.auth.oauth

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="cdk.streams.auth.oauth.Oauth2Authenticator"></a>
## Oauth2Authenticator Objects

```python
class Oauth2Authenticator(HttpAuthenticator)
```

Generates OAuth2.0 access tokens from an OAuth2.0 refresh token and client credentials.
The generated access token is attached to each request via the Authorization header.

<a name="cdk.streams.auth.oauth.Oauth2Authenticator.get_refresh_request_body"></a>
#### get\_refresh\_request\_body

```python
 | get_refresh_request_body() -> Mapping[str, any]
```

Override to define additional parameters

<a name="cdk.streams.auth.oauth.Oauth2Authenticator.refresh_access_token"></a>
#### refresh\_access\_token

```python
 | refresh_access_token() -> Tuple[str, int]
```

returns a tuple of (access_token, token_lifespan_in_seconds)

<a name="cdk.streams.auth.jwt"></a>
# cdk.streams.auth.jwt

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="cdk.streams.core"></a>
# cdk.streams.core

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="cdk.streams.core.package_name_from_class"></a>
#### package\_name\_from\_class

```python
package_name_from_class(cls: object) -> str
```

Find the package name given a class name

<a name="cdk.streams.core.Stream"></a>
## Stream Objects

```python
class Stream(ABC)
```

Base abstract class for an Airbyte Stream. Makes no assumption of the Stream's underlying transport protocol.

<a name="cdk.streams.core.Stream.name"></a>
#### name

```python
 | @property
 | name() -> str
```

**Returns**:

Stream name. By default this is the implementing class name, but it can be overridden as needed.

<a name="cdk.streams.core.Stream.read_records"></a>
#### read\_records

```python
 | @abstractmethod
 | read_records(sync_mode: SyncMode, cursor_field: List[str] = None, stream_slice: Mapping[str, any] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Mapping[str, Any]]
```

This method should be overridden by subclasses to read records based on the inputs

<a name="cdk.streams.core.Stream.get_json_schema"></a>
#### get\_json\_schema

```python
 | get_json_schema() -> Mapping[str, Any]
```

**Returns**:

A dict of the JSON schema representing this stream.

The default implementation of this method looks for a JSONSchema file with the same name as this stream's "name" property.
Override as needed.

<a name="cdk.streams.core.Stream.supports_incremental"></a>
#### supports\_incremental

```python
 | @property
 | supports_incremental() -> bool
```

**Returns**:

True if this stream supports incrementally reading data

<a name="cdk.streams.core.Stream.cursor_field"></a>
#### cursor\_field

```python
 | @property
 | cursor_field() -> Union[str, List[str]]
```

Override to return the default cursor field used by this stream e.g: an API entity might always use created_at as the cursor field.

**Returns**:

The name of the field used as a cursor. If the cursor is nested, return an array consisting of the path to the cursor.

<a name="cdk.streams.core.Stream.source_defined_cursor"></a>
#### source\_defined\_cursor

```python
 | @property
 | source_defined_cursor() -> bool
```

Return False if the cursor can be configured by the user.

<a name="cdk.streams.core.Stream.stream_slices"></a>
#### stream\_slices

```python
 | stream_slices(sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, any]]]
```

Override to define the slices for this stream. See the stream slicing section of the docs for more information.

**Arguments**:

- `stream_state`: 

**Returns**:



<a name="cdk.streams.core.Stream.state_checkpoint_interval"></a>
#### state\_checkpoint\_interval

```python
 | @property
 | state_checkpoint_interval() -> Optional[int]
```

Decides how often to checkpoint state (i.e: emit a STATE message). E.g: if this returns a value of 100, then state is persisted after reading
100 records, then 200, 300, etc.. A good default value is 1000 although your mileage may vary depending on the underlying data source.

Checkpointing a stream avoids re-reading records in the case a sync is failed or cancelled.

return None if state should not be checkpointed e.g: because records returned from the underlying data source are not returned in
ascending order with respect to the cursor field. This can happen if the source does not support reading records in ascending order of
created_at date (or whatever the cursor is). In those cases, state must only be saved once the full stream has been read.

<a name="cdk.streams.core.Stream.get_updated_state"></a>
#### get\_updated\_state

```python
 | get_updated_state(current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any])
```

Override to extract state from the latest record. Needed to implement incremental sync.

Inspects the latest record extracted from the data source and the current state object and return an updated state object.

For example: if the state object is based on created_at timestamp, and the current state is {'created_at': 10}, and the latest_record is
{'name': 'octavia', 'created_at': 20 } then this method would return {'created_at': 20} to indicate state should be updated to this object.

**Arguments**:

- `current_stream_state`: The stream's current state object
- `latest_record`: The latest record extracted from the stream

**Returns**:

An updated state object

<a name="cdk.streams.http"></a>
# cdk.streams.http

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="cdk.streams.http.HttpStream"></a>
## HttpStream Objects

```python
class HttpStream(Stream,  ABC)
```

Base abstract class for an Airbyte Stream using the HTTP protocol. Basic building block for users building an Airbyte source for a HTTP API.

<a name="cdk.streams.http.HttpStream.url_base"></a>
#### url\_base

```python
 | @property
 | @abstractmethod
 | url_base() -> str
```

**Returns**:

URL base for the  API endpoint e.g: if you wanted to hit https://myapi.com/v1/some_entity then this should return "https://myapi.com/v1/"

<a name="cdk.streams.http.HttpStream.http_method"></a>
#### http\_method

```python
 | @property
 | http_method() -> str
```

Override if needed. See get_request_data if using POST.

<a name="cdk.streams.http.HttpStream.next_page_token"></a>
#### next\_page\_token

```python
 | @abstractmethod
 | next_page_token(response: requests.Response) -> Optional[Mapping[str, Any]]
```

Override this method to define a pagination strategy.

The value returned from this method is passed to most other methods in this class. Use it to form a request e.g: set headers or query params.

**Returns**:

The token for the next page from the input response object. Returning None means there are no more pages to read in this response.

<a name="cdk.streams.http.HttpStream.path"></a>
#### path

```python
 | @abstractmethod
 | path(stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str
```

Returns the URL path for the API endpoint e.g: if you wanted to hit https://myapi.com/v1/some_entity then this should return "some_entity"

<a name="cdk.streams.http.HttpStream.request_params"></a>
#### request\_params

```python
 | request_params(stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]
```

Override this method to define the query parameters that should be set on an outgoing HTTP request given the inputs.

E.g: you might want to define query parameters for paging if next_page_token is not None.

<a name="cdk.streams.http.HttpStream.request_headers"></a>
#### request\_headers

```python
 | request_headers(stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Mapping[str, Any]
```

Override to return any non-auth headers. Authentication headers will overwrite any overlapping headers returned from this method.

<a name="cdk.streams.http.HttpStream.request_body_json"></a>
#### request\_body\_json

```python
 | request_body_json(stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Optional[Mapping]
```

TODO make this possible to do for non-JSON APIs
Override when creating POST requests to populate the body of the request with a JSON payload.

<a name="cdk.streams.http.HttpStream.parse_response"></a>
#### parse\_response

```python
 | @abstractmethod
 | parse_response(response: requests.Response, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Iterable[Mapping]
```

Parses the raw response object into a list of records.
By default, this returns an iterable containing the input. Override to parse differently.

**Arguments**:

- `response`: 

**Returns**:

An iterable containing the parsed response

<a name="cdk.streams.http.HttpStream.should_retry"></a>
#### should\_retry

```python
 | should_retry(response: requests.Response) -> bool
```

Override to set different conditions for backoff based on the response from the server.

By default, back off on the following HTTP response statuses:
 - 429 (Too Many Requests) indicating rate limiting
 - 500s to handle transient server errors

Unexpected but transient exceptions (connection timeout, DNS resolution failed, etc..) are retried by default.

<a name="cdk.streams.http.HttpStream.backoff_time"></a>
#### backoff\_time

```python
 | backoff_time(response: requests.Response) -> Optional[float]
```

Override this method to dynamically determine backoff time e.g: by reading the X-Retry-After header.

This method is called only if should_backoff() returns True for the input request.

:return how long to backoff in seconds. The return value may be a floating point number for subsecond precision. Returning None defers backoff
to the default backoff behavior (e.g using an exponential algorithm).

<a name="cdk.streams.exceptions"></a>
# cdk.streams.exceptions

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="cdk.streams.exceptions.UserDefinedBackoffException"></a>
## UserDefinedBackoffException Objects

```python
class UserDefinedBackoffException(BaseBackoffException)
```

An exception that exposes how long it attempted to backoff

<a name="cdk.streams.exceptions.UserDefinedBackoffException.__init__"></a>
#### \_\_init\_\_

```python
 | __init__(backoff: Union[int, float], request: requests.PreparedRequest, response: requests.Response)
```

**Arguments**:

- `backoff`: how long to backoff in seconds
- `request`: the request that triggered this backoff exception
- `response`: the response that triggered the backoff exception

<a name="cdk.utils"></a>
# cdk.utils

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="cdk.utils.casing"></a>
# cdk.utils.casing

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="cdk.abstract_source"></a>
# cdk.abstract\_source

MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<a name="cdk.abstract_source.AbstractSource"></a>
## AbstractSource Objects

```python
class AbstractSource(Source,  ABC)
```

Abstract base class for an Airbyte Source. Consumers should implement any abstract methods
in this class to create an Airbyte Specification compliant Source.

<a name="cdk.abstract_source.AbstractSource.check_connection"></a>
#### check\_connection

```python
 | @abstractmethod
 | check_connection(logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]
```

**Arguments**:

- `config`: The user-provided configuration as specified by the source's spec. This usually contains information required to check connection e.g. tokens, secrets and keys etc.

**Returns**:

A tuple of (boolean, error). If boolean is true, then the connection check is successful and we can connect to the underlying data
source using the provided configuration.
Otherwise, the input config cannot be used to connect to the underlying data source, and the "error" object should describe what went wrong.
The error object will be cast to string to display the problem to the user.

<a name="cdk.abstract_source.AbstractSource.streams"></a>
#### streams

```python
 | @abstractmethod
 | streams(config: Mapping[str, Any]) -> List[Stream]
```

**Arguments**:

- `config`: The user-provided configuration as specified by the source's spec. Any stream construction related operation should happen here.

**Returns**:

A list of the streams in this source connector.

<a name="cdk.abstract_source.AbstractSource.name"></a>
#### name

```python
 | @property
 | name() -> str
```

Source name

<a name="cdk.abstract_source.AbstractSource.discover"></a>
#### discover

```python
 | discover(logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteCatalog
```

Implements the Discover operation from the Airbyte Specification. See https://docs.airbyte.io/architecture/airbyte-specification.

<a name="cdk.abstract_source.AbstractSource.check"></a>
#### check

```python
 | check(logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus
```

Implements the Check Connection operation from the Airbyte Specification. See https://docs.airbyte.io/architecture/airbyte-specification.

<a name="cdk.abstract_source.AbstractSource.read"></a>
#### read

```python
 | read(logger: AirbyteLogger, config: Mapping[str, Any], catalog: ConfiguredAirbyteCatalog, state: MutableMapping[str, Any] = None) -> Iterator[AirbyteMessage]
```

Implements the Read operation from the Airbyte Specification. See https://docs.airbyte.io/architecture/airbyte-specification.

