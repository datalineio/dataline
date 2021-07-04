# TODO: Define your stream schemas
Your connector must describe the schema of each stream it can output using [JSONSchema](https://json-schema.org). 

The simplest way to do this is to describe the schema of your streams using one `.json` file per stream. You can also dynamically generate the schema of your stream in code, or you can combine both approaches: start with a `.json` file and dynamically add properties to it. 
 
The schema of a stream is the return value of `Stream.get_json_schema`.
 
## Static schemas
By default, `Stream.get_json_schema` reads a `.json` file in the `schemas/` directory whose name is equal to the value of the `Stream.name` property. In turn `Stream.name` by default returns the name of the class in snake case. Therefore, if you have a class `class EmployeeBenefits(HttpStream)` the default behavior will look for a file called `schemas/employee_benefits.json`. You can override any of these behaviors as you need.

Important note: any objects referenced via `$ref` should be placed in the `shared/` directory in their own `.json` files.
 
## Dynamic schemas
If you'd rather define your schema in code, override `Stream.get_json_schema` in your stream class to return a `dict` describing the schema using [JSONSchema](https://json-schema.org).

## Dynamically modifying static schemas    
Override `Stream.get_json_schema` to run the default behavior, edit the returned value, then return the edited value: 
```
def get_json_schema(self):
    schema = super().get_json_schema()
    schema['dynamically_determined_property'] = "property"
    return schema
```

Delete this file once you're done. Or don't. Up to you :)  
