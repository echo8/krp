---
sidebar_position: 2
---

# OpenAPI Spec

[OpenAPI](https://www.openapis.org/) specs are useful for documenting REST APIs and generating clients to interact with them.

KRP can convert your config to an OpenAPI spec. Here is an example:

```bash
krp -config path/to/config.yaml openapi > openapi.yaml
```

The output `openapi.yaml` file can then be used with tools like [OpenAPI Generator](https://github.com/OpenAPITools/openapi-generator) to generate clients in [many different languages](https://openapi-generator.tech/docs/generators#client-generators). And you can use tools like [Redoc](https://github.com/Redocly/redoc) or [Swagger UI](https://github.com/swagger-api/swagger-ui) to generate custom documentation for your KRP REST service.
