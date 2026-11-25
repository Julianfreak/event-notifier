package docs

import _ "embed"

// OpenAPISpec is the OpenAPI definition served by the API and Swagger UI.
//
//go:embed openapi.yaml
var OpenAPISpec []byte
