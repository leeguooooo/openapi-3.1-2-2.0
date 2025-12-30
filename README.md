# OpenAPI 3.1 to 2.0 Worker

Cloudflare Worker that fetches an OpenAPI 3.1 URL and returns OpenAPI 2.0 (Swagger) JSON or YAML.

## Usage

```
https://YOUR_DOMAIN/?url=https://example.com/openapi.yaml
```

### Query params

- `format=json|yaml` Output format (default: json)
- `pretty=1` Pretty-print JSON output
- `diagnostics=1` Add `x-conversion-info` with warnings and metadata
- `timeout=15` Fetch timeout in seconds (max 30)

## Local dev

```
npm install
npm run dev
```

## Deploy

```
npm run deploy
```

## Notes

- Remote `$ref` values are bundled into local refs before conversion.
- OpenAPI 3.1 JSON Schema keywords that are not supported by OpenAPI 3.0 are moved to
  `x-oas31-unsupported` and a warning is emitted in diagnostics mode.
