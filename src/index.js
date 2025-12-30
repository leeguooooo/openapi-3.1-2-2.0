import RefParserModule from "@apidevtools/json-schema-ref-parser";
import YamlModule from "js-yaml";
import { convertOpenapi3ToSwagger2 } from "./openapi3ToSwagger2.js";

const RefParser = RefParserModule.default ?? RefParserModule;
const Yaml = YamlModule.default ?? YamlModule;

const DEFAULT_TIMEOUT_MS = 15000;
const MAX_TIMEOUT_MS = 30000;
const MAX_SPEC_BYTES = 5 * 1024 * 1024;
const USER_AGENT = "openapi-31-to-20-worker";

const HTTP_METHODS = ["get", "put", "post", "delete", "options", "head", "patch", "trace"];

class SpecTooLargeError extends Error {
  constructor(message) {
    super(message);
    this.name = "SpecTooLargeError";
  }
}

const UNSUPPORTED_JSON_SCHEMA_KEYS = [
  "$schema",
  "$id",
  "anchor",
  "defs",
  "$defs",
  "if",
  "then",
  "else",
  "dependentSchemas",
  "dependentRequired",
  "unevaluatedItems",
  "unevaluatedProperties",
  "propertyNames",
  "patternProperties",
  "contains",
  "minContains",
  "maxContains",
  "prefixItems",
  "contentEncoding",
  "contentMediaType",
  "contentSchema",
  "examples",
];

export default {
  async fetch(request) {
    const requestStartedAt = Date.now();
    if (request.method === "OPTIONS") {
      return respond(
        requestStartedAt,
        null,
        new Response(null, { status: 204, headers: corsHeaders() }),
        "options"
      );
    }

    if (request.method !== "GET") {
      return respond(
        requestStartedAt,
        null,
        errorResponse(405, "method_not_allowed", "Only GET is supported.")
      );
    }

    const requestUrl = new URL(request.url);
    const debug = parseBoolean(requestUrl.searchParams.get("debug"));
    const timings = {};
    const log = createLogger(makeRequestId(), debug);
    log.info("request_start", request.method, requestUrl.pathname, requestUrl.search);

    const sourceUrl = requestUrl.searchParams.get("url");
    if (!sourceUrl) {
      return respond(requestStartedAt, log, usageResponse(), "usage", timings, wantsTimings);
    }

    let parsedSource;
    try {
      parsedSource = new URL(sourceUrl);
    } catch (error) {
      return respond(
        requestStartedAt,
        log,
        errorResponse(400, "invalid_url", "Query param 'url' must be a valid URL.")
      );
    }

    if (parsedSource.protocol !== "http:" && parsedSource.protocol !== "https:") {
      return respond(
        requestStartedAt,
        log,
        errorResponse(400, "invalid_url", "Only http and https URLs are supported.")
      );
    }

    const format = normalizeFormat(requestUrl.searchParams.get("format"));
    const pretty = parseBoolean(requestUrl.searchParams.get("pretty"));
    const diagnostics = parseBoolean(requestUrl.searchParams.get("diagnostics"));
    const wantsTimings = debug || diagnostics;
    const timeoutMs = clampTimeoutMs(requestUrl.searchParams.get("timeout"));

    let bundledSpec;
    let usedBundle = false;
    try {
      const loadStartedAt = Date.now();
      log.info("spec_load_start", parsedSource.toString());
      const loaded = await loadSpec(parsedSource.toString(), timeoutMs, log, timings);
      bundledSpec = loaded.spec;
      usedBundle = loaded.usedBundle;
      log.info("spec_load_done", `${Date.now() - loadStartedAt}ms`, usedBundle ? "bundled" : "direct");
    } catch (error) {
      log.error("spec_load_failed", error);
      if (error instanceof SpecTooLargeError) {
        return respond(
          requestStartedAt,
          log,
          errorResponse(413, "spec_too_large", "OpenAPI document is too large.")
        );
      }
      return respond(
        requestStartedAt,
        log,
        errorResponse(
          502,
          "fetch_failed",
          "Failed to fetch or parse the OpenAPI document.",
          String(error && error.message ? error.message : error)
        )
      );
    }

    if (usedBundle && estimateSize(bundledSpec) > MAX_SPEC_BYTES) {
      log.warn("spec_too_large", "bundled");
      return respond(
        requestStartedAt,
        log,
        errorResponse(413, "spec_too_large", "OpenAPI document is too large.")
      );
    }

    if (bundledSpec.swagger === "2.0") {
      const renderStartedAt = Date.now();
      const response = renderSpec(bundledSpec, {
        format,
        pretty,
        diagnostics,
        warnings: [],
        sourceUrl: parsedSource.toString(),
        log,
        timings,
      });
      timings.renderMs = Date.now() - renderStartedAt;
      return respond(requestStartedAt, log, response, "pass_through", timings, wantsTimings);
    }

    if (!bundledSpec.openapi) {
      return respond(
        requestStartedAt,
        log,
        errorResponse(400, "missing_openapi", "Input does not look like OpenAPI 3.x.")
      );
    }

    const warnings = diagnostics ? [] : null;
    const originalVersion = bundledSpec.openapi;
    const normalizeStartedAt = Date.now();
    log.debug("normalize_start");
    const normalized = normalizeOpenapi31(bundledSpec, warnings, diagnostics, log, debug);
    timings.normalizeMs = Date.now() - normalizeStartedAt;
    log.debug("normalize_done", `${Date.now() - normalizeStartedAt}ms`);

    let swaggerSpec;
    try {
      const convertStartedAt = Date.now();
      log.debug("convert_start");
      swaggerSpec = convertOpenapi3ToSwagger2(normalized, { log, debug });
      timings.convertMs = Date.now() - convertStartedAt;
      log.debug("convert_done", `${Date.now() - convertStartedAt}ms`);
    } catch (error) {
      log.error("conversion_failed", error);
      return respond(
        requestStartedAt,
        log,
        errorResponse(
          422,
          "conversion_failed",
          "Failed to convert the OpenAPI document.",
          String(error && error.message ? error.message : error)
        )
      );
    }
    const finalizedSpec = finalizeSwaggerSpec(swaggerSpec);

    const renderStartedAt = Date.now();
    const response = renderSpec(finalizedSpec, {
      format,
      pretty,
      diagnostics,
      warnings: warnings || [],
      sourceUrl: parsedSource.toString(),
      originalVersion,
      log,
      timings,
    });
    timings.renderMs = Date.now() - renderStartedAt;
    return respond(requestStartedAt, log, response, "ok", timings, wantsTimings);
  },
};

function corsHeaders() {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET, OPTIONS",
    "access-control-allow-headers": "content-type",
  };
}

function usageResponse() {
  const body = [
    "OpenAPI 3.1 to OpenAPI 2.0 converter.",
    "", 
    "Usage:",
    "  https://xxx.com/?url=https://example.com/openapi.yaml",
    "",
    "Optional query params:",
    "  format=json|yaml   Output format (default: json)",
    "  pretty=1           Pretty-print JSON output",
    "  diagnostics=1      Include conversion notes in x-conversion-info",
    "  timeout=15         Fetch timeout in seconds (max 30)",
    "  debug=1            Print timing logs in worker console",
  ].join("\n");

  return new Response(body, {
    status: 200,
    headers: {
      ...corsHeaders(),
      "content-type": "text/plain; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function errorResponse(status, code, message, details) {
  const payload = { error: code, message };
  if (details) {
    payload.details = details;
  }

  return new Response(JSON.stringify(payload, null, 2), {
    status,
    headers: {
      ...corsHeaders(),
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function normalizeFormat(value) {
  if (!value) return "json";
  const normalized = value.toLowerCase();
  return normalized === "yaml" ? "yaml" : "json";
}

function parseBoolean(value) {
  if (!value) return false;
  return value === "1" || value.toLowerCase() === "true";
}

function clampTimeoutMs(value) {
  if (!value) return DEFAULT_TIMEOUT_MS;
  const seconds = Number.parseFloat(value);
  if (!Number.isFinite(seconds) || seconds <= 0) return DEFAULT_TIMEOUT_MS;
  return Math.min(seconds * 1000, MAX_TIMEOUT_MS);
}

async function loadSpec(sourceUrl, timeoutMs, log, timings) {
  const fetchStartedAt = Date.now();
  const text = await fetchSpecText(sourceUrl, timeoutMs, log);
  if (log) {
    log.debug("fetch_text_done", `${Date.now() - fetchStartedAt}ms`, `bytes=${text.length}`);
  }
  if (timings) {
    timings.fetchMs = Date.now() - fetchStartedAt;
  }
  if (text.length > MAX_SPEC_BYTES) {
    throw new SpecTooLargeError("OpenAPI document is too large.");
  }

  const parseStartedAt = Date.now();
  const parsed = parseSpecText(text);
  if (log) {
    log.debug("parse_done", `${Date.now() - parseStartedAt}ms`);
  }
  if (timings) {
    timings.parseMs = Date.now() - parseStartedAt;
    timings.specBytes = text.length;
  }
  if (!parsed || typeof parsed !== "object") {
    throw new Error("OpenAPI document is not an object.");
  }

  const externalRef = findExternalRef(parsed);
  if (!externalRef) {
    if (log) {
      log.debug("external_ref", "none");
    }
    return { spec: parsed, usedBundle: false };
  }

  if (log) {
    log.info("external_ref", externalRef);
  }

  const bundleStartedAt = Date.now();
  const bundled = await bundleSpec(sourceUrl, parsed, timeoutMs);
  if (log) {
    log.info("bundle_done", `${Date.now() - bundleStartedAt}ms`);
  }
  if (timings) {
    timings.bundleMs = Date.now() - bundleStartedAt;
  }
  return { spec: bundled, usedBundle: true };
}

async function fetchSpecText(sourceUrl, timeoutMs, log) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    if (log) {
      log.debug("fetch_text_start", sourceUrl);
    }
    const response = await fetch(sourceUrl, {
      headers: {
        "user-agent": USER_AGENT,
        "accept": "application/json, application/yaml, text/yaml, */*",
      },
      signal: controller.signal,
    });

    if (!response.ok) {
      throw new Error(`Upstream returned ${response.status}.`);
    }

    const contentLength = response.headers.get("content-length");
    if (contentLength && Number(contentLength) > MAX_SPEC_BYTES) {
      throw new SpecTooLargeError("OpenAPI document is too large.");
    }

    const text = await response.text();
    return text;
  } finally {
    clearTimeout(timeoutId);
  }
}

function parseSpecText(text) {
  try {
    return JSON.parse(text);
  } catch (error) {
    try {
      return Yaml.load(text, { schema: Yaml.JSON_SCHEMA });
    } catch (yamlError) {
      return Yaml.load(text);
    }
  }
}

function findExternalRef(root) {
  if (!root || typeof root !== "object") return null;
  const seen = new WeakSet();
  const stack = [root];

  while (stack.length > 0) {
    const node = stack.pop();
    if (!node || typeof node !== "object") continue;
    if (seen.has(node)) continue;
    seen.add(node);

    if (Object.prototype.hasOwnProperty.call(node, "$ref")) {
      const refValue = node.$ref;
      if (typeof refValue === "string" && !refValue.startsWith("#")) {
        return refValue;
      }
    }

    if (Array.isArray(node)) {
      for (let i = node.length - 1; i >= 0; i -= 1) {
        stack.push(node[i]);
      }
    } else {
      for (const key in node) {
        stack.push(node[key]);
      }
    }
  }

  return null;
}

async function bundleSpec(sourceUrl, schema, timeoutMs) {
  return RefParser.bundle(sourceUrl, schema, {
    resolve: {
      file: false,
      http: {
        timeout: timeoutMs,
        headers: {
          "user-agent": USER_AGENT,
          "accept": "application/json, application/yaml, text/yaml, */*",
        },
      },
    },
    mutateInputSchema: true,
  });
}

function estimateSize(value) {
  try {
    return JSON.stringify(value).length;
  } catch (error) {
    return MAX_SPEC_BYTES + 1;
  }
}

function normalizeOpenapi31(spec, warnings, collectWarnings, log, debug) {
  const openapiVersion = String(spec.openapi || "");
  const shouldWarn = collectWarnings && Array.isArray(warnings);
  const is31 = openapiVersion.startsWith("3.1");
  const shouldLog = Boolean(debug && log && typeof log.debug === "function");
  let visited = 0;

  if (is31) {
    spec.openapi = "3.0.3";
    if (shouldWarn) {
      warnings.push("Downgraded openapi version 3.1.x to 3.0.3 for conversion.");
    }
  }

  if (spec.jsonSchemaDialect) {
    delete spec.jsonSchemaDialect;
    if (shouldWarn) {
      warnings.push("Removed jsonSchemaDialect (not supported in OpenAPI 3.0).");
    }
  }

  if (!is31) {
    if (shouldLog) {
      log.debug("normalize_skip", `openapi=${openapiVersion || "unknown"}`);
    }
    return spec;
  }

  walkSchemas(
    spec,
    (schema, path) => {
      if (shouldLog) {
        visited += 1;
        if (visited % 2000 === 0) {
          log.debug("normalize_schema_progress", `schemas=${visited}`);
        }
      }
      normalizeSchemaNode(schema, path, warnings, shouldWarn);
    },
    { trackPath: shouldWarn }
  );

  if (shouldLog) {
    log.debug("normalize_complete", `nodes=${visited}`);
  }
  return spec;
}

function walkSchemas(spec, visitor, options) {
  if (!spec || typeof spec !== "object") return;
  const trackPath = Boolean(options && options.trackPath);
  const seen = new WeakSet();
  const stack = [];

  const pushSchema = (schema, path) => {
    if (!schema || typeof schema !== "object") return;
    if (seen.has(schema)) return;
    seen.add(schema);
    stack.push(trackPath ? { schema, path } : { schema });
  };

  const walkContent = (content, basePath) => {
    if (!content || typeof content !== "object") return;
    for (const mediaType in content) {
      const media = content[mediaType];
      if (media && media.schema) {
        pushSchema(media.schema, basePath.concat(mediaType, "schema"));
      }
    }
  };

  const walkParameter = (param, basePath) => {
    if (!param || typeof param !== "object") return;
    if (param.schema) {
      pushSchema(param.schema, basePath.concat("schema"));
    }
    if (param.content) {
      walkContent(param.content, basePath.concat("content"));
    }
  };

  const walkParameters = (parameters, basePath) => {
    if (!Array.isArray(parameters)) return;
    parameters.forEach((param, index) => {
      walkParameter(param, basePath.concat("parameters", index));
    });
  };

  const walkHeader = (header, basePath) => {
    if (!header || typeof header !== "object") return;
    if (header.schema) {
      pushSchema(header.schema, basePath.concat("schema"));
    }
    if (header.content) {
      walkContent(header.content, basePath.concat("content"));
    }
  };

  const walkHeaders = (headers, basePath) => {
    if (!headers || typeof headers !== "object") return;
    for (const name in headers) {
      walkHeader(headers[name], basePath.concat(name));
    }
  };

  const walkResponse = (response, basePath) => {
    if (!response || typeof response !== "object") return;
    if (response.content) {
      walkContent(response.content, basePath.concat("content"));
    }
    if (response.headers) {
      walkHeaders(response.headers, basePath.concat("headers"));
    }
  };

  const walkResponses = (responses, basePath) => {
    if (!responses || typeof responses !== "object") return;
    for (const code in responses) {
      walkResponse(responses[code], basePath.concat(code));
    }
  };

  const walkRequestBody = (requestBody, basePath) => {
    if (!requestBody || typeof requestBody !== "object") return;
    if (requestBody.content) {
      walkContent(requestBody.content, basePath.concat("content"));
    }
  };

  const walkCallback = (callback, basePath) => {
    if (!callback || typeof callback !== "object") return;
    for (const expr in callback) {
      walkPathItem(callback[expr], basePath.concat(expr));
    }
  };

  const walkCallbacks = (callbacks, basePath) => {
    if (!callbacks || typeof callbacks !== "object") return;
    for (const name in callbacks) {
      walkCallback(callbacks[name], basePath.concat(name));
    }
  };

  const walkOperation = (operation, basePath) => {
    if (!operation || typeof operation !== "object") return;
    walkParameters(operation.parameters, basePath);
    if (operation.requestBody) {
      walkRequestBody(operation.requestBody, basePath.concat("requestBody"));
    }
    if (operation.responses) {
      walkResponses(operation.responses, basePath.concat("responses"));
    }
    if (operation.callbacks) {
      walkCallbacks(operation.callbacks, basePath.concat("callbacks"));
    }
  };

  const walkPathItem = (pathItem, basePath) => {
    if (!pathItem || typeof pathItem !== "object") return;
    walkParameters(pathItem.parameters, basePath);
    for (const method of HTTP_METHODS) {
      if (pathItem[method]) {
        walkOperation(pathItem[method], basePath.concat(method));
      }
    }
    if (pathItem.callbacks) {
      walkCallbacks(pathItem.callbacks, basePath.concat("callbacks"));
    }
  };

  const walkComponents = (components, basePath) => {
    if (!components || typeof components !== "object") return;
    if (components.schemas) {
      for (const name in components.schemas) {
        pushSchema(components.schemas[name], basePath.concat("schemas", name));
      }
    }
    if (components.parameters) {
      for (const name in components.parameters) {
        walkParameter(components.parameters[name], basePath.concat("parameters", name));
      }
    }
    if (components.requestBodies) {
      for (const name in components.requestBodies) {
        walkRequestBody(components.requestBodies[name], basePath.concat("requestBodies", name));
      }
    }
    if (components.responses) {
      for (const name in components.responses) {
        walkResponse(components.responses[name], basePath.concat("responses", name));
      }
    }
    if (components.headers) {
      for (const name in components.headers) {
        walkHeader(components.headers[name], basePath.concat("headers", name));
      }
    }
    if (components.pathItems) {
      for (const name in components.pathItems) {
        walkPathItem(components.pathItems[name], basePath.concat("pathItems", name));
      }
    }
    if (components.callbacks) {
      for (const name in components.callbacks) {
        walkCallback(components.callbacks[name], basePath.concat("callbacks", name));
      }
    }
  };

  if (spec.paths) {
    for (const path in spec.paths) {
      walkPathItem(spec.paths[path], ["paths", path]);
    }
  }
  if (spec.webhooks) {
    for (const name in spec.webhooks) {
      walkPathItem(spec.webhooks[name], ["webhooks", name]);
    }
  }
  if (spec.components) {
    walkComponents(spec.components, ["components"]);
  }

  while (stack.length > 0) {
    const current = stack.pop();
    const schema = current && current.schema;
    const path = trackPath && current ? current.path : undefined;
    if (!schema || typeof schema !== "object") continue;
    visitor(schema, path || []);

    if (schema.allOf) {
      schema.allOf.forEach((item, index) => {
        pushSchema(item, (path || []).concat("allOf", index));
      });
    }
    if (schema.anyOf) {
      schema.anyOf.forEach((item, index) => {
        pushSchema(item, (path || []).concat("anyOf", index));
      });
    }
    if (schema.oneOf) {
      schema.oneOf.forEach((item, index) => {
        pushSchema(item, (path || []).concat("oneOf", index));
      });
    }
    if (schema.not) {
      pushSchema(schema.not, (path || []).concat("not"));
    }
    if (schema.items) {
      if (Array.isArray(schema.items)) {
        schema.items.forEach((item, index) => {
          pushSchema(item, (path || []).concat("items", index));
        });
      } else {
        pushSchema(schema.items, (path || []).concat("items"));
      }
    }
    if (schema.properties && typeof schema.properties === "object") {
      for (const propName in schema.properties) {
        pushSchema(schema.properties[propName], (path || []).concat("properties", propName));
      }
    }
    if (schema.additionalProperties && typeof schema.additionalProperties === "object") {
      pushSchema(schema.additionalProperties, (path || []).concat("additionalProperties"));
    }
  }
}

function normalizeSchemaNode(schema, path, warnings, collectWarnings) {
  if (Array.isArray(schema.examples) && schema.examples.length > 0 && schema.example === undefined) {
    schema.example = schema.examples[0];
  }

  const unsupported = {};
  for (const key of UNSUPPORTED_JSON_SCHEMA_KEYS) {
    if (Object.prototype.hasOwnProperty.call(schema, key)) {
      unsupported[key] = schema[key];
      delete schema[key];
    }
  }

  if (Object.keys(unsupported).length > 0) {
    schema["x-oas31-unsupported"] = unsupported;
    if (collectWarnings) {
      warnings.push(`Removed unsupported JSON Schema keywords at ${formatPath(path)}.`);
    }
  }

  if (Array.isArray(schema.type)) {
    const types = schema.type.filter((item) => item !== "null");
    if (schema.type.includes("null")) {
      schema.nullable = schema.nullable ?? true;
    }

    if (types.length === 1) {
      schema.type = types[0];
    } else if (types.length > 1) {
      schema.type = types[0];
      schema["x-type-alternatives"] = types.slice(1);
      if (collectWarnings) {
        warnings.push(`Collapsed multiple schema types at ${formatPath(path)}.`);
      }
    } else {
      delete schema.type;
      if (collectWarnings) {
        warnings.push(`Dropped null-only schema type at ${formatPath(path)}.`);
      }
    }
  }

  if (Object.prototype.hasOwnProperty.call(schema, "const")) {
    if (!Object.prototype.hasOwnProperty.call(schema, "enum")) {
      schema.enum = [schema.const];
    }
    delete schema.const;
    if (collectWarnings) {
      warnings.push(`Replaced const with enum at ${formatPath(path)}.`);
    }
  }

  if (typeof schema.exclusiveMinimum === "number") {
    if (schema.minimum !== undefined && schema.minimum !== schema.exclusiveMinimum) {
      if (collectWarnings) {
        warnings.push(`exclusiveMinimum overwrote minimum at ${formatPath(path)}.`);
      }
    }
    schema.minimum = schema.exclusiveMinimum;
    schema.exclusiveMinimum = true;
  }

  if (typeof schema.exclusiveMaximum === "number") {
    if (schema.maximum !== undefined && schema.maximum !== schema.exclusiveMaximum) {
      if (collectWarnings) {
        warnings.push(`exclusiveMaximum overwrote maximum at ${formatPath(path)}.`);
      }
    }
    schema.maximum = schema.exclusiveMaximum;
    schema.exclusiveMaximum = true;
  }
}

function formatPath(path) {
  if (!path || !path.length) return "#/";
  const parts = path.map((part) => String(part).replace(/~/g, "~0").replace(/\//g, "~1"));
  return `#/${parts.join("/")}`;
}

function finalizeSwaggerSpec(spec) {
  const output = spec && typeof spec === "object" ? spec : {};
  output.swagger = "2.0";
  output.info = output.info || { title: "API", version: "0.0.0" };
  output.info.title = output.info.title || "API";
  output.info.version = output.info.version || "0.0.0";
  output.paths = output.paths || {};
  return output;
}

function renderSpec(spec, options) {
  const { format, pretty, diagnostics, warnings, sourceUrl, originalVersion, log, timings } = options;
  const outputSpec = spec && typeof spec === "object" ? spec : {};

  if (diagnostics) {
    outputSpec["x-conversion-info"] = {
      source: sourceUrl,
      warnings,
      originalOpenapi: originalVersion,
      convertedAt: new Date().toISOString(),
      timings: snapshotTimings(timings),
    };
  }

  const body = format === "yaml"
    ? Yaml.dump(outputSpec, { lineWidth: -1, noRefs: true })
    : JSON.stringify(outputSpec, null, pretty ? 2 : 0);
  if (log) {
    log.debug("render_done", `format=${format}`, `bytes=${body.length}`);
  }

  return new Response(body, {
    status: 200,
    headers: {
      ...corsHeaders(),
      "content-type": format === "yaml" ? "application/yaml; charset=utf-8" : "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function respond(startedAt, log, response, label, timings, includeTimingsHeader) {
  if (log) {
    const ms = Date.now() - startedAt;
    log.info("request_done", label || "response", `status=${response.status}`, `${ms}ms`);
  }
  if (!timings && !includeTimingsHeader) {
    return response;
  }
  const headers = new Headers(response.headers);
  if (includeTimingsHeader && timings) {
    timings.totalMs = Date.now() - startedAt;
    const serverTiming = formatServerTiming(timings);
    if (serverTiming) {
      headers.set("server-timing", serverTiming);
    }
  }
  return new Response(response.body, {
    status: response.status,
    headers,
  });
}

function createLogger(requestId, debug) {
  const prefix = `[req:${requestId}]`;
  return {
    info: (...args) => console.log(prefix, ...args),
    debug: (...args) => {
      if (debug) console.log(prefix, ...args);
    },
    warn: (...args) => console.warn(prefix, ...args),
    error: (...args) => console.error(prefix, ...args),
  };
}

function makeRequestId() {
  return Math.random().toString(36).slice(2, 8);
}

function snapshotTimings(timings) {
  if (!timings) return undefined;
  const output = {};
  for (const [key, value] of Object.entries(timings)) {
    if (!key.endsWith("Ms")) {
      continue;
    }
    if (typeof value === "number" && Number.isFinite(value)) {
      output[key] = Math.round(value);
    }
  }
  return Object.keys(output).length ? output : undefined;
}

function formatServerTiming(timings) {
  if (!timings) return "";
  const mapping = [
    ["fetchMs", "fetch"],
    ["parseMs", "parse"],
    ["bundleMs", "bundle"],
    ["normalizeMs", "normalize"],
    ["convertMs", "convert"],
    ["renderMs", "render"],
    ["totalMs", "total"],
  ];
  const parts = [];
  for (const [key, label] of mapping) {
    const value = timings[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      parts.push(`${label};dur=${Math.round(value)}`);
    }
  }
  return parts.join(", ");
}
