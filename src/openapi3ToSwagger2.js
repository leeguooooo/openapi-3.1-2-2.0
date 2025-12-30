const HTTP_METHODS = ["get", "put", "post", "delete", "options", "head", "patch", "trace"];
const SCHEMA_PROPERTIES = [
  "format",
  "minimum",
  "maximum",
  "exclusiveMinimum",
  "exclusiveMaximum",
  "minLength",
  "maxLength",
  "multipleOf",
  "minItems",
  "maxItems",
  "uniqueItems",
  "minProperties",
  "maxProperties",
  "additionalProperties",
  "pattern",
  "enum",
  "default",
];
const ARRAY_PROPERTIES = ["type", "items"];

const APPLICATION_JSON_REGEX = /^(application\/json|[^;\/ \t]+\/[^;\/ \t]+[+]json)[ \t]*(;.*)?$/;
const SUPPORTED_MIME_TYPES = {
  APPLICATION_X_WWW_URLENCODED: "application/x-www-form-urlencoded",
  MULTIPART_FORM_DATA: "multipart/form-data",
};
const hasOwnProperty = Object.prototype.hasOwnProperty;

export function convertOpenapi3ToSwagger2(spec, options = {}) {
  const converter = new Converter(spec, options);
  return converter.convert();
}

class Converter {
  constructor(spec, options) {
    this.spec = spec;
    this.log = options && options.log;
    this.debug = Boolean(options && options.debug);
    this.trace = this.debug ? { schemaCount: 0 } : null;
  }

  convert() {
    const totalStartedAt = Date.now();
    this.spec.swagger = "2.0";

    const infosStartedAt = Date.now();
    this.convertInfos();
    this.logDebug("convert_infos_done", `${Date.now() - infosStartedAt}ms`);

    const opsStartedAt = Date.now();
    this.convertOperations();
    this.logDebug("convert_operations_done", `${Date.now() - opsStartedAt}ms`);
    if (this.spec.components) {
      const schemasStartedAt = Date.now();
      this.convertSchemas();
      const schemaInfo = this.trace ? `schemas=${this.trace.schemaCount}` : undefined;
      this.logDebug("convert_schemas_done", `${Date.now() - schemasStartedAt}ms`, schemaInfo);

      const securityStartedAt = Date.now();
      this.convertSecurityDefinitions();
      this.logDebug("convert_security_done", `${Date.now() - securityStartedAt}ms`);

      this.spec["x-components"] = this.spec.components;
      delete this.spec.components;

      fixRefs(this.spec);
    }
    this.logDebug("convert_total_done", `${Date.now() - totalStartedAt}ms`);
    return this.spec;
  }

  logDebug(...args) {
    if (this.debug && this.log && typeof this.log.debug === "function") {
      this.log.debug(...args.filter((value) => value));
    }
  }

  resolveReference(base, obj, shouldClone) {
    if (!obj || !obj.$ref) return obj;
    const ref = obj.$ref;
    if (ref.startsWith("#")) {
      const keys = ref.split("/").map((key) => key.replace(/~1/g, "/").replace(/~0/g, "~"));
      keys.shift();
      let cur = base;
      keys.forEach((key) => {
        cur = cur[key];
      });
      return shouldClone ? deepClone(cur) : cur;
    }
    throw new Error("External $ref values are not supported after bundling.");
  }

  convertInfos() {
    const server = this.spec.servers && this.spec.servers[0];
    if (server) {
      let serverUrl = server.url;
      const variables = server.variables || {};
      for (const variable in variables) {
        const variableObject = variables[variable] || {};
        if (variableObject.default) {
          const re = new RegExp(`{${variable}}`, "g");
          serverUrl = serverUrl.replace(re, variableObject.default);
        }
      }

      const parsed = parseServerUrl(serverUrl);
      if (parsed.host == null) {
        delete this.spec.host;
      } else {
        this.spec.host = parsed.host;
      }
      if (parsed.scheme == null) {
        delete this.spec.schemes;
      } else {
        this.spec.schemes = [parsed.scheme];
      }
      if (parsed.pathname) {
        this.spec.basePath = parsed.pathname;
      }
    }
    delete this.spec.servers;
    delete this.spec.openapi;
  }

  convertOperations() {
    const paths = this.spec.paths || {};
    const pathKeys = Object.keys(paths);
    let operationCount = 0;
    this.logDebug("convert_operations_start", `paths=${pathKeys.length}`);

    for (const path of pathKeys) {
      const pathObject = (paths[path] = this.resolveReference(this.spec, paths[path], true));
      this.convertParameters(pathObject);
      for (const method in pathObject) {
        if (HTTP_METHODS.indexOf(method) >= 0) {
          operationCount += 1;
          if (this.debug && operationCount % 200 === 0) {
            this.logDebug("convert_operations_progress", `ops=${operationCount}`);
          }
          const operation = (pathObject[method] = this.resolveReference(this.spec, pathObject[method], true));
          this.convertOperationParameters(operation);
          this.convertResponses(operation);
        }
      }
    }
    this.logDebug("convert_operations_end", `ops=${operationCount}`);
  }

  convertOperationParameters(operation) {
    let content;
    let param;
    let contentKey;
    let mediaRanges;
    let mediaTypes;
    operation.parameters = operation.parameters || [];
    if (operation.requestBody) {
      param = this.resolveReference(this.spec, operation.requestBody, true);

      if (operation.requestBody.content) {
        const type = getSupportedMimeTypes(operation.requestBody.content)[0];
        const structuredObj = { content: {} };
        const data = operation.requestBody.content[type];

        if (data && data.schema && data.schema.$ref && !data.schema.$ref.startsWith("#")) {
          param = this.resolveReference(this.spec, data.schema, true);
          structuredObj.content[`${type}`] = { schema: param };
          param = structuredObj;
        }
      }

      param.name = "body";
      content = param.content;
      if (content && Object.keys(content).length) {
        mediaRanges = Object.keys(content).filter((mediaRange) => mediaRange.indexOf("/") > 0);
        mediaTypes = mediaRanges.filter((range) => range.indexOf("*") < 0);
        contentKey = getSupportedMimeTypes(content)[0];
        delete param.content;

        if (
          contentKey === SUPPORTED_MIME_TYPES.APPLICATION_X_WWW_URLENCODED ||
          contentKey === SUPPORTED_MIME_TYPES.MULTIPART_FORM_DATA
        ) {
          operation.consumes = mediaTypes;
          param.in = "formData";
          param.schema = content[contentKey].schema;
          param.schema = this.resolveReference(this.spec, param.schema, true);
          if (param.schema.type === "object" && param.schema.properties) {
            const required = param.schema.required || [];
            for (const name in param.schema.properties) {
              const schema = param.schema.properties[name];
              if (!schema.readOnly) {
                const formDataParam = {
                  name,
                  in: "formData",
                  schema,
                };
                if (required.indexOf(name) >= 0) {
                  formDataParam.required = true;
                }
                operation.parameters.push(formDataParam);
              }
            }
          } else {
            operation.parameters.push(param);
          }
        } else if (contentKey) {
          operation.consumes = mediaTypes;
          param.in = "body";
          param.schema = content[contentKey].schema;
          operation.parameters.push(param);
        } else if (mediaRanges) {
          operation.consumes = mediaTypes || ["application/octet-stream"];
          param.in = "body";
          param.name = param.name || "file";
          delete param.type;
          param.schema = content[mediaRanges[0]].schema || {
            type: "string",
            format: "binary",
          };
          operation.parameters.push(param);
        }

        if (param.schema) {
          this.convertSchema(param.schema, "request");
        }
      }
      delete operation.requestBody;
    }
    this.convertParameters(operation);
  }

  convertParameters(obj) {
    if (obj.parameters === undefined) {
      return;
    }

    obj.parameters = obj.parameters || [];

    obj.parameters.forEach((param, index) => {
      param = obj.parameters[index] = this.resolveReference(this.spec, param, false);
      if (param.in !== "body") {
        this.copySchemaProperties(param, SCHEMA_PROPERTIES);
        this.copySchemaProperties(param, ARRAY_PROPERTIES);
        this.copySchemaXProperties(param);
        if (!param.description) {
          const schema = this.resolveReference(this.spec, param.schema, false);
          if (schema && schema.description) {
            param.description = schema.description;
          }
        }
        delete param.schema;
        delete param.allowReserved;
        if (param.example !== undefined) {
          param["x-example"] = param.example;
        }
        delete param.example;
      }
      if (param.type === "array") {
        const style = param.style || (param.in === "query" || param.in === "cookie" ? "form" : "simple");
        if (style === "matrix") {
          param.collectionFormat = param.explode ? undefined : "csv";
        } else if (style === "label") {
          param.collectionFormat = undefined;
        } else if (style === "simple") {
          param.collectionFormat = "csv";
        } else if (style === "spaceDelimited") {
          param.collectionFormat = "ssv";
        } else if (style === "pipeDelimited") {
          param.collectionFormat = "pipes";
        } else if (style === "deepOpbject") {
          param.collectionFormat = "multi";
        } else if (style === "form") {
          param.collectionFormat = param.explode === false ? "csv" : "multi";
        }
      }
      delete param.style;
      delete param.explode;
    });
  }

  copySchemaProperties(obj, props) {
    const schema = this.resolveReference(this.spec, obj.schema, true);
    if (!schema) return;
    props.forEach((prop) => {
      const value = schema[prop];

      switch (prop) {
        case "additionalProperties":
          if (typeof value === "boolean") return;
      }

      if (value !== undefined) {
        obj[prop] = value;
      }
    });
  }

  copySchemaXProperties(obj) {
    const schema = this.resolveReference(this.spec, obj.schema, true);
    if (!schema) return;
    for (const propName in schema) {
      if (hasOwnProperty.call(schema, propName) && !hasOwnProperty.call(obj, propName) && propName.startsWith("x-")) {
        obj[propName] = schema[propName];
      }
    }
  }

  convertResponses(operation) {
    let anySchema;
    let jsonSchema;
    let response;
    let resolved;
    for (const code in operation.responses) {
      response = operation.responses[code] = this.resolveReference(this.spec, operation.responses[code], true);
      if (response.content) {
        anySchema = jsonSchema = null;
        for (const mediaRange in response.content) {
          const mediaType = mediaRange.indexOf("*") < 0 ? mediaRange : "application/octet-stream";
          if (!operation.produces) {
            operation.produces = [mediaType];
          } else if (operation.produces.indexOf(mediaType) < 0) {
            operation.produces.push(mediaType);
          }

          const content = response.content[mediaRange];

          anySchema = anySchema || content.schema;
          if (!jsonSchema && isJsonMimeType(mediaType)) {
            jsonSchema = content.schema;
          }

          if (content.example) {
            response.examples = response.examples || {};
            response.examples[mediaType] = content.example;
          }
        }

        if (anySchema) {
          response.schema = jsonSchema || anySchema;
          resolved = this.resolveReference(this.spec, response.schema, true);
          if (resolved && response.schema.$ref && !response.schema.$ref.startsWith("#")) {
            response.schema = resolved;
          }

          this.convertSchema(response.schema, "response");
        }
      }

      const headers = response.headers;
      if (headers) {
        for (const header in headers) {
          resolved = this.resolveReference(this.spec, headers[header], true);
          if (resolved.schema) {
            resolved.type = resolved.schema.type;
            resolved.format = resolved.schema.format;
            delete resolved.schema;
          }
          headers[header] = resolved;
        }
      }

      delete response.content;
    }
  }

  convertSchema(def, operationDirection) {
    if (this.trace) {
      this.trace.schemaCount += 1;
      if (this.trace.schemaCount % 2000 === 0) {
        this.logDebug("convert_schema_progress", `schemas=${this.trace.schemaCount}`);
      }
    }
    if (def.oneOf) {
      delete def.oneOf;

      if (def.discriminator) {
        delete def.discriminator;
      }
    }

    if (def.anyOf) {
      delete def.anyOf;

      if (def.discriminator) {
        delete def.discriminator;
      }
    }

    if (def.allOf) {
      for (const index in def.allOf) {
        this.convertSchema(def.allOf[index], operationDirection);
      }
    }

    if (def.discriminator) {
      if (def.discriminator.mapping) {
        this.convertDiscriminatorMapping(def.discriminator.mapping);
      }

      def.discriminator = def.discriminator.propertyName;
    }

    switch (def.type) {
      case "object":
        if (def.properties) {
          for (const propName in def.properties) {
            if (def.properties[propName].writeOnly === true && operationDirection === "response") {
              delete def.properties[propName];
            } else {
              this.convertSchema(def.properties[propName], operationDirection);
              delete def.properties[propName].writeOnly;
            }
          }
        }
      case "array":
        if (def.items) {
          this.convertSchema(def.items, operationDirection);
        }
    }

    if (def.nullable) {
      def["x-nullable"] = true;
      delete def.nullable;
    }

    if (def.deprecated !== undefined) {
      if (def["x-deprecated"] === undefined) {
        def["x-deprecated"] = def.deprecated;
      }
      delete def.deprecated;
    }
  }

  convertSchemas() {
    this.spec.definitions = this.spec.components.schemas;
    this.logDebug("convert_schemas_start", `definitions=${Object.keys(this.spec.definitions || {}).length}`);

    for (const defName in this.spec.definitions) {
      this.convertSchema(this.spec.definitions[defName]);
    }

    delete this.spec.components.schemas;
  }

  convertDiscriminatorMapping(mapping) {
    for (const payload in mapping) {
      const schemaNameOrRef = mapping[payload];
      if (typeof schemaNameOrRef !== "string") {
        console.warn(`Ignoring ${schemaNameOrRef} for ${payload} in discriminator.mapping.`);
        continue;
      }

      let schema;
      if (/^[a-zA-Z0-9._-]+$/.test(schemaNameOrRef)) {
        try {
          schema = this.resolveReference(this.spec, { $ref: `#/components/schemas/${schemaNameOrRef}` }, false);
        } catch (err) {
          console.debug(
            `Error resolving ${schemaNameOrRef} for ${payload} as schema name in discriminator.mapping: ${err}`
          );
        }
      }

      if (!schema) {
        try {
          schema = this.resolveReference(this.spec, { $ref: schemaNameOrRef }, false);
        } catch (err) {
          console.debug(`Error resolving ${schemaNameOrRef} for ${payload} in discriminator.mapping: ${err}`);
        }
      }

      if (schema) {
        schema["x-discriminator-value"] = payload;
        schema["x-ms-discriminator-value"] = payload;
      } else {
        console.warn(`Unable to resolve ${schemaNameOrRef} for ${payload} in discriminator.mapping.`);
      }
    }
  }

  convertSecurityDefinitions() {
    this.spec.securityDefinitions = this.spec.components.securitySchemes;
    for (const secKey in this.spec.securityDefinitions) {
      const security = this.spec.securityDefinitions[secKey];
      if (security.type === "http" && security.scheme === "basic") {
        security.type = "basic";
        delete security.scheme;
      } else if (security.type === "http" && security.scheme === "bearer") {
        security.type = "apiKey";
        security.name = "Authorization";
        security.in = "header";
        delete security.scheme;
        delete security.bearerFormat;
      } else if (security.type === "oauth2") {
        const flowName = Object.keys(security.flows)[0];
        const flow = security.flows[flowName];

        if (flowName === "clientCredentials") {
          security.flow = "application";
        } else if (flowName === "authorizationCode") {
          security.flow = "accessCode";
        } else {
          security.flow = flowName;
        }
        security.authorizationUrl = flow.authorizationUrl;
        security.tokenUrl = flow.tokenUrl;
        security.scopes = flow.scopes;
        delete security.flows;
      }
    }
    delete this.spec.components.securitySchemes;
  }
}

function fixRef(ref) {
  return ref.replace("#/components/schemas/", "#/definitions/").replace("#/components/", "#/x-components/");
}

function fixRefs(obj) {
  if (Array.isArray(obj)) {
    obj.forEach(fixRefs);
  } else if (typeof obj === "object" && obj !== null) {
    for (const key in obj) {
      if (key === "$ref") {
        obj.$ref = fixRef(obj.$ref);
      } else {
        fixRefs(obj[key]);
      }
    }
  }
}

function isJsonMimeType(type) {
  return new RegExp(APPLICATION_JSON_REGEX, "i").test(type);
}

function getSupportedMimeTypes(content) {
  const mimeValues = Object.keys(SUPPORTED_MIME_TYPES).map((key) => SUPPORTED_MIME_TYPES[key]);
  return Object.keys(content).filter((key) => mimeValues.indexOf(key) > -1 || isJsonMimeType(key));
}

function deepClone(value) {
  if (typeof structuredClone === "function") {
    return structuredClone(value);
  }
  return JSON.parse(JSON.stringify(value));
}

function parseServerUrl(serverUrl) {
  if (!serverUrl) return {};
  if (serverUrl.startsWith("http://") || serverUrl.startsWith("https://")) {
    try {
      const parsed = new URL(serverUrl);
      return {
        host: parsed.host || null,
        scheme: parsed.protocol ? parsed.protocol.replace(":", "") : null,
        pathname: parsed.pathname || "",
      };
    } catch (error) {
      return {};
    }
  }

  return {
    host: null,
    scheme: null,
    pathname: serverUrl,
  };
}
