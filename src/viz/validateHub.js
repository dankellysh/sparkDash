/**
 * Frozen v1 hub-output validator. Ajv against contracts JSON Schema.
 * Semantic stream rules stay in frameReducer.js.
 */
import Ajv from "ajv/dist/2020.js";
import nodeSchema from "./contracts/node-snapshot-v1.schema.json" with { type: "json" };
import traceSchema from "./contracts/trace-event-v1.schema.json" with { type: "json" };

const TRACE_ID = "https://schemas.dgxspark.local/telemetry/trace-event-v1.schema.json";

const ajv = new Ajv({
  allErrors: true,
  strict: false,
  validateFormats: false,
});
ajv.addSchema(nodeSchema);
ajv.addSchema(traceSchema);

const validate = ajv.getSchema(`${TRACE_ID}#hub-output`);
if (!validate) {
  throw new Error("failed to compile #hub-output");
}

export function hubEventErrors(ev) {
  if (validate(ev)) return null;
  return (validate.errors || []).map((e) => `${e.instancePath || "/"} ${e.message}`);
}
