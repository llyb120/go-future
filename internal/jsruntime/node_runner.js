"use strict";

const { Console } = require("node:console");
const readline = require("node:readline");
const util = require("node:util");
const vm = require("node:vm");

globalThis.console = new Console({
  stdout: process.stderr,
  stderr: process.stderr,
});

let nextHostCallId = 0;
const pendingHostCalls = new Map();

function send(message) {
  process.stdout.write(JSON.stringify(message) + "\n");
}

function serializeError(error) {
  if (error instanceof Error) {
    return {
      message: error.message,
      stack: error.stack || "",
    };
  }

  return {
    message: util.inspect(error),
    stack: "",
  };
}

globalThis.__go_bridge_call__ = async function (name, args) {
  const id = ++nextHostCallId;
  send({
    type: "host_call",
    id,
    name,
    args,
    result: null,
  });

  return await new Promise((resolve, reject) => {
    pendingHostCalls.set(id, { resolve, reject });
  });
};

const rl = readline.createInterface({
  input: process.stdin,
  crlfDelay: Infinity,
});

async function handleCommand(message) {
  try {
    switch (message.type) {
      case "load":
        vm.runInThisContext(message.source, {
          filename: message.filename || "inline.js",
        });
        send({ type: "response", id: message.id, result: null });
        return;

      case "call": {
        const fn = globalThis.host?.exports?.[message.name];
        if (typeof fn !== "function") {
          throw new Error(`host export "${message.name}" was not registered`);
        }

        const result = await fn(...(message.args || []));
        send({
          type: "response",
          id: message.id,
          result: result === undefined ? null : result,
        });
        return;
      }

      case "close":
        process.stdout.write(
          JSON.stringify({ type: "response", id: message.id, result: null }) + "\n",
          () => process.exit(0),
        );
        return;

      default:
        throw new Error(`unsupported command type: ${message.type}`);
    }
  } catch (error) {
    send({
      type: "response",
      id: message.id,
      result: null,
      error: serializeError(error),
    });
  }
}

rl.on("line", (line) => {
  if (!line.trim()) {
    return;
  }

  let message;
  try {
    message = JSON.parse(line);
  } catch (error) {
    send({
      type: "response",
      id: 0,
      result: null,
      error: serializeError(error),
    });
    return;
  }

  if (message.type === "host_response") {
    const pending = pendingHostCalls.get(message.id);
    if (!pending) {
      return;
    }

    pendingHostCalls.delete(message.id);

    if (message.error) {
      pending.reject(new Error(message.error.stack || message.error.message));
      return;
    }

    pending.resolve(message.result === undefined ? null : message.result);
    return;
  }

  void handleCommand(message);
});

rl.on("close", () => {
  for (const { reject } of pendingHostCalls.values()) {
    reject(new Error("Go host closed the Node bridge"));
  }
  pendingHostCalls.clear();
});
