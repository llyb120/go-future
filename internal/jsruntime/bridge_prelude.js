(() => {
  if (globalThis.host) {
    return;
  }

  const internals = {
    exports: Object.create(null),
    export(name, fn) {
      if (typeof name !== "string" || name.length === 0) {
        throw new Error("host.export(name, fn) requires a non-empty string name");
      }
      if (typeof fn !== "function") {
        throw new Error(`host.export(${name}) requires a function`);
      }
      this.exports[name] = fn;
    },
    async call(name, ...args) {
      if (typeof globalThis.__go_bridge_call__ !== "function") {
        throw new Error("__go_bridge_call__ is not installed");
      }
      return await globalThis.__go_bridge_call__(String(name), args);
    },
  };

  globalThis.host = new Proxy(internals, {
    get(target, prop, receiver) {
      if (typeof prop === "symbol" || prop in target) {
        return Reflect.get(target, prop, receiver);
      }
      return (...args) => target.call(String(prop), ...args);
    },
  });
})();
