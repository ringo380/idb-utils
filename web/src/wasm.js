// WASM module loader
let wasmModule = null;

export async function initWasm() {
  if (wasmModule) return wasmModule;
  const mod = await import('../../pkg/idb.js');
  await mod.default();
  wasmModule = mod;
  return mod;
}

export function getWasm() {
  if (!wasmModule) throw new Error('WASM not initialized');
  return wasmModule;
}
