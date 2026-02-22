import init, { Engine } from './wasm/pkg/backtrack_wasm.js';

let engine = null;
let storedFillsBytes = null;
let storedDepositsBytes = null;
let storedFundingBytes = null;

async function boot() {
  await init();
  engine = new Engine();
  postMessage({ type: 'ready' });
}

onmessage = function(e) {
  const msg = e.data;

  if (msg.type === 'upload') {
    const bytes = new Uint8Array(msg.buffer);
    if (msg.fileType === 'fills') {
      storedFillsBytes = bytes;
    } else if (msg.fileType === 'deposits') {
      storedDepositsBytes = bytes;
    } else {
      storedFundingBytes = bytes;
    }
    // Count rows (newlines) in the worker — not main thread
    let rows = 0;
    for (let i = 0; i < bytes.length; i++) {
      if (bytes[i] === 10) rows++;
    }
    postMessage({ type: 'fileInfo', fileType: msg.fileType, rows: rows });
  }

  else if (msg.type === 'process') {
    if (!storedFillsBytes || !storedDepositsBytes) return;
    const t0 = performance.now();
    const fundingBytes = storedFundingBytes || new Uint8Array(0);
    engine.process(storedFillsBytes, storedDepositsBytes, fundingBytes, msg.configsJson);
    const total = engine.total_snapshots();
    const elapsed = (performance.now() - t0).toFixed(0);
    postMessage({ type: 'processed', totalSnapshots: total, elapsed: elapsed });
  }

  else if (msg.type === 'getState') {
    const json = msg.pricesJson
      ? engine.get_state_json_with_prices(msg.index, msg.pricesJson)
      : engine.get_state_json(msg.index);
    postMessage({ type: 'state', json: json, index: msg.index });
  }

  else if (msg.type === 'getLogPage') {
    const json = engine.get_log_page_json(msg.start, msg.end);
    postMessage({ type: 'logPage', json: json });
  }
};

boot();
