(function() {
  'use strict';

  var worker = new Worker('./worker.js', { type: 'module' });
  var workerReady = false;
  var fillsUploaded = false;
  var depositsUploaded = false;
  var fundingUploaded = false;
  var totalSnaps = 0;
  var currentIndex = 0;
  var currentEnv = 'dev';

  // Pagination
  var SLIDER_PAGE_SIZE = 1000;
  var LOG_PAGE_SIZE = 200;
  var sliderPage = 0;
  var logPage = 0;

  // Debounce
  var stateRafId = null;

  // Custom index price overrides: { market: price }
  var priceOverrides = {};

  // Last rendered state for clipboard
  var lastState = null;

  // ============================================================
  // HELPERS
  // ============================================================

  function fmt(v) {
    var s = v.toFixed(10);
    var dot = s.indexOf('.');
    return s.slice(0, dot + 9);
  }

  function pnlClass(v) {
    if (v > 1e-12) return 'pnl-pos';
    if (v < -1e-12) return 'pnl-neg';
    return '';
  }

  function shortTime(ts) {
    if (!ts) return '';
    var d = new Date(ts);
    if (isNaN(d.getTime())) return ts.slice(0, 19);
    var mm = String(d.getMonth() + 1).padStart(2, '0');
    var dd = String(d.getDate()).padStart(2, '0');
    var hh = String(d.getHours()).padStart(2, '0');
    var mi = String(d.getMinutes()).padStart(2, '0');
    var ss = String(d.getSeconds()).padStart(2, '0');
    return mm + '-' + dd + ' ' + hh + ':' + mi + ':' + ss;
  }

  function escapeHtml(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  // ============================================================
  // PAGINATION HELPERS
  // ============================================================

  function totalSliderPages() { return Math.ceil(totalSnaps / SLIDER_PAGE_SIZE); }
  function sliderPageStart() { return sliderPage * SLIDER_PAGE_SIZE; }
  function sliderPageEnd() { return Math.min((sliderPage + 1) * SLIDER_PAGE_SIZE - 1, totalSnaps - 1); }
  function totalLogPages() { return Math.max(1, Math.ceil((totalSnaps - 1) / LOG_PAGE_SIZE)); }
  function logPageStart() { return logPage * LOG_PAGE_SIZE + 1; }
  function logPageEnd() { return Math.min((logPage + 1) * LOG_PAGE_SIZE, totalSnaps - 1); }
  function pageForIndex(index) { return Math.floor(index / SLIDER_PAGE_SIZE); }
  function logPageForIndex(index) { return index < 1 ? 0 : Math.floor((index - 1) / LOG_PAGE_SIZE); }

  // ============================================================
  // WORKER COMMUNICATION
  // ============================================================

  worker.onmessage = function(e) {
    var msg = e.data;

    if (msg.type === 'ready') {
      workerReady = true;
      tryProcess();
    }

    else if (msg.type === 'fileInfo') {
      var statusEl = document.getElementById(msg.fileType + '-status');
      var current = statusEl.textContent;
      // Append row count to existing filename
      var nameEnd = current.indexOf(' (');
      var name = nameEnd > 0 ? current.slice(0, nameEnd) : current;
      statusEl.textContent = name + ' (' + msg.rows + ' rows)';
    }

    else if (msg.type === 'processed') {
      totalSnaps = msg.totalSnapshots;
      console.log('WASM processed ' + (totalSnaps - 1) + ' events in ' + msg.elapsed + 'ms');

      currentIndex = totalSnaps - 1;
      sliderPage = pageForIndex(currentIndex);
      logPage = logPageForIndex(currentIndex);

      document.getElementById('content-section').classList.remove('hidden');
      document.getElementById('log-section').classList.remove('hidden');
      document.getElementById('page-row').classList.toggle('hidden', totalSliderPages() <= 1);

      updateSlider();
      requestLogPage();
      requestState(currentIndex);
    }

    else if (msg.type === 'state') {
      if (msg.index !== currentIndex) return; // skip stale
      var state = JSON.parse(msg.json);
      lastState = state;
      renderEventInfo(state.event);
      renderWalletPanel(state.metrics);
      renderPositionsTable(state.positions);
      highlightLogRow();
    }

    else if (msg.type === 'logPage') {
      var entries = JSON.parse(msg.json);
      renderLogBody(entries);
      renderLogPagination();
      highlightLogRow();
    }
  };

  function requestState(index) {
    document.getElementById('slider-label').textContent = index + ' / ' + (totalSnaps - 1);
    document.getElementById('timeline-slider').value = index;

    if (stateRafId) cancelAnimationFrame(stateRafId);
    stateRafId = requestAnimationFrame(function() {
      stateRafId = null;
      var msg = { type: 'getState', index: currentIndex };
      var keys = Object.keys(priceOverrides);
      if (keys.length > 0) msg.pricesJson = JSON.stringify(priceOverrides);
      worker.postMessage(msg);
    });
  }

  function requestLogPage() {
    worker.postMessage({ type: 'getLogPage', start: logPageStart(), end: logPageEnd() });
  }

  function tryProcess() {
    if (!fillsUploaded || !depositsUploaded || !workerReady) return;

    document.getElementById('timeline-section').classList.remove('hidden');
    document.getElementById('event-info').innerHTML =
      '<span style="color:#718096">Processing...</span>';

    worker.postMessage({
      type: 'process',
      configsJson: JSON.stringify(MARKET_CONFIGS[currentEnv]),
    });
  }

  // ============================================================
  // RENDERING
  // ============================================================

  function updateSlider() {
    var slider = document.getElementById('timeline-slider');
    var start = sliderPageStart();
    var end = sliderPageEnd();
    slider.min = start;
    slider.max = end;
    slider.value = Math.max(start, Math.min(end, currentIndex));

    var tp = totalSliderPages();
    if (tp > 1) {
      document.getElementById('page-label').textContent =
        'Page ' + (sliderPage + 1) + ' / ' + tp + ' (' + start + '-' + end + ')';
      document.getElementById('btn-page-prev').disabled = sliderPage <= 0;
      document.getElementById('btn-page-next').disabled = sliderPage >= tp - 1;

      var show10 = tp > 10;
      document.getElementById('btn-page-back10').classList.toggle('hidden', !show10);
      document.getElementById('btn-page-fwd10').classList.toggle('hidden', !show10);
      if (show10) {
        document.getElementById('btn-page-back10').disabled = sliderPage < 10;
        document.getElementById('btn-page-fwd10').disabled = sliderPage + 10 >= tp;
      }
    }
  }

  function renderEventInfo(ev) {
    var el = document.getElementById('event-info');
    if (!ev) {
      el.innerHTML = '<span style="color:#718096">Initial state (before any events)</span>';
      return;
    }

    if (ev.kind === 'deposit') {
      var badge = ev.type === 'deposit' ? 'badge-deposit' : 'badge-withdrawal';
      el.innerHTML =
        '<span class="badge ' + badge + '">' + escapeHtml(ev.type) + '</span> ' +
        '<strong>' + fmt(ev.amount) + '</strong> USDC &mdash; ' +
        '<span style="color:#718096">' + shortTime(ev.time) + '</span>';
    } else if (ev.kind === 'funding') {
      var pnl = ev.quantity;
      el.innerHTML =
        '<span class="badge badge-funding">funding</span> ' +
        escapeHtml(ev.market) + ' <strong class="' + pnlClass(pnl) + '">' + fmt(pnl) + '</strong> USDC' +
        (ev.funding_rate ? ' (rate: ' + ev.funding_rate + ')' : '') +
        ' &mdash; <span style="color:#718096">' + shortTime(ev.time) + '</span>';
    } else {
      var badge = ev.type === 'liquidation' ? 'badge-liquidation' : 'badge-fill';
      var sideHtml = ev.side === 'buy'
        ? '<span class="pnl-pos">' + escapeHtml(ev.side) + '</span>'
        : '<span class="pnl-neg">' + escapeHtml(ev.side) + '</span>';
      el.innerHTML =
        '<span class="badge ' + badge + '">' + escapeHtml(ev.type) + '</span> ' +
        sideHtml + ' <strong>' + fmt(ev.quantity) + '</strong> ' +
        escapeHtml(ev.market) + ' @ ' + fmt(ev.price) +
        ' &mdash; <span style="color:#718096">' + shortTime(ev.time) + '</span>';
    }
  }

  function buildClipboardJson() {
    if (!lastState) return '{}';
    var m = lastState.metrics;
    var out = {
      equity: fmt(m.equity),
      quoteBalance: fmt(m.quoteBalance),
      unrealizedPnL: fmt(m.totalUnrealizedPnL),
      realizedPnL: fmt(m.totalRealizedPnL),
      totalFunding: fmt(m.totalFunding),
      totalFees: fmt(m.totalFees),
      freeCollateral: fmt(m.freeCollateral),
      leverage: fmt(m.leverage),
      marginRatio: fmt(m.marginRatio),
      totalIMR: fmt(m.totalIMR),
      totalMMR: fmt(m.totalMMR),
      positions: (lastState.positions || []).map(function(p) {
        return {
          market: p.market,
          quantity: fmt(p.quantity),
          entryPrice: fmt(p.entryPrice),
          indexPrice: fmt(p.lastIndexPrice),
          liquidationPrice: fmt(p.liquidationPrice),
          value: fmt(p.notional),
          unrealizedPnL: fmt(p.unrealizedPnL),
          realizedPnL: fmt(p.cumulativeRealizedPnL),
          totalFunding: fmt(p.cumulativeFunding || 0),
          initialMargin: fmt(p.imr),
          maintenanceMargin: fmt(p.mmr),
        };
      }),
    };
    return JSON.stringify([out], null, 4);
  }

  function renderWalletPanel(m) {
    var rows = [
      ['Equity', fmt(m.equity)],
      ['Quote Balance', fmt(m.quoteBalance)],
      ['Unrealized PnL', fmt(m.totalUnrealizedPnL), pnlClass(m.totalUnrealizedPnL)],
      ['Realized PnL', fmt(m.totalRealizedPnL), pnlClass(m.totalRealizedPnL)],
      ['Funding', fmt(m.totalFunding), pnlClass(m.totalFunding)],
      ['Fees', fmt(m.totalFees), 'pnl-neg'],
      ['Free Collateral', fmt(m.freeCollateral)],
      ['Leverage', fmt(m.leverage) + 'x'],
      ['Margin Ratio', fmt(m.marginRatio)],
      ['Total IMR', fmt(m.totalIMR)],
      ['Total MMR', fmt(m.totalMMR)],
    ];

    var html = '';
    for (var i = 0; i < rows.length; i++) {
      var cls = rows[i][2] || '';
      html += '<tr><td>' + rows[i][0] + '</td><td class="' + cls + '">' + rows[i][1] + '</td></tr>';
    }
    document.getElementById('wallet-table').innerHTML = html;
  }

  function renderPositionsTable(positions) {
    var el = document.getElementById('positions-content');
    if (!positions || positions.length === 0) {
      el.innerHTML = '<div class="no-positions">No open positions</div>';
      return;
    }

    var html = '<table class="pos-table"><thead><tr>' +
      '<th>Market</th><th>Side</th><th>Qty</th><th>Entry</th><th>Index</th>' +
      '<th>Notional</th><th>uPnL</th><th>rPnL</th><th>Liq Price</th><th>IMR</th><th>MMR</th>' +
      '</tr></thead><tbody>';

    for (var i = 0; i < positions.length; i++) {
      var pos = positions[i];
      var side = pos.quantity > 0 ? 'LONG' : 'SHORT';
      var sideClass = pos.quantity > 0 ? 'pnl-pos' : 'pnl-neg';

      html += '<tr>' +
        '<td>' + escapeHtml(pos.market) + '</td>' +
        '<td class="' + sideClass + '">' + side + '</td>' +
        '<td class="num">' + fmt(pos.quantity) + '</td>' +
        '<td class="num">' + fmt(pos.entryPrice) + '</td>' +
        '<td class="num"><input type="text" class="idx-price-input" data-market="' + escapeHtml(pos.market) + '" value="' + fmt(pos.lastIndexPrice) + '"></td>' +
        '<td class="num">' + fmt(pos.notional) + '</td>' +
        '<td class="num ' + pnlClass(pos.unrealizedPnL) + '">' + fmt(pos.unrealizedPnL) + '</td>' +
        '<td class="num ' + pnlClass(pos.cumulativeRealizedPnL + (pos.cumulativeFunding || 0)) + '">' + fmt(pos.cumulativeRealizedPnL + (pos.cumulativeFunding || 0)) + '</td>' +
        '<td class="num">' + fmt(pos.liquidationPrice) + '</td>' +
        '<td class="num">' + fmt(pos.imr) + '</td>' +
        '<td class="num">' + fmt(pos.mmr) + '</td>' +
        '</tr>';
    }

    html += '</tbody></table>';
    el.innerHTML = html;
  }

  function renderLogBody(entries) {
    document.getElementById('log-head').innerHTML =
      '<tr><th>#</th><th>Time</th><th>Type</th><th>Market</th><th>Side</th>' +
      '<th>Qty</th><th>Price</th><th>Fee</th><th>rPnL</th>' +
      '<th>QuoteBal</th><th>Equity</th></tr>';

    var html = '';
    for (var i = 0; i < entries.length; i++) {
      var e = entries[i];
      var isFill = e.kind === 'fill';
      var isFunding = e.kind === 'funding';
      var badge = isFunding ? 'badge-funding'
        : isFill
          ? (e.type === 'liquidation' ? 'badge-liquidation' : 'badge-fill')
          : (e.type === 'deposit' ? 'badge-deposit' : 'badge-withdrawal');

      html += '<tr id="log-row-' + e.index + '" class="clickable" data-index="' + e.index + '">' +
        '<td>' + e.index + '</td>' +
        '<td>' + escapeHtml(shortTime(e.time)) + '</td>' +
        '<td><span class="badge ' + badge + '">' + escapeHtml(e.type) + '</span></td>' +
        '<td>' + escapeHtml(e.market) + '</td>' +
        '<td>' + escapeHtml(e.side) + '</td>' +
        '<td class="num">' + fmt(e.qty) + '</td>' +
        '<td class="num">' + ((isFill || isFunding) ? fmt(e.price) : '') + '</td>' +
        '<td class="num">' + (isFill ? fmt(e.fee) : '') + '</td>' +
        '<td class="num">' + ((isFill || isFunding) ? fmt(e.rpnl) : '') + '</td>' +
        '<td class="num">' + fmt(e.quoteBal) + '</td>' +
        '<td class="num">' + fmt(e.equity) + '</td>' +
        '</tr>';
    }
    document.getElementById('log-body').innerHTML = html;
  }

  function renderLogPagination() {
    var el = document.getElementById('log-pagination');
    var total = totalLogPages();
    if (total <= 1) { el.innerHTML = ''; return; }
    var start = logPageStart();
    var end = logPageEnd();
    var eventCount = totalSnaps - 1;
    el.innerHTML =
      '<button id="btn-log-prev"' + (logPage <= 0 ? ' disabled' : '') + '>&laquo;</button>' +
      '<span>' + start + '-' + end + ' of ' + eventCount + '</span>' +
      '<button id="btn-log-next"' + (logPage >= total - 1 ? ' disabled' : '') + '>&raquo;</button>';
  }

  function highlightLogRow() {
    var prev = document.querySelector('.log-table tr.active');
    if (prev) prev.classList.remove('active');
    if (currentIndex > 0) {
      var row = document.getElementById('log-row-' + currentIndex);
      if (row) {
        row.classList.add('active');
        row.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
      }
    }
  }

  // ============================================================
  // EVENT HANDLERS
  // ============================================================

  function onFileUpload(fileInput, type) {
    var file = fileInput.files[0];
    if (!file) return;

    var statusEl = document.getElementById(type + '-status');
    statusEl.textContent = file.name + ' (' + (file.size / 1024 / 1024).toFixed(1) + ' MB)';
    statusEl.classList.add('loaded');

    // Read as ArrayBuffer — no string parsing on main thread
    var reader = new FileReader();
    reader.onload = function(e) {
      var buffer = e.target.result;

      if (type === 'fills') fillsUploaded = true;
      else if (type === 'deposits') depositsUploaded = true;
      else fundingUploaded = true;

      // Transfer buffer to worker (zero-copy, instant)
      worker.postMessage({ type: 'upload', fileType: type, buffer: buffer }, [buffer]);

      tryProcess();
    };
    reader.readAsArrayBuffer(file);
  }

  function goTo(index) {
    if (totalSnaps === 0) return;
    if (index < 0) index = 0;
    if (index >= totalSnaps) index = totalSnaps - 1;
    priceOverrides = {};
    currentIndex = index;

    var targetPage = pageForIndex(currentIndex);
    if (targetPage !== sliderPage) {
      sliderPage = targetPage;
      updateSlider();
    }

    var targetLogPage = logPageForIndex(currentIndex);
    if (targetLogPage !== logPage) {
      logPage = targetLogPage;
      requestLogPage();
    }

    requestState(currentIndex);
  }

  // ============================================================
  // INIT
  // ============================================================

  document.getElementById('fills-file').addEventListener('change', function() {
    onFileUpload(this, 'fills');
  });
  document.getElementById('deposits-file').addEventListener('change', function() {
    onFileUpload(this, 'deposits');
  });
  document.getElementById('funding-file').addEventListener('change', function() {
    onFileUpload(this, 'funding');
  });

  document.getElementById('env-select').addEventListener('change', function() {
    currentEnv = this.value;
    tryProcess();
  });

  document.getElementById('timeline-slider').addEventListener('input', function() {
    goTo(parseInt(this.value, 10));
  });

  document.getElementById('btn-prev').addEventListener('click', function() {
    goTo(currentIndex - 1);
  });
  document.getElementById('btn-next').addEventListener('click', function() {
    goTo(currentIndex + 1);
  });

  document.addEventListener('keydown', function(e) {
    if (totalSnaps === 0) return;
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT') return;
    if (e.key === 'ArrowLeft') { e.preventDefault(); goTo(currentIndex - 1); }
    else if (e.key === 'ArrowRight') { e.preventDefault(); goTo(currentIndex + 1); }
    else if (e.key === 'Home') { e.preventDefault(); goTo(0); }
    else if (e.key === 'End') { e.preventDefault(); goTo(totalSnaps - 1); }
  });

  document.getElementById('btn-copy-state').addEventListener('click', function() {
    var json = buildClipboardJson();
    var btn = document.getElementById('btn-copy-state');
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(json).then(function() {
        btn.textContent = 'Copied!';
        setTimeout(function() { btn.textContent = 'Copy'; }, 1500);
      }, function() { fallbackCopy(json, btn); });
    } else {
      fallbackCopy(json, btn);
    }
  });

  function fallbackCopy(text, btn) {
    var ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
    btn.textContent = 'Copied!';
    setTimeout(function() { btn.textContent = 'Copy'; }, 1500);
  }

  document.getElementById('positions-content').addEventListener('change', function(e) {
    var input = e.target.closest('.idx-price-input');
    if (!input) return;
    var market = input.dataset.market;
    var val = parseFloat(input.value);
    if (isNaN(val) || val <= 0) {
      delete priceOverrides[market];
    } else {
      priceOverrides[market] = val;
    }
    requestState(currentIndex);
  });

  document.getElementById('log-body').addEventListener('click', function(e) {
    var row = e.target.closest('tr[data-index]');
    if (row) goTo(parseInt(row.dataset.index, 10));
  });

  function jumpPages(delta) {
    var target = sliderPage + delta;
    if (target < 0) target = 0;
    if (target >= totalSliderPages()) target = totalSliderPages() - 1;
    if (target === sliderPage) return;
    sliderPage = target;
    currentIndex = delta > 0 ? sliderPageStart() : sliderPageEnd();
    updateSlider();
    requestLogPage();
    requestState(currentIndex);
  }

  document.getElementById('btn-page-back10').addEventListener('click', function() { jumpPages(-10); });
  document.getElementById('btn-page-prev').addEventListener('click', function() { jumpPages(-1); });
  document.getElementById('btn-page-next').addEventListener('click', function() { jumpPages(1); });
  document.getElementById('btn-page-fwd10').addEventListener('click', function() { jumpPages(10); });

  document.getElementById('log-pagination').addEventListener('click', function(e) {
    var btn = e.target.closest('button');
    if (!btn || btn.disabled) return;
    if (btn.id === 'btn-log-prev' && logPage > 0) {
      logPage--;
      requestLogPage();
    } else if (btn.id === 'btn-log-next' && logPage < totalLogPages() - 1) {
      logPage++;
      requestLogPage();
    }
  });

})();
