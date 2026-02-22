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

  // Quote balance adjustment (equity modifier)
  var quoteBalanceAdjustment = 0;

  // Last rendered state for clipboard
  var lastState = null;

  // Daily stats cache for heatmap
  var dailyStatsCache = null;

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
      dailyStatsCache = null;
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

    else if (msg.type === 'dailyStats') {
      var parsed = JSON.parse(msg.json);
      dailyStatsCache = parsed;
      renderHeatmap(parsed);
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
      if (Math.abs(quoteBalanceAdjustment) > 1e-12) msg.adjustment = quoteBalanceAdjustment;
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
    var displayName = file.name.length > 12 ? file.name.slice(0, 8) + '...' + file.name.slice(-4) : file.name;
    statusEl.textContent = displayName + ' (' + (file.size / 1024 / 1024).toFixed(1) + ' MB)';
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

  document.getElementById('qb-adjust-input').addEventListener('change', function() {
    var val = parseFloat(this.value);
    quoteBalanceAdjustment = isNaN(val) ? 0 : val;
    requestState(currentIndex);
  });

  document.getElementById('btn-qb-reset').addEventListener('click', function() {
    quoteBalanceAdjustment = 0;
    document.getElementById('qb-adjust-input').value = '';
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

  // ============================================================
  // HEATMAP
  // ============================================================

  document.getElementById('btn-heatmap').addEventListener('click', function() {
    var modal = document.getElementById('heatmap-modal');
    modal.classList.remove('hidden');
    if (dailyStatsCache) {
      renderHeatmap(dailyStatsCache);
    } else if (totalSnaps > 0) {
      worker.postMessage({ type: 'getDailyStats' });
    } else {
      document.getElementById('heatmap-body').innerHTML =
        '<div style="color:#718096;text-align:center;padding:20px">Upload and process files first</div>';
    }
  });

  document.getElementById('btn-heatmap-close').addEventListener('click', function() {
    document.getElementById('heatmap-modal').classList.add('hidden');
  });

  document.getElementById('heatmap-modal').addEventListener('click', function(e) {
    if (e.target === this) this.classList.add('hidden');
  });

  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
      document.getElementById('heatmap-modal').classList.add('hidden');
    }
  });

  function renderHeatmap(data) {
    var days = data.daily || data;
    var markets = data.markets || [];
    var body = document.getElementById('heatmap-body');
    if (!days || days.length === 0) {
      body.innerHTML = '<div style="color:#718096;text-align:center;padding:20px">No data</div>';
      return;
    }

    // Build day lookup
    var dayMap = {};
    for (var i = 0; i < days.length; i++) {
      dayMap[days[i].date] = days[i];
    }

    // Date range — pad to full weeks
    var firstDate = new Date(days[0].date + 'T00:00:00');
    var lastDate = new Date(days[days.length - 1].date + 'T00:00:00');

    var gridStart = new Date(firstDate);
    gridStart.setDate(gridStart.getDate() - gridStart.getDay());

    var gridEnd = new Date(lastDate);
    gridEnd.setDate(gridEnd.getDate() + (6 - gridEnd.getDay()));

    // Max events for intensity scaling
    var maxEvents = 0;
    for (var i = 0; i < days.length; i++) {
      if (days[i].eventCount > maxEvents) maxEvents = days[i].eventCount;
    }

    // Build cells
    var cells = [];
    var d = new Date(gridStart);
    while (d <= gridEnd) {
      var key = d.getFullYear() + '-' +
        String(d.getMonth() + 1).padStart(2, '0') + '-' +
        String(d.getDate()).padStart(2, '0');
      cells.push({ date: key, stats: dayMap[key] || null });
      d.setDate(d.getDate() + 1);
    }

    // Month labels
    var weekCount = Math.ceil(cells.length / 7);
    var monthLabels = [];
    var lastMonth = -1;
    var monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    for (var w = 0; w < weekCount; w++) {
      var cd = new Date(cells[w * 7].date + 'T00:00:00');
      var m = cd.getMonth();
      if (m !== lastMonth) {
        monthLabels.push({ col: w, label: monthNames[m] + ' ' + cd.getFullYear() });
        lastMonth = m;
      }
    }

    // Build HTML
    var html = '<div class="heatmap-container">';

    // Month labels — skip if too close to previous (< 5 weeks apart)
    html += '<div class="heatmap-months" style="width:' + (weekCount * 21) + 'px">';
    var lastLabelCol = -4;
    for (var i = 0; i < monthLabels.length; i++) {
      if (monthLabels[i].col - lastLabelCol < 4) continue;
      lastLabelCol = monthLabels[i].col;
      html += '<span style="left:' + (monthLabels[i].col * 21) + 'px">' +
        escapeHtml(monthLabels[i].label) + '</span>';
    }
    html += '</div>';

    // Wrapper: day labels + grid
    html += '<div class="heatmap-wrapper">';

    // Day-of-week labels
    var dayLabels = ['', 'Mon', '', 'Wed', '', 'Fri', ''];
    html += '<div class="heatmap-row-labels">';
    for (var i = 0; i < 7; i++) {
      html += '<span>' + dayLabels[i] + '</span>';
    }
    html += '</div>';

    // Grid cells
    html += '<div class="heatmap-grid">';
    for (var i = 0; i < cells.length; i++) {
      var level = 0;
      if (cells[i].stats) {
        var count = cells[i].stats.eventCount;
        if (maxEvents <= 4) {
          level = Math.min(count, 4);
        } else {
          var pct = count / maxEvents;
          if (pct <= 0.25) level = 1;
          else if (pct <= 0.50) level = 2;
          else if (pct <= 0.75) level = 3;
          else level = 4;
        }
      }
      html += '<div class="heatmap-cell heatmap-' + level +
        '" data-hi="' + i + '"></div>';
    }
    html += '</div></div>';

    // Legend
    html += '<div class="heatmap-legend">' +
      '<span>Less</span>' +
      '<div class="heatmap-cell heatmap-0"></div>' +
      '<div class="heatmap-cell heatmap-1"></div>' +
      '<div class="heatmap-cell heatmap-2"></div>' +
      '<div class="heatmap-cell heatmap-3"></div>' +
      '<div class="heatmap-cell heatmap-4"></div>' +
      '<span>More</span></div>';

    html += '</div>'; // end heatmap-container

    // Summary stats
    var totalPnl = 0, totalFees = 0, totalVol = 0, totalFunding = 0, totalTrades = 0;
    var bestDay = null, worstDay = null, busiestDay = null;
    var bestPnl = -Infinity, worstPnl = Infinity, maxAct = 0;
    for (var i = 0; i < days.length; i++) {
      var ds = days[i];
      totalPnl += ds.totalRealizedPnl;
      totalFees += ds.totalFees;
      totalVol += ds.totalVolume;
      totalFunding += ds.totalFundingPayments;
      totalTrades += ds.fillCount;
      if (ds.totalRealizedPnl > bestPnl) { bestPnl = ds.totalRealizedPnl; bestDay = ds.date; }
      if (ds.totalRealizedPnl < worstPnl) { worstPnl = ds.totalRealizedPnl; worstDay = ds.date; }
      if (ds.eventCount > maxAct) { maxAct = ds.eventCount; busiestDay = ds.date; }
    }

    html += '<div class="heatmap-summary">';
    html += statBox('Total Trades', totalTrades, '');
    html += statBox('Total rPnL', fmt(totalPnl), pnlClass(totalPnl));
    html += statBox('Total Fees', fmt(totalFees), 'pnl-neg');
    html += statBox('Total Volume', fmt(totalVol), '');
    html += statBox('Total Funding', fmt(totalFunding), pnlClass(totalFunding));
    html += statBox('Best Day', bestDay + ' (' + fmt(bestPnl) + ')', 'pnl-pos');
    html += statBox('Worst Day', worstDay + ' (' + fmt(worstPnl) + ')', 'pnl-neg');
    html += statBox('Busiest Day', busiestDay + ' (' + maxAct + ')', '');
    html += statBox('Active Days', days.length + ' days', '');
    html += '</div>';

    // Per-market breakdown table
    if (markets.length > 0) {
      html += '<div style="margin-top:16px;padding-top:16px;border-top:1px solid #2D3748">';
      html += '<h3 style="font-size:13px;color:#A0AEC0;margin-bottom:8px">Per Market</h3>';
      html += '<table class="log-table" style="font-size:12px">';
      html += '<thead><tr><th>Market</th><th>Trades</th><th>Volume</th><th>Fees</th><th>rPnL</th><th>Funding</th><th>Fund. Count</th></tr></thead>';
      html += '<tbody>';
      for (var mi = 0; mi < markets.length; mi++) {
        var mk = markets[mi];
        html += '<tr>';
        html += '<td>' + escapeHtml(mk.market) + '</td>';
        html += '<td class="num">' + mk.fillCount + '</td>';
        html += '<td class="num">' + fmt(mk.totalVolume) + '</td>';
        html += '<td class="num">' + fmt(mk.totalFees) + '</td>';
        html += '<td class="num ' + pnlClass(mk.totalRealizedPnl) + '">' + fmt(mk.totalRealizedPnl) + '</td>';
        html += '<td class="num ' + pnlClass(mk.totalFundingPayments) + '">' + fmt(mk.totalFundingPayments) + '</td>';
        html += '<td class="num">' + mk.fundingCount + '</td>';
        html += '</tr>';
      }
      html += '</tbody></table></div>';
    }

    // Tooltip element
    html += '<div class="heatmap-tooltip" id="heatmap-tooltip"></div>';

    body.innerHTML = html;

    // Store cells for handlers
    var heatmapCells = cells;
    var tooltip = document.getElementById('heatmap-tooltip');

    // Hover tooltip
    body.addEventListener('mouseover', function(e) {
      var cell = e.target.closest('[data-hi]');
      if (!cell) { tooltip.classList.remove('visible'); return; }
      var idx = parseInt(cell.dataset.hi, 10);
      var cd = heatmapCells[idx];
      if (!cd) return;

      var th = '<div class="tt-date">' + escapeHtml(cd.date) + '</div>';
      if (cd.stats) {
        var s = cd.stats;
        th += ttRow('Trades', s.fillCount);
        th += ttRow('Deposits', s.depositCount + ' (' + fmt(s.depositAmount) + ')');
        th += ttRow('Withdrawals', s.withdrawalCount + ' (' + fmt(s.withdrawalAmount) + ')');
        th += ttRow('Funding', s.fundingCount);
        th += ttRow('Volume', fmt(s.totalVolume));
        th += ttRow('Fees', fmt(s.totalFees));
        th += ttRow('rPnL', fmt(s.totalRealizedPnl));
        th += ttRow('Funding PnL', fmt(s.totalFundingPayments));
        th += ttRow('EOD Equity', fmt(s.endEquity));
        th += ttRow('EOD QuoteBal', fmt(s.endQuoteBalance));
      } else {
        th += '<div style="color:#718096">No activity</div>';
      }
      tooltip.innerHTML = th;
      tooltip.classList.add('visible');

      var rect = cell.getBoundingClientRect();
      tooltip.style.left = (rect.right + 8) + 'px';
      tooltip.style.top = (rect.top - 4) + 'px';

      // Keep in viewport
      var tr = tooltip.getBoundingClientRect();
      if (tr.right > window.innerWidth - 8) {
        tooltip.style.left = (rect.left - tr.width - 8) + 'px';
      }
      if (tr.bottom > window.innerHeight - 8) {
        tooltip.style.top = (window.innerHeight - tr.height - 8) + 'px';
      }
    });

    body.addEventListener('mouseout', function(e) {
      if (!e.target.closest('[data-hi]')) {
        tooltip.classList.remove('visible');
      }
    });

    // Click to copy
    body.addEventListener('click', function(e) {
      var cell = e.target.closest('[data-hi]');
      if (!cell) return;
      var idx = parseInt(cell.dataset.hi, 10);
      var cd = heatmapCells[idx];
      if (!cd || !cd.stats) return;

      var json = JSON.stringify(cd.stats, null, 2);
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(json).catch(function() {});
      } else {
        var ta = document.createElement('textarea');
        ta.value = json;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
      }
      cell.style.outline = '2px solid #68D391';
      setTimeout(function() { cell.style.outline = ''; }, 500);
    });
  }

  function statBox(label, value, cls) {
    return '<div class="heatmap-stat">' +
      '<div class="stat-value ' + (cls || '') + '">' + escapeHtml(String(value)) + '</div>' +
      '<div class="stat-label">' + escapeHtml(label) + '</div></div>';
  }

  function ttRow(label, value) {
    return '<div class="tt-row"><span class="tt-label">' + escapeHtml(String(label)) +
      '</span><span>' + escapeHtml(String(value)) + '</span></div>';
  }

})();
