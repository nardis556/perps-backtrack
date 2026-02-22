use std::collections::HashMap;
use wasm_bindgen::prelude::*;
use serde::{Serialize, Deserialize};

const CHECKPOINT_INTERVAL: usize = 500;
const SCALE: i128 = 100_000_000; // 10^8
const SCALE_F64: f64 = 100_000_000.0;
const SCALE_SQ: f64 = SCALE_F64 * SCALE_F64; // 10^16
const SCALE_CU: f64 = SCALE_F64 * SCALE_F64 * SCALE_F64; // 10^24
const EPS: i128 = 1; // smallest unit: 0.00000001

// ============================================================
// Fixed-point helpers (all i128)
// ============================================================

fn parse_fixed(s: &str) -> i128 {
    let s = s.trim();
    if s.is_empty() { return 0; }
    let negative = s.starts_with('-');
    let s = if negative { &s[1..] } else { s };

    let (int_part, dec_part) = match s.find('.') {
        Some(dot) => (&s[..dot], &s[dot + 1..]),
        None => (s, ""),
    };

    let int_val: i128 = int_part.parse().unwrap_or(0);

    let dec_val: i128 = if dec_part.is_empty() {
        0
    } else if dec_part.len() <= 8 {
        let mut buf = String::from(dec_part);
        while buf.len() < 8 { buf.push('0'); }
        buf.parse().unwrap_or(0)
    } else {
        dec_part[..8].parse().unwrap_or(0)
    };

    let result = int_val * SCALE + dec_val;
    if negative { -result } else { result }
}

// Convert SCALE (10^8) to f64
#[inline]
fn to_f64_s1(v: i128) -> f64 { v as f64 / SCALE_F64 }

// Convert SCALE^2 (10^16) to f64
#[inline]
fn to_f64_s2(v: i128) -> f64 { v as f64 / SCALE_SQ }

// Convert SCALE^3 (10^24) to f64
#[inline]
fn to_f64_s3(v: i128) -> f64 { v as f64 / SCALE_CU }

// ============================================================
// Internal types
// All values stored as i128. Prices, quantities, fees etc are
// at SCALE (10^8). total_notional is at SCALE^2 (10^16) —
// the raw product of qty * price with no division.
// ============================================================

#[derive(Clone)]
enum Event {
    Deposit {
        timestamp: String,
        dep_type: String,
        amount: i128, // SCALE
    },
    Fill {
        time: String,
        market: String,
        side: String,
        fill_type: String,
        price: i128,          // SCALE
        index_price: i128,    // SCALE
        quantity: i128,        // SCALE
        quote_quantity: i128,  // SCALE
        fee: i128,             // SCALE
        realized_pnl: i128,   // SCALE
    },
    Funding {
        time: String,
        market: String,
        payment_quantity: i128, // SCALE
        position_quantity: i128,
        funding_rate: i128,
        index_price: i128,     // SCALE
    },
}

impl Event {
    fn sort_key(&self) -> &str {
        match self {
            Event::Deposit { timestamp, .. } => timestamp,
            Event::Fill { time, .. } => time,
            Event::Funding { time, .. } => time,
        }
    }
}

#[derive(Clone)]
struct Position {
    market: String,
    quantity: i128,                  // SCALE
    total_notional: i128,            // SCALE^2 (raw qty * price, no division)
    cumulative_realized_pnl: i128,   // SCALE
    cumulative_fees: i128,           // SCALE
    cumulative_funding: i128,        // SCALE
    last_index_price: i128,          // SCALE
}

impl Position {
    fn new(market: &str) -> Self {
        Position {
            market: market.to_string(),
            quantity: 0,
            total_notional: 0,
            cumulative_realized_pnl: 0,
            cumulative_fees: 0,
            cumulative_funding: 0,
            last_index_price: 0,
        }
    }
}

struct MarketConfig {
    initial_margin_fraction: i128,     // SCALE
    maintenance_margin_fraction: i128, // SCALE
    base_position_size: i128,          // SCALE
    incremental_position_size: i128,   // SCALE
    incremental_initial_margin_fraction: i128, // SCALE
}

struct Checkpoint {
    quote_balance: i128,
    positions: HashMap<String, Position>,
}

// ============================================================
// Config input (from JS JSON)
// ============================================================

#[derive(Deserialize)]
struct MarketConfigInput {
    market: String,
    #[serde(rename = "initialMarginFraction")]
    initial_margin_fraction: String,
    #[serde(rename = "maintenanceMarginFraction")]
    maintenance_margin_fraction: String,
    #[serde(rename = "basePositionSize")]
    base_position_size: String,
    #[serde(rename = "incrementalPositionSize")]
    incremental_position_size: String,
    #[serde(rename = "incrementalInitialMarginFraction")]
    incremental_initial_margin_fraction: String,
}

// ============================================================
// JSON output types (f64 only at serialization boundary)
// ============================================================

#[derive(Serialize)]
struct LogEntry {
    index: usize,
    time: String,
    kind: String,
    #[serde(rename = "type")]
    event_type: String,
    market: String,
    side: String,
    qty: f64,
    price: f64,
    fee: f64,
    rpnl: f64,
    #[serde(rename = "quoteBal")]
    quote_bal: f64,
    equity: f64,
}

#[derive(Serialize)]
struct EventOut {
    kind: String,
    time: String,
    #[serde(rename = "type")]
    event_type: String,
    market: String,
    side: String,
    quantity: f64,
    price: f64,
    amount: f64,
    #[serde(rename = "fundingRate")]
    funding_rate: f64,
}

#[derive(Serialize)]
struct PositionOut {
    market: String,
    quantity: f64,
    #[serde(rename = "entryPrice")]
    entry_price: f64,
    #[serde(rename = "lastIndexPrice")]
    last_index_price: f64,
    notional: f64,
    #[serde(rename = "unrealizedPnL")]
    unrealized_pnl: f64,
    #[serde(rename = "cumulativeRealizedPnL")]
    cumulative_realized_pnl: f64,
    #[serde(rename = "cumulativeFunding")]
    cumulative_funding: f64,
    #[serde(rename = "liquidationPrice")]
    liquidation_price: f64,
    imr: f64,
    mmr: f64,
}

#[derive(Serialize)]
struct MetricsOut {
    equity: f64,
    #[serde(rename = "quoteBalance")]
    quote_balance: f64,
    #[serde(rename = "totalUnrealizedPnL")]
    total_unrealized_pnl: f64,
    #[serde(rename = "totalIMR")]
    total_imr: f64,
    #[serde(rename = "totalMMR")]
    total_mmr: f64,
    #[serde(rename = "freeCollateral")]
    free_collateral: f64,
    leverage: f64,
    #[serde(rename = "marginRatio")]
    margin_ratio: f64,
    #[serde(rename = "totalRealizedPnL")]
    total_realized_pnl: f64,
    #[serde(rename = "totalFunding")]
    total_funding: f64,
    #[serde(rename = "totalFees")]
    total_fees: f64,
    #[serde(rename = "openPositions")]
    open_positions: usize,
}

#[derive(Serialize)]
struct StateOut {
    event: Option<EventOut>,
    #[serde(rename = "quoteBalance")]
    quote_balance: f64,
    positions: Vec<PositionOut>,
    metrics: MetricsOut,
}

// ============================================================
// Engine
// ============================================================

#[wasm_bindgen]
pub struct Engine {
    events: Vec<Event>,
    ev_quote_bal: Vec<i128>,  // SCALE
    ev_equity: Vec<i128>,     // SCALE^2
    checkpoints: HashMap<usize, Checkpoint>,
    market_configs: HashMap<String, MarketConfig>,
    total_snapshots: usize,
}

// Private methods
impl Engine {
    fn parse_configs(&mut self, json: &str) {
        self.market_configs.clear();
        let inputs: Vec<MarketConfigInput> = match serde_json::from_str(json) {
            Ok(v) => v,
            Err(_) => return,
        };
        for input in inputs {
            self.market_configs.insert(input.market, MarketConfig {
                initial_margin_fraction: parse_fixed(&input.initial_margin_fraction),
                maintenance_margin_fraction: parse_fixed(&input.maintenance_margin_fraction),
                base_position_size: parse_fixed(&input.base_position_size),
                incremental_position_size: parse_fixed(&input.incremental_position_size),
                incremental_initial_margin_fraction: parse_fixed(&input.incremental_initial_margin_fraction),
            });
        }
    }

    fn apply_event(quote_balance: i128, positions: &mut HashMap<String, Position>, ev: &Event) -> i128 {
        match ev {
            Event::Deposit { dep_type, amount, .. } => {
                if dep_type == "deposit" {
                    quote_balance + amount
                } else {
                    quote_balance - amount
                }
            }
            Event::Fill { fill_type, side, price, index_price, quantity, quote_quantity, fee, realized_pnl, market, .. } => {
                if fill_type == "liquidation" {
                    for pos in positions.values_mut() {
                        pos.quantity = 0;
                        pos.total_notional = 0;
                        pos.cumulative_realized_pnl = 0;
                        pos.cumulative_fees = 0;
                        pos.cumulative_funding = 0;
                    }
                    0
                } else {
                    let mut qb = quote_balance;
                    if side == "buy" {
                        qb -= quote_quantity;
                    } else {
                        qb += quote_quantity;
                    }
                    qb -= fee;
                    Self::process_fill(positions, market, side, *price, *index_price, *quantity, *fee, *realized_pnl);
                    qb
                }
            }
            Event::Funding { market, payment_quantity, index_price, .. } => {
                let pos = positions.entry(market.clone()).or_insert_with(|| Position::new(market));
                pos.cumulative_funding += payment_quantity;
                pos.last_index_price = *index_price;
                quote_balance + payment_quantity
            }
        }
    }

    fn process_fill(
        positions: &mut HashMap<String, Position>,
        market: &str, side: &str, price: i128, index_price: i128,
        quantity: i128, fee: i128, realized_pnl: i128,
    ) {
        let pos = positions.entry(market.to_string()).or_insert_with(|| Position::new(market));

        let signed_fill_qty: i128 = if side == "buy" { quantity } else { -quantity };
        let old_qty = pos.quantity;
        let new_qty = old_qty + signed_fill_qty;

        let same_direction = old_qty.abs() < EPS
            || (old_qty > 0 && signed_fill_qty > 0)
            || (old_qty < 0 && signed_fill_qty < 0);

        let crosses_zero = old_qty.abs() >= EPS
            && ((old_qty > 0 && new_qty < 0) || (old_qty < 0 && new_qty > 0));

        if same_direction {
            if old_qty.abs() < EPS {
                // Fresh open — raw product, no division (SCALE^2)
                pos.total_notional = quantity * price;
                pos.cumulative_realized_pnl = 0;
                pos.cumulative_fees = 0;
                pos.cumulative_funding = 0;
            } else {
                // Add to position — raw product, no division (SCALE^2)
                pos.total_notional += quantity * price;
            }
            pos.quantity = new_qty;
        } else if crosses_zero {
            // closeAndOpen — new position with remainder
            pos.quantity = new_qty;
            pos.total_notional = new_qty.abs() * price; // SCALE^2
            pos.cumulative_realized_pnl = 0;
            pos.cumulative_fees = 0;
            pos.cumulative_funding = 0;
        } else {
            // Reducing position
            if new_qty.abs() < EPS {
                pos.quantity = 0;
                pos.total_notional = 0;
            } else {
                // Proportional notional reduction — single division, no intermediate entry_price
                let close_qty = quantity.min(old_qty.abs());
                let removed = pos.total_notional * close_qty / old_qty.abs();
                pos.total_notional -= removed;
                pos.quantity = new_qty;
            }
        }

        pos.cumulative_realized_pnl += realized_pnl;
        pos.cumulative_fees += fee;
        pos.last_index_price = index_price;
    }

    fn tiered_imf(&self, abs_qty: i128, mc: &MarketConfig) -> i128 {
        let imf = mc.initial_margin_fraction;
        if abs_qty <= mc.base_position_size {
            return imf;
        }
        let diff = abs_qty - mc.base_position_size;
        // ceil(diff / incremental_position_size) — both are SCALE, scales cancel
        let inc = mc.incremental_position_size;
        let steps = (diff + inc - 1) / inc;
        // steps is unscaled integer, incremental_imf is SCALE → result is SCALE
        imf + steps * mc.incremental_initial_margin_fraction
    }

    // All intermediate values tracked at their natural scale.
    // Only f64 conversion happens at output.
    fn compute_metrics(&self, quote_balance: i128, positions: &HashMap<String, Position>) -> MetricsOut {
        let mut index_notional_sum: i128 = 0;       // SCALE^2
        let mut total_abs_index_notional: i128 = 0;  // SCALE^2
        let mut total_unrealized_pnl: i128 = 0;      // SCALE^2
        let mut total_imr: i128 = 0;                  // SCALE^3
        let mut total_mmr: i128 = 0;                  // SCALE^3
        let mut total_realized_pnl: i128 = 0;         // SCALE
        let mut total_funding: i128 = 0;               // SCALE
        let mut total_fees: i128 = 0;                  // SCALE
        let mut open_count = 0usize;

        for pos in positions.values() {
            total_realized_pnl += pos.cumulative_realized_pnl;
            total_funding += pos.cumulative_funding;
            total_fees += pos.cumulative_fees;

            if pos.quantity.abs() < EPS { continue; }
            open_count += 1;

            let qty = pos.quantity;
            let abs_qty = qty.abs();
            let ip = pos.last_index_price;

            // qty * ip — no division (SCALE^2)
            index_notional_sum += qty * ip;
            total_abs_index_notional += abs_qty * ip;

            // Unrealized PnL directly from total_notional — no intermediate entry_price
            // uPnL = (indexPrice - entryPrice) * qty
            //       = indexPrice * qty - entryPrice * abs(qty) * sign(qty)
            //       = indexPrice * qty - total_notional * sign(qty)
            // All terms in SCALE^2
            let sign_qty: i128 = if qty > 0 { 1 } else { -1 };
            total_unrealized_pnl += ip * qty - pos.total_notional * sign_qty;

            if let Some(mc) = self.market_configs.get(&pos.market) {
                let imf = self.tiered_imf(abs_qty, mc);
                let mmf = mc.maintenance_margin_fraction;
                // imf * abs_qty * ip — no intermediate division (SCALE^3)
                total_imr += imf * abs_qty * ip;
                total_mmr += mmf * abs_qty * ip;
            }
        }

        // equity = quoteBalance + sum(qty * indexPrice)
        // Promote qb from SCALE to SCALE^2, then add SCALE^2 notional sum
        let equity_s2: i128 = quote_balance * SCALE + index_notional_sum;

        // free_collateral = equity - totalIMR
        // Promote equity from SCALE^2 to SCALE^3, then subtract SCALE^3 IMR
        let free_collateral_s3: i128 = equity_s2 * SCALE - total_imr;

        // leverage = totalAbsIndexNotional / equity (dimensionless)
        // Both SCALE^2 — ratio computed as f64
        let leverage_f64 = if total_abs_index_notional == 0 || equity_s2 == 0 {
            0.0
        } else {
            total_abs_index_notional as f64 / equity_s2 as f64
        };

        // margin_ratio = totalMMR / equity
        // mmr is SCALE^3, equity is SCALE^2 — ratio is SCALE, convert with to_f64_s1
        let margin_ratio_f64 = if equity_s2 == 0 {
            0.0
        } else {
            (total_mmr / equity_s2) as f64 / SCALE_F64
        };

        MetricsOut {
            equity: to_f64_s2(equity_s2),
            quote_balance: to_f64_s1(quote_balance),
            total_unrealized_pnl: to_f64_s2(total_unrealized_pnl),
            total_imr: to_f64_s3(total_imr),
            total_mmr: to_f64_s3(total_mmr),
            free_collateral: to_f64_s3(free_collateral_s3),
            leverage: leverage_f64,
            margin_ratio: margin_ratio_f64,
            total_realized_pnl: to_f64_s1(total_realized_pnl),
            total_funding: to_f64_s1(total_funding),
            total_fees: to_f64_s1(total_fees),
            open_positions: open_count,
        }
    }

    fn calculate_liquidation_price(&self, market: &str, quote_balance: i128, positions: &HashMap<String, Position>) -> i128 {
        let pos = match positions.get(market) {
            Some(p) if p.quantity.abs() >= EPS => p,
            _ => return 0,
        };

        let mc = match self.market_configs.get(market) {
            Some(c) => c,
            None => return 0,
        };

        let qty = pos.quantity;
        let abs_qty = qty.abs();
        let mmf = mc.maintenance_margin_fraction;

        // denom = abs(qty) * mmf - qty
        // abs_qty * mmf is SCALE^2, qty is SCALE — promote qty to SCALE^2
        let denom_s2: i128 = abs_qty * mmf - qty * SCALE;
        if denom_s2.abs() < EPS { return 0; }

        // num starts as quote_balance (SCALE) — promote to SCALE^2
        let mut num_s2: i128 = quote_balance * SCALE;
        for (m, p) in positions.iter() {
            if m == market { continue; }
            if p.quantity.abs() < EPS { continue; }
            if let Some(omc) = self.market_configs.get(m.as_str()) {
                let pqty = p.quantity;
                let pip = p.last_index_price;
                let ommf = omc.maintenance_margin_fraction;
                // qty * indexPrice (SCALE^2) - abs(qty) * indexPrice * mmf (SCALE^3 / SCALE → SCALE^2)
                num_s2 += pqty * pip - pqty.abs() * pip * ommf / SCALE;
            }
        }

        // liq_price = num / denom — both SCALE^2, result is dimensionless
        // We want liq_price in SCALE, so multiply num by SCALE first
        let liq_price = num_s2 * SCALE / denom_s2;
        if liq_price > 0 { liq_price } else { 0 }
    }

    fn replay_to(&self, index: usize) -> (i128, HashMap<String, Position>) {
        if index == 0 {
            return (0, HashMap::new());
        }

        let cp_idx = (index / CHECKPOINT_INTERVAL) * CHECKPOINT_INTERVAL;
        let cp = &self.checkpoints[&cp_idx];
        let mut quote_balance = cp.quote_balance;
        let mut positions = cp.positions.clone();

        for i in cp_idx..index {
            quote_balance = Self::apply_event(quote_balance, &mut positions, &self.events[i]);
        }

        (quote_balance, positions)
    }

    fn make_event_out(ev: &Event) -> EventOut {
        match ev {
            Event::Deposit { timestamp, dep_type, amount } => EventOut {
                kind: "deposit".to_string(),
                time: timestamp.clone(),
                event_type: dep_type.clone(),
                market: String::new(),
                side: String::new(),
                quantity: 0.0,
                price: 0.0,
                amount: to_f64_s1(*amount),
                funding_rate: 0.0,
            },
            Event::Fill { time, market, side, fill_type, price, quantity, .. } => EventOut {
                kind: "fill".to_string(),
                time: time.clone(),
                event_type: fill_type.clone(),
                market: market.clone(),
                side: side.clone(),
                quantity: to_f64_s1(*quantity),
                price: to_f64_s1(*price),
                amount: 0.0,
                funding_rate: 0.0,
            },
            Event::Funding { time, market, payment_quantity, funding_rate, index_price, .. } => EventOut {
                kind: "funding".to_string(),
                time: time.clone(),
                event_type: "funding".to_string(),
                market: market.clone(),
                side: String::new(),
                quantity: 0.0,
                price: to_f64_s1(*index_price),
                amount: to_f64_s1(*payment_quantity),
                funding_rate: to_f64_s1(*funding_rate),
            },
        }
    }

    fn make_positions_out(&self, quote_balance: i128, positions: &HashMap<String, Position>) -> Vec<PositionOut> {
        positions.values()
            .filter(|p| p.quantity.abs() >= EPS)
            .map(|p| {
                let qty = p.quantity;
                let abs_qty = qty.abs();
                let ip = p.last_index_price;

                // Notional: abs_qty * ip (SCALE^2, no division)
                let notional_s2 = abs_qty * ip;

                // Entry price: total_notional / abs_qty (SCALE^2 / SCALE = SCALE)
                let entry_price_s1 = if abs_qty > 0 { p.total_notional / abs_qty } else { 0 };

                // Unrealized PnL: ip * qty - total_notional * sign (SCALE^2, no intermediate)
                let sign_qty: i128 = if qty > 0 { 1 } else { -1 };
                let upnl_s2 = ip * qty - p.total_notional * sign_qty;

                let liq_price = self.calculate_liquidation_price(&p.market, quote_balance, positions);

                let (imr_s3, mmr_s3) = if let Some(mc) = self.market_configs.get(&p.market) {
                    let imf = self.tiered_imf(abs_qty, mc);
                    let mmf = mc.maintenance_margin_fraction;
                    // imf * abs_qty * ip (SCALE^3, no intermediate division)
                    (imf * abs_qty * ip, mmf * abs_qty * ip)
                } else {
                    (0i128, 0i128)
                };

                PositionOut {
                    market: p.market.clone(),
                    quantity: to_f64_s1(qty),
                    entry_price: to_f64_s1(entry_price_s1),
                    last_index_price: to_f64_s1(ip),
                    notional: to_f64_s2(notional_s2),
                    unrealized_pnl: to_f64_s2(upnl_s2),
                    cumulative_realized_pnl: to_f64_s1(p.cumulative_realized_pnl),
                    cumulative_funding: to_f64_s1(p.cumulative_funding),
                    liquidation_price: to_f64_s1(liq_price),
                    imr: to_f64_s3(imr_s3),
                    mmr: to_f64_s3(mmr_s3),
                }
            })
            .collect()
    }
}

// Public WASM bindings
#[wasm_bindgen]
impl Engine {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Engine {
        console_error_panic_hook::set_once();
        Engine {
            events: Vec::new(),
            ev_quote_bal: Vec::new(),
            ev_equity: Vec::new(),
            checkpoints: HashMap::new(),
            market_configs: HashMap::new(),
            total_snapshots: 0,
        }
    }

    pub fn process(&mut self, fills_bytes: &[u8], deposits_bytes: &[u8], funding_bytes: &[u8], configs_json: &str) {
        self.parse_configs(configs_json);

        let fills_csv = std::str::from_utf8(fills_bytes).unwrap_or("");
        let deposits_csv = std::str::from_utf8(deposits_bytes).unwrap_or("");
        let funding_csv = std::str::from_utf8(funding_bytes).unwrap_or("");

        let mut events: Vec<Event> = Vec::new();

        // Parse deposits
        for line in deposits_csv.lines().skip(1) {
            let cols: Vec<&str> = line.split(',').collect();
            if cols.len() < 4 { continue; }
            events.push(Event::Deposit {
                timestamp: cols[0].to_string(),
                dep_type: cols[1].to_string(),
                amount: parse_fixed(cols[3]),
            });
        }

        // Parse fills
        for line in fills_csv.lines().skip(1) {
            let cols: Vec<&str> = line.split(',').collect();
            if cols.len() < 20 { continue; }
            events.push(Event::Fill {
                time: cols[0].to_string(),
                market: cols[1].to_string(),
                side: cols[2].to_string(),
                fill_type: cols[3].to_string(),
                price: parse_fixed(cols[4]),
                index_price: parse_fixed(cols[5]),
                quantity: parse_fixed(cols[6]),
                quote_quantity: parse_fixed(cols[7]),
                fee: parse_fixed(cols[8]),
                realized_pnl: parse_fixed(cols[9]),
            });
        }

        // Parse funding
        for line in funding_csv.lines().skip(1) {
            let cols: Vec<&str> = line.split(',').collect();
            if cols.len() < 6 { continue; }
            events.push(Event::Funding {
                time: cols[0].to_string(),
                market: cols[1].to_string(),
                payment_quantity: parse_fixed(cols[2]),
                position_quantity: parse_fixed(cols[3]),
                funding_rate: parse_fixed(cols[4]),
                index_price: parse_fixed(cols[5]),
            });
        }

        // Sort by timestamp
        events.sort_by(|a, b| a.sort_key().cmp(b.sort_key()));

        self.total_snapshots = events.len() + 1;
        self.ev_quote_bal = vec![0i128; self.total_snapshots];
        self.ev_equity = vec![0i128; self.total_snapshots];
        self.checkpoints.clear();

        // Initial checkpoint
        self.checkpoints.insert(0, Checkpoint {
            quote_balance: 0,
            positions: HashMap::new(),
        });

        let mut quote_balance: i128 = 0;
        let mut positions: HashMap<String, Position> = HashMap::new();

        for (i, ev) in events.iter().enumerate() {
            quote_balance = Self::apply_event(quote_balance, &mut positions, ev);

            let snap_idx = i + 1;
            self.ev_quote_bal[snap_idx] = quote_balance; // SCALE

            // Equity: qb * SCALE + sum(qty * ip) — all SCALE^2, no truncation
            let mut equity_s2: i128 = quote_balance * SCALE;
            for pos in positions.values() {
                if pos.quantity.abs() >= EPS {
                    equity_s2 += pos.quantity * pos.last_index_price;
                }
            }
            self.ev_equity[snap_idx] = equity_s2; // SCALE^2

            // Checkpoint at regular intervals
            if snap_idx % CHECKPOINT_INTERVAL == 0 {
                self.checkpoints.insert(snap_idx, Checkpoint {
                    quote_balance,
                    positions: positions.clone(),
                });
            }
        }

        self.events = events;
    }

    pub fn total_snapshots(&self) -> usize {
        self.total_snapshots
    }

    pub fn get_log_page_json(&self, start: usize, end: usize) -> String {
        let cap = if end >= start { end - start + 1 } else { 0 };
        let mut entries = Vec::with_capacity(cap.min(self.total_snapshots));

        let actual_end = end.min(self.total_snapshots - 1);
        for i in start..=actual_end {
            if i == 0 { continue; }
            let ev = &self.events[i - 1];

            let entry = match ev {
                Event::Deposit { timestamp, dep_type, amount } => LogEntry {
                    index: i,
                    time: timestamp.clone(),
                    kind: "deposit".to_string(),
                    event_type: dep_type.clone(),
                    market: String::new(),
                    side: String::new(),
                    qty: to_f64_s1(*amount),
                    price: 0.0,
                    fee: 0.0,
                    rpnl: 0.0,
                    quote_bal: to_f64_s1(self.ev_quote_bal[i]),
                    equity: to_f64_s2(self.ev_equity[i]),
                },
                Event::Fill { time, market, side, fill_type, price, quantity, fee, realized_pnl, .. } => LogEntry {
                    index: i,
                    time: time.clone(),
                    kind: "fill".to_string(),
                    event_type: fill_type.clone(),
                    market: market.clone(),
                    side: side.clone(),
                    qty: to_f64_s1(*quantity),
                    price: to_f64_s1(*price),
                    fee: to_f64_s1(*fee),
                    rpnl: to_f64_s1(*realized_pnl),
                    quote_bal: to_f64_s1(self.ev_quote_bal[i]),
                    equity: to_f64_s2(self.ev_equity[i]),
                },
                Event::Funding { time, market, payment_quantity, index_price, .. } => LogEntry {
                    index: i,
                    time: time.clone(),
                    kind: "funding".to_string(),
                    event_type: "funding".to_string(),
                    market: market.clone(),
                    side: String::new(),
                    qty: to_f64_s1(*payment_quantity),
                    price: to_f64_s1(*index_price),
                    fee: 0.0,
                    rpnl: to_f64_s1(*payment_quantity),
                    quote_bal: to_f64_s1(self.ev_quote_bal[i]),
                    equity: to_f64_s2(self.ev_equity[i]),
                },
            };
            entries.push(entry);
        }

        serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string())
    }

    pub fn get_state_json(&self, index: usize) -> String {
        self.get_state_json_inner(index, None)
    }

    pub fn get_state_json_with_prices(&self, index: usize, prices_json: &str) -> String {
        let overrides: HashMap<String, f64> = serde_json::from_str(prices_json).unwrap_or_default();
        self.get_state_json_inner(index, Some(&overrides))
    }
}

impl Engine {
    fn get_state_json_inner(&self, index: usize, price_overrides: Option<&HashMap<String, f64>>) -> String {
        if index >= self.total_snapshots {
            return "{}".to_string();
        }

        let (quote_balance, mut positions) = self.replay_to(index);

        if let Some(overrides) = price_overrides {
            for (market, price) in overrides {
                if let Some(pos) = positions.get_mut(market) {
                    pos.last_index_price = (*price * SCALE_F64) as i128;
                }
            }
        }

        let metrics = self.compute_metrics(quote_balance, &positions);
        let positions_out = self.make_positions_out(quote_balance, &positions);

        let event = if index > 0 {
            Some(Self::make_event_out(&self.events[index - 1]))
        } else {
            None
        };

        let state = StateOut {
            event,
            quote_balance: to_f64_s1(quote_balance),
            positions: positions_out,
            metrics,
        };

        serde_json::to_string(&state).unwrap_or_else(|_| "{}".to_string())
    }
}
