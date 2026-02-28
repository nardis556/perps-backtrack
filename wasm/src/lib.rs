use std::collections::{HashMap, BTreeMap};
use wasm_bindgen::prelude::*;
use serde::{Serialize, Deserialize};

const CHECKPOINT_INTERVAL: usize = 500;
const SCALE: i128 = 100_000_000; // 10^8  — 1 unit in fixed-point
const SCALE_F64: f64 = 100_000_000.0;
const SCALE_SQ: f64 = SCALE_F64 * SCALE_F64; // 10^16
const SCALE_CU: f64 = SCALE_F64 * SCALE_F64 * SCALE_F64; // 10^24
const MIN_QTY: i128 = 1; // smallest representable quantity: 0.00000001

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
fn to_f64_s1(value: i128) -> f64 { value as f64 / SCALE_F64 }

// Convert SCALE^2 (10^16) to f64
#[inline]
fn to_f64_s2(value: i128) -> f64 { value as f64 / SCALE_SQ }

// Convert SCALE^3 (10^24) to f64
#[inline]
fn to_f64_s3(value: i128) -> f64 { value as f64 / SCALE_CU }

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
        deposit_type: String,
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

#[derive(Serialize)]
struct MarketStats {
    market: String,
    #[serde(rename = "fillCount")]
    fill_count: usize,
    #[serde(rename = "totalVolume")]
    total_volume: f64,
    #[serde(rename = "totalFees")]
    total_fees: f64,
    #[serde(rename = "totalRealizedPnl")]
    total_realized_pnl: f64,
    #[serde(rename = "totalFundingPayments")]
    total_funding_payments: f64,
    #[serde(rename = "fundingCount")]
    funding_count: usize,
}

#[derive(Serialize)]
struct StatsResponse {
    daily: Vec<DailyStats>,
    markets: Vec<MarketStats>,
}

#[derive(Serialize)]
struct DailyStats {
    date: String,
    #[serde(rename = "fillCount")]
    fill_count: usize,
    #[serde(rename = "depositCount")]
    deposit_count: usize,
    #[serde(rename = "withdrawalCount")]
    withdrawal_count: usize,
    #[serde(rename = "depositAmount")]
    deposit_amount: f64,
    #[serde(rename = "withdrawalAmount")]
    withdrawal_amount: f64,
    #[serde(rename = "fundingCount")]
    funding_count: usize,
    #[serde(rename = "totalVolume")]
    total_volume: f64,
    #[serde(rename = "totalFees")]
    total_fees: f64,
    #[serde(rename = "totalRealizedPnl")]
    total_realized_pnl: f64,
    #[serde(rename = "totalFundingPayments")]
    total_funding_payments: f64,
    #[serde(rename = "endEquity")]
    end_equity: f64,
    #[serde(rename = "endQuoteBalance")]
    end_quote_balance: f64,
    #[serde(rename = "eventCount")]
    event_count: usize,
}

// ============================================================
// Engine
// ============================================================

#[wasm_bindgen]
pub struct Engine {
    events: Vec<Event>,
    event_quote_balances: Vec<i128>,  // SCALE
    event_equities: Vec<i128>,        // SCALE^2
    checkpoints: HashMap<usize, Checkpoint>,
    market_configs: HashMap<String, MarketConfig>,
    total_snapshots: usize,
}

// Private methods
impl Engine {
    fn parse_configs(&mut self, json: &str) {
        self.market_configs.clear();
        let inputs: Vec<MarketConfigInput> = match serde_json::from_str(json) {
            Ok(parsed) => parsed,
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

    fn apply_event(quote_balance: i128, positions: &mut HashMap<String, Position>, event: &Event) -> i128 {
        match event {
            Event::Deposit { deposit_type, amount, .. } => {
                if deposit_type == "deposit" {
                    quote_balance + amount
                } else {
                    quote_balance - amount
                }
            }
            Event::Fill { fill_type, side, price, index_price, quantity, quote_quantity, fee, realized_pnl, market, .. } => {
                if fill_type == "liquidation" {
                    for position in positions.values_mut() {
                        position.quantity = 0;
                        position.total_notional = 0;
                        position.cumulative_realized_pnl = 0;
                        position.cumulative_fees = 0;
                        position.cumulative_funding = 0;
                    }
                    0
                } else {
                    let mut updated_balance = quote_balance;
                    if side == "buy" {
                        updated_balance -= quote_quantity;
                    } else {
                        updated_balance += quote_quantity;
                    }
                    updated_balance -= fee;
                    Self::process_fill(positions, market, side, *price, *index_price, *quantity, *fee, *realized_pnl);
                    updated_balance
                }
            }
            Event::Funding { market, payment_quantity, index_price, .. } => {
                let position = positions.entry(market.clone()).or_insert_with(|| Position::new(market));
                position.cumulative_funding += payment_quantity;
                position.last_index_price = *index_price;
                quote_balance + payment_quantity
            }
        }
    }

    fn process_fill(
        positions: &mut HashMap<String, Position>,
        market: &str, side: &str, price: i128, index_price: i128,
        quantity: i128, fee: i128, realized_pnl: i128,
    ) {
        let position = positions.entry(market.to_string()).or_insert_with(|| Position::new(market));

        let signed_fill_qty: i128 = if side == "buy" { quantity } else { -quantity };
        let old_quantity = position.quantity;
        let new_quantity = old_quantity + signed_fill_qty;

        let is_same_direction = old_quantity.abs() < MIN_QTY
            || (old_quantity > 0 && signed_fill_qty > 0)
            || (old_quantity < 0 && signed_fill_qty < 0);

        let crosses_zero = old_quantity.abs() >= MIN_QTY
            && ((old_quantity > 0 && new_quantity < 0) || (old_quantity < 0 && new_quantity > 0));

        if is_same_direction {
            if old_quantity.abs() < MIN_QTY {
                // Fresh open — raw product, no division (SCALE^2)
                position.total_notional = quantity * price;
                position.cumulative_realized_pnl = 0;
                position.cumulative_fees = 0;
                position.cumulative_funding = 0;
            } else {
                // Add to position — raw product, no division (SCALE^2)
                position.total_notional += quantity * price;
            }
            position.quantity = new_quantity;
        } else if crosses_zero {
            // closeAndOpen — new position with remainder
            position.quantity = new_quantity;
            position.total_notional = new_quantity.abs() * price; // SCALE^2
            position.cumulative_realized_pnl = 0;
            position.cumulative_fees = 0;
            position.cumulative_funding = 0;
        } else {
            // Reducing position
            if new_quantity.abs() < MIN_QTY {
                position.quantity = 0;
                position.total_notional = 0;
            } else {
                // Proportional notional reduction — single division, no intermediate entry_price
                let close_quantity = quantity.min(old_quantity.abs());
                let removed_notional = position.total_notional * close_quantity / old_quantity.abs();
                position.total_notional -= removed_notional;
                position.quantity = new_quantity;
            }
        }

        position.cumulative_realized_pnl += realized_pnl;
        position.cumulative_fees += fee;
        position.last_index_price = index_price;
    }

    fn tiered_imf(&self, abs_quantity: i128, config: &MarketConfig) -> i128 {
        let imf = config.initial_margin_fraction;
        if abs_quantity <= config.base_position_size {
            return imf;
        }
        let excess = abs_quantity - config.base_position_size;
        // ceil(excess / incremental_position_size) — both are SCALE, scales cancel
        let increment = config.incremental_position_size;
        let steps = (excess + increment - 1) / increment;
        // steps is unscaled integer, incremental_imf is SCALE → result is SCALE
        imf + steps * config.incremental_initial_margin_fraction
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
        let mut open_position_count = 0usize;

        for position in positions.values() {
            total_realized_pnl += position.cumulative_realized_pnl;
            total_funding += position.cumulative_funding;
            total_fees += position.cumulative_fees;

            if position.quantity.abs() < MIN_QTY { continue; }
            open_position_count += 1;

            let quantity = position.quantity;
            let abs_quantity = quantity.abs();
            let index_price = position.last_index_price;

            // qty * ip — no division (SCALE^2)
            index_notional_sum += quantity * index_price;
            total_abs_index_notional += abs_quantity * index_price;

            // Unrealized PnL directly from total_notional — no intermediate entry_price
            // uPnL = (indexPrice - entryPrice) * qty
            //       = indexPrice * qty - entryPrice * abs(qty) * sign(qty)
            //       = indexPrice * qty - total_notional * sign(qty)
            // All terms in SCALE^2
            let sign: i128 = if quantity > 0 { 1 } else { -1 };
            total_unrealized_pnl += index_price * quantity - position.total_notional * sign;

            if let Some(config) = self.market_configs.get(&position.market) {
                let imf = self.tiered_imf(abs_quantity, config);
                let mmf = config.maintenance_margin_fraction;
                // imf * abs_qty * ip — no intermediate division (SCALE^3)
                total_imr += imf * abs_quantity * index_price;
                total_mmr += mmf * abs_quantity * index_price;
            }
        }

        // equity = quoteBalance + sum(qty * indexPrice)
        // Promote quote_balance from SCALE to SCALE^2, then add SCALE^2 notional sum
        let equity_s2: i128 = quote_balance * SCALE + index_notional_sum;

        // free_collateral = equity - totalIMR
        // Promote equity from SCALE^2 to SCALE^3, then subtract SCALE^3 IMR
        let free_collateral_s3: i128 = equity_s2 * SCALE - total_imr;

        // leverage = totalAbsIndexNotional / equity (dimensionless)
        // Both SCALE^2 — ratio computed as f64
        let leverage = if total_abs_index_notional == 0 || equity_s2 == 0 {
            0.0
        } else {
            total_abs_index_notional as f64 / equity_s2 as f64
        };

        // margin_ratio = totalMMR / equity
        // mmr is SCALE^3, equity is SCALE^2 — ratio is SCALE, convert with to_f64_s1
        let margin_ratio = if equity_s2 == 0 {
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
            leverage,
            margin_ratio,
            total_realized_pnl: to_f64_s1(total_realized_pnl),
            total_funding: to_f64_s1(total_funding),
            total_fees: to_f64_s1(total_fees),
            open_positions: open_position_count,
        }
    }

    fn calculate_liquidation_price(&self, market: &str, quote_balance: i128, positions: &HashMap<String, Position>) -> i128 {
        let target_position = match positions.get(market) {
            Some(position) if position.quantity.abs() >= MIN_QTY => position,
            _ => return 0,
        };

        let config = match self.market_configs.get(market) {
            Some(config) => config,
            None => return 0,
        };

        let quantity = target_position.quantity;
        let abs_quantity = quantity.abs();
        let mmf = config.maintenance_margin_fraction;

        // denom = abs(qty) * mmf - qty
        // abs_quantity * mmf is SCALE^2, qty is SCALE — promote qty to SCALE^2
        let denominator = abs_quantity * mmf - quantity * SCALE;
        if denominator.abs() < MIN_QTY { return 0; }

        // numerator starts as quote_balance (SCALE) — promote to SCALE^2
        let mut numerator = quote_balance * SCALE;
        for (other_market, other_position) in positions.iter() {
            if other_market == market { continue; }
            if other_position.quantity.abs() < MIN_QTY { continue; }
            if let Some(other_config) = self.market_configs.get(other_market.as_str()) {
                let other_quantity = other_position.quantity;
                let other_index_price = other_position.last_index_price;
                let other_mmf = other_config.maintenance_margin_fraction;
                // qty * indexPrice (SCALE^2) - abs(qty) * indexPrice * mmf (SCALE^3 / SCALE → SCALE^2)
                numerator += other_quantity * other_index_price - other_quantity.abs() * other_index_price * other_mmf / SCALE;
            }
        }

        // liq_price = numerator / denominator — both SCALE^2, result is dimensionless
        // We want liq_price in SCALE, so multiply numerator by SCALE first
        let liquidation_price = numerator * SCALE / denominator;
        if liquidation_price > 0 { liquidation_price } else { 0 }
    }

    fn replay_to(&self, index: usize) -> (i128, HashMap<String, Position>) {
        if index == 0 {
            return (0, HashMap::new());
        }

        let checkpoint_index = (index / CHECKPOINT_INTERVAL) * CHECKPOINT_INTERVAL;
        let checkpoint = &self.checkpoints[&checkpoint_index];
        let mut quote_balance = checkpoint.quote_balance;
        let mut positions = checkpoint.positions.clone();

        for i in checkpoint_index..index {
            quote_balance = Self::apply_event(quote_balance, &mut positions, &self.events[i]);
        }

        (quote_balance, positions)
    }

    fn make_event_out(event: &Event) -> EventOut {
        match event {
            Event::Deposit { timestamp, deposit_type, amount } => EventOut {
                kind: "deposit".to_string(),
                time: timestamp.clone(),
                event_type: deposit_type.clone(),
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
            .filter(|position| position.quantity.abs() >= MIN_QTY)
            .map(|position| {
                let quantity = position.quantity;
                let abs_quantity = quantity.abs();
                let index_price = position.last_index_price;

                // Notional: abs_qty * ip (SCALE^2, no division)
                let notional = abs_quantity * index_price;

                // Entry price: total_notional / abs_qty (SCALE^2 / SCALE = SCALE)
                let entry_price = if abs_quantity > 0 { position.total_notional / abs_quantity } else { 0 };

                // Unrealized PnL: ip * qty - total_notional * sign (SCALE^2, no intermediate)
                let sign: i128 = if quantity > 0 { 1 } else { -1 };
                let unrealized_pnl = index_price * quantity - position.total_notional * sign;

                let liquidation_price = self.calculate_liquidation_price(&position.market, quote_balance, positions);

                let (initial_margin, maintenance_margin) = if let Some(config) = self.market_configs.get(&position.market) {
                    let imf = self.tiered_imf(abs_quantity, config);
                    let mmf = config.maintenance_margin_fraction;
                    // imf * abs_qty * ip (SCALE^3, no intermediate division)
                    (imf * abs_quantity * index_price, mmf * abs_quantity * index_price)
                } else {
                    (0i128, 0i128)
                };

                PositionOut {
                    market: position.market.clone(),
                    quantity: to_f64_s1(quantity),
                    entry_price: to_f64_s1(entry_price),
                    last_index_price: to_f64_s1(index_price),
                    notional: to_f64_s2(notional),
                    unrealized_pnl: to_f64_s2(unrealized_pnl),
                    cumulative_realized_pnl: to_f64_s1(position.cumulative_realized_pnl),
                    cumulative_funding: to_f64_s1(position.cumulative_funding),
                    liquidation_price: to_f64_s1(liquidation_price),
                    imr: to_f64_s3(initial_margin),
                    mmr: to_f64_s3(maintenance_margin),
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
            event_quote_balances: Vec::new(),
            event_equities: Vec::new(),
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
                deposit_type: cols[1].to_string(),
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
        self.event_quote_balances = vec![0i128; self.total_snapshots];
        self.event_equities = vec![0i128; self.total_snapshots];
        self.checkpoints.clear();

        // Initial checkpoint
        self.checkpoints.insert(0, Checkpoint {
            quote_balance: 0,
            positions: HashMap::new(),
        });

        let mut quote_balance: i128 = 0;
        let mut positions: HashMap<String, Position> = HashMap::new();

        for (i, event) in events.iter().enumerate() {
            quote_balance = Self::apply_event(quote_balance, &mut positions, event);

            let snapshot_index = i + 1;
            self.event_quote_balances[snapshot_index] = quote_balance; // SCALE

            // Equity: quote_balance * SCALE + sum(qty * ip) — all SCALE^2, no truncation
            let mut equity: i128 = quote_balance * SCALE;
            for position in positions.values() {
                if position.quantity.abs() >= MIN_QTY {
                    equity += position.quantity * position.last_index_price;
                }
            }
            self.event_equities[snapshot_index] = equity; // SCALE^2

            // Checkpoint at regular intervals for fast seeking
            if snapshot_index % CHECKPOINT_INTERVAL == 0 {
                self.checkpoints.insert(snapshot_index, Checkpoint {
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
        let capacity = if end >= start { end - start + 1 } else { 0 };
        let mut entries = Vec::with_capacity(capacity.min(self.total_snapshots));

        let actual_end = end.min(self.total_snapshots - 1);
        for i in start..=actual_end {
            if i == 0 { continue; }
            let event = &self.events[i - 1];

            let entry = match event {
                Event::Deposit { timestamp, deposit_type, amount } => LogEntry {
                    index: i,
                    time: timestamp.clone(),
                    kind: "deposit".to_string(),
                    event_type: deposit_type.clone(),
                    market: String::new(),
                    side: String::new(),
                    qty: to_f64_s1(*amount),
                    price: 0.0,
                    fee: 0.0,
                    rpnl: 0.0,
                    quote_bal: to_f64_s1(self.event_quote_balances[i]),
                    equity: to_f64_s2(self.event_equities[i]),
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
                    quote_bal: to_f64_s1(self.event_quote_balances[i]),
                    equity: to_f64_s2(self.event_equities[i]),
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
                    quote_bal: to_f64_s1(self.event_quote_balances[i]),
                    equity: to_f64_s2(self.event_equities[i]),
                },
            };
            entries.push(entry);
        }

        serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string())
    }

    pub fn get_state_json(&self, index: usize) -> String {
        self.get_state_json_inner(index, None, None)
    }

    pub fn get_state_json_with_prices(&self, index: usize, prices_json: &str) -> String {
        let overrides: HashMap<String, f64> = serde_json::from_str(prices_json).unwrap_or_default();
        self.get_state_json_inner(index, Some(&overrides), None)
    }

    pub fn get_state_json_full(&self, index: usize, prices_json: &str, quote_balance_adjustment: f64) -> String {
        let overrides: HashMap<String, f64> = serde_json::from_str(prices_json).unwrap_or_default();
        let adj = if quote_balance_adjustment.abs() > 1e-12 {
            Some((quote_balance_adjustment * SCALE_F64) as i128)
        } else {
            None
        };
        self.get_state_json_inner(index, Some(&overrides), adj)
    }

    pub fn get_daily_stats_json(&self) -> String {
        struct DayAccum {
            fill_count: usize,
            deposit_count: usize,
            withdrawal_count: usize,
            deposit_amount: i128,
            withdrawal_amount: i128,
            funding_count: usize,
            total_volume: i128,
            total_fees: i128,
            total_realized_pnl: i128,
            total_funding_payments: i128,
            last_event_index: usize,
            event_count: usize,
        }

        let mut days: BTreeMap<String, DayAccum> = BTreeMap::new();

        struct MktAccum {
            fill_count: usize,
            total_volume: i128,
            total_fees: i128,
            total_realized_pnl: i128,
            total_funding_payments: i128,
            funding_count: usize,
        }
        let mut markets: BTreeMap<String, MktAccum> = BTreeMap::new();

        for (i, event) in self.events.iter().enumerate() {
            let time_str = event.sort_key();
            let date = if time_str.len() >= 10 { &time_str[..10] } else { time_str };
            let snapshot_index = i + 1;

            let accum = days.entry(date.to_string()).or_insert_with(|| DayAccum {
                fill_count: 0, deposit_count: 0, withdrawal_count: 0,
                deposit_amount: 0, withdrawal_amount: 0, funding_count: 0,
                total_volume: 0, total_fees: 0, total_realized_pnl: 0,
                total_funding_payments: 0, last_event_index: 0, event_count: 0,
            });

            accum.event_count += 1;
            accum.last_event_index = snapshot_index;

            match event {
                Event::Fill { market, quote_quantity, fee, realized_pnl, .. } => {
                    accum.fill_count += 1;
                    accum.total_volume += quote_quantity;
                    accum.total_fees += fee;
                    accum.total_realized_pnl += realized_pnl;

                    let ma = markets.entry(market.clone()).or_insert_with(|| MktAccum {
                        fill_count: 0, total_volume: 0, total_fees: 0,
                        total_realized_pnl: 0, total_funding_payments: 0, funding_count: 0,
                    });
                    ma.fill_count += 1;
                    ma.total_volume += quote_quantity;
                    ma.total_fees += fee;
                    ma.total_realized_pnl += realized_pnl;
                }
                Event::Deposit { deposit_type, amount, .. } => {
                    if deposit_type == "deposit" {
                        accum.deposit_count += 1;
                        accum.deposit_amount += amount;
                    } else {
                        accum.withdrawal_count += 1;
                        accum.withdrawal_amount += amount;
                    }
                }
                Event::Funding { market, payment_quantity, .. } => {
                    accum.funding_count += 1;
                    accum.total_funding_payments += payment_quantity;

                    let ma = markets.entry(market.clone()).or_insert_with(|| MktAccum {
                        fill_count: 0, total_volume: 0, total_fees: 0,
                        total_realized_pnl: 0, total_funding_payments: 0, funding_count: 0,
                    });
                    ma.funding_count += 1;
                    ma.total_funding_payments += payment_quantity;
                }
            }
        }

        let daily: Vec<DailyStats> = days.into_iter().map(|(date, a)| {
            DailyStats {
                date,
                fill_count: a.fill_count,
                deposit_count: a.deposit_count,
                withdrawal_count: a.withdrawal_count,
                deposit_amount: to_f64_s1(a.deposit_amount),
                withdrawal_amount: to_f64_s1(a.withdrawal_amount),
                funding_count: a.funding_count,
                total_volume: to_f64_s1(a.total_volume),
                total_fees: to_f64_s1(a.total_fees),
                total_realized_pnl: to_f64_s1(a.total_realized_pnl),
                total_funding_payments: to_f64_s1(a.total_funding_payments),
                end_equity: to_f64_s2(self.event_equities[a.last_event_index]),
                end_quote_balance: to_f64_s1(self.event_quote_balances[a.last_event_index]),
                event_count: a.event_count,
            }
        }).collect();

        let market_stats: Vec<MarketStats> = markets.into_iter().map(|(market, a)| {
            MarketStats {
                market,
                fill_count: a.fill_count,
                total_volume: to_f64_s1(a.total_volume),
                total_fees: to_f64_s1(a.total_fees),
                total_realized_pnl: to_f64_s1(a.total_realized_pnl),
                total_funding_payments: to_f64_s1(a.total_funding_payments),
                funding_count: a.funding_count,
            }
        }).collect();

        let response = StatsResponse { daily, markets: market_stats };
        serde_json::to_string(&response).unwrap_or_else(|_| "{\"daily\":[],\"markets\":[]}".to_string())
    }
}

impl Engine {
    fn get_state_json_inner(&self, index: usize, price_overrides: Option<&HashMap<String, f64>>, quote_balance_adjustment: Option<i128>) -> String {
        if index >= self.total_snapshots {
            return "{}".to_string();
        }

        let (mut quote_balance, mut positions) = self.replay_to(index);

        if let Some(overrides) = price_overrides {
            for (market, price) in overrides {
                if let Some(position) = positions.get_mut(market) {
                    position.last_index_price = (*price * SCALE_F64) as i128;
                }
            }
        }

        // Apply quote balance adjustment (modifying equity by adjusting quote balance)
        if let Some(adj) = quote_balance_adjustment {
            quote_balance += adj;
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

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_position(market: &str) -> Position {
        Position::new(market)
    }

    fn entry_price(pos: &Position) -> f64 {
        let abs_q = pos.quantity.abs();
        if abs_q == 0 { return 0.0; }
        // total_notional is SCALE^2, abs_q is SCALE → result is SCALE
        to_f64_s1(pos.total_notional / abs_q)
    }

    fn p(s: &str) -> i128 { parse_fixed(s) }

    fn fill(positions: &mut HashMap<String, Position>, market: &str, side: &str, price: &str, qty: &str, fee: &str, rpnl: &str) {
        Engine::process_fill(
            positions, market, side,
            p(price), p(price), // index_price = price
            p(qty), p(fee), p(rpnl),
        );
    }

    // ----------------------------------------------------------
    // Basic: single fill opens a long
    // ----------------------------------------------------------
    #[test]
    fn test_single_buy_opens_long() {
        let mut positions: HashMap<String, Position> = HashMap::new();
        fill(&mut positions, "BTC-USD", "buy", "50000.00", "1.00", "20.00", "0.00");

        let pos = &positions["BTC-USD"];
        assert_eq!(to_f64_s1(pos.quantity), 1.0);
        assert!((entry_price(pos) - 50000.0).abs() < 0.01);
    }

    // ----------------------------------------------------------
    // Basic: single fill opens a short
    // ----------------------------------------------------------
    #[test]
    fn test_single_sell_opens_short() {
        let mut positions: HashMap<String, Position> = HashMap::new();
        fill(&mut positions, "BTC-USD", "sell", "60000.00", "2.00", "24.00", "0.00");

        let pos = &positions["BTC-USD"];
        assert_eq!(to_f64_s1(pos.quantity), -2.0);
        assert!((entry_price(pos) - 60000.0).abs() < 0.01);
    }

    // ----------------------------------------------------------
    // Weighted average: multiple buys at different prices
    // ----------------------------------------------------------
    #[test]
    fn test_weighted_average_entry_multiple_buys() {
        let mut positions: HashMap<String, Position> = HashMap::new();

        // Buy 100 @ $50
        fill(&mut positions, "BTC-USD", "buy", "50.00", "100.00", "0.00", "0.00");
        let pos = &positions["BTC-USD"];
        assert!((entry_price(pos) - 50.0).abs() < 0.01);

        // Buy 100 more @ $60
        fill(&mut positions, "BTC-USD", "buy", "60.00", "100.00", "0.00", "0.00");
        let pos = &positions["BTC-USD"];
        assert_eq!(to_f64_s1(pos.quantity), 200.0);
        // Weighted avg: (100*50 + 100*60) / 200 = 55
        assert!((entry_price(pos) - 55.0).abs() < 0.01);
    }

    // ----------------------------------------------------------
    // Weighted average: multiple sells at different prices (short)
    // ----------------------------------------------------------
    #[test]
    fn test_weighted_average_entry_multiple_sells() {
        let mut positions: HashMap<String, Position> = HashMap::new();

        // Sell 50 @ $100
        fill(&mut positions, "BTC-USD", "sell", "100.00", "50.00", "0.00", "0.00");
        // Sell 150 @ $120
        fill(&mut positions, "BTC-USD", "sell", "120.00", "150.00", "0.00", "0.00");

        let pos = &positions["BTC-USD"];
        assert_eq!(to_f64_s1(pos.quantity), -200.0);
        // Weighted avg: (50*100 + 150*120) / 200 = (5000 + 18000) / 200 = 115
        assert!((entry_price(pos) - 115.0).abs() < 0.01);
    }

    // ----------------------------------------------------------
    // Weighted average: 3 fills, unequal sizes
    // ----------------------------------------------------------
    #[test]
    fn test_weighted_average_three_fills_unequal() {
        let mut positions: HashMap<String, Position> = HashMap::new();

        // Buy 10 @ $100
        fill(&mut positions, "BTC-USD", "buy", "100.00", "10.00", "0.00", "0.00");
        // Buy 30 @ $110
        fill(&mut positions, "BTC-USD", "buy", "110.00", "30.00", "0.00", "0.00");
        // Buy 60 @ $120
        fill(&mut positions, "BTC-USD", "buy", "120.00", "60.00", "0.00", "0.00");

        let pos = &positions["BTC-USD"];
        assert_eq!(to_f64_s1(pos.quantity), 100.0);
        // Weighted avg: (10*100 + 30*110 + 60*120) / 100 = (1000 + 3300 + 7200) / 100 = 115
        assert!((entry_price(pos) - 115.0).abs() < 0.01);
    }

    // ----------------------------------------------------------
    // Partial close: entry price stays the same
    // ----------------------------------------------------------
    #[test]
    fn test_partial_close_preserves_entry_price() {
        let mut positions: HashMap<String, Position> = HashMap::new();

        // Build a long: buy 100 @ $50, buy 100 @ $60 → avg $55
        fill(&mut positions, "BTC-USD", "buy", "50.00", "100.00", "0.00", "0.00");
        fill(&mut positions, "BTC-USD", "buy", "60.00", "100.00", "0.00", "0.00");
        assert!((entry_price(&positions["BTC-USD"]) - 55.0).abs() < 0.01);

        // Sell 50 (partial close) @ $70
        fill(&mut positions, "BTC-USD", "sell", "70.00", "50.00", "0.00", "750.00");

        let pos = &positions["BTC-USD"];
        assert_eq!(to_f64_s1(pos.quantity), 150.0);
        // Entry price should still be $55
        assert!((entry_price(pos) - 55.0).abs() < 0.01);
    }

    // ----------------------------------------------------------
    // Full close: position goes to zero
    // ----------------------------------------------------------
    #[test]
    fn test_full_close() {
        let mut positions: HashMap<String, Position> = HashMap::new();

        fill(&mut positions, "BTC-USD", "buy", "50.00", "100.00", "0.00", "0.00");
        fill(&mut positions, "BTC-USD", "sell", "60.00", "100.00", "5.00", "1000.00");

        let pos = &positions["BTC-USD"];
        assert_eq!(pos.quantity, 0);
        assert_eq!(pos.total_notional, 0);
    }

    // ----------------------------------------------------------
    // Position flip: long to short
    // ----------------------------------------------------------
    #[test]
    fn test_flip_long_to_short() {
        let mut positions: HashMap<String, Position> = HashMap::new();

        // Open long 100 @ $50
        fill(&mut positions, "BTC-USD", "buy", "50.00", "100.00", "0.00", "0.00");
        assert_eq!(to_f64_s1(positions["BTC-USD"].quantity), 100.0);

        // Sell 150 @ $60 → flip to short 50
        fill(&mut positions, "BTC-USD", "sell", "60.00", "150.00", "0.00", "1000.00");

        let pos = &positions["BTC-USD"];
        assert_eq!(to_f64_s1(pos.quantity), -50.0);
        // New entry price should be $60 (the flip price)
        assert!((entry_price(pos) - 60.0).abs() < 0.01);
        // Cumulative tracking resets on flip
        assert_eq!(pos.cumulative_fees, p("0.00"));
    }

    // ----------------------------------------------------------
    // Position flip: short to long
    // ----------------------------------------------------------
    #[test]
    fn test_flip_short_to_long() {
        let mut positions: HashMap<String, Position> = HashMap::new();

        // Open short 80 @ $100
        fill(&mut positions, "BTC-USD", "sell", "100.00", "80.00", "0.00", "0.00");
        // Buy 120 @ $90 → flip to long 40
        fill(&mut positions, "BTC-USD", "buy", "90.00", "120.00", "0.00", "800.00");

        let pos = &positions["BTC-USD"];
        assert_eq!(to_f64_s1(pos.quantity), 40.0);
        assert!((entry_price(pos) - 90.0).abs() < 0.01);
    }

    // ----------------------------------------------------------
    // Close then re-open: entry price resets
    // ----------------------------------------------------------
    #[test]
    fn test_close_then_reopen() {
        let mut positions: HashMap<String, Position> = HashMap::new();

        // Open long @ $50
        fill(&mut positions, "BTC-USD", "buy", "50.00", "100.00", "0.00", "0.00");
        // Close @ $60
        fill(&mut positions, "BTC-USD", "sell", "60.00", "100.00", "0.00", "1000.00");
        assert_eq!(positions["BTC-USD"].quantity, 0);

        // Re-open long @ $70 — entry should be $70, not $50
        fill(&mut positions, "BTC-USD", "buy", "70.00", "50.00", "0.00", "0.00");

        let pos = &positions["BTC-USD"];
        assert_eq!(to_f64_s1(pos.quantity), 50.0);
        assert!((entry_price(pos) - 70.0).abs() < 0.01);
        // Cumulative tracking resets on fresh open
        assert_eq!(pos.cumulative_realized_pnl, 0);
    }

    // ----------------------------------------------------------
    // Multiple partial closes: entry stays, fees accumulate
    // ----------------------------------------------------------
    #[test]
    fn test_multiple_partial_closes() {
        let mut positions: HashMap<String, Position> = HashMap::new();

        // Open long 100 @ $50
        fill(&mut positions, "BTC-USD", "buy", "50.00", "100.00", "2.00", "0.00");

        // Close 30 @ $55
        fill(&mut positions, "BTC-USD", "sell", "55.00", "30.00", "1.65", "150.00");
        assert_eq!(to_f64_s1(positions["BTC-USD"].quantity), 70.0);
        assert!((entry_price(&positions["BTC-USD"]) - 50.0).abs() < 0.01);

        // Close 30 more @ $60
        fill(&mut positions, "BTC-USD", "sell", "60.00", "30.00", "1.80", "300.00");
        assert_eq!(to_f64_s1(positions["BTC-USD"].quantity), 40.0);
        assert!((entry_price(&positions["BTC-USD"]) - 50.0).abs() < 0.01);

        // Close remaining 40 @ $65
        fill(&mut positions, "BTC-USD", "sell", "65.00", "40.00", "2.60", "600.00");
        assert_eq!(positions["BTC-USD"].quantity, 0);

        // Fees should have accumulated: 2 + 1.65 + 1.80 + 2.60 = 8.05
        assert!((to_f64_s1(positions["BTC-USD"].cumulative_fees) - 8.05).abs() < 0.01);
        // rPnL should have accumulated: 0 + 150 + 300 + 600 = 1050
        assert!((to_f64_s1(positions["BTC-USD"].cumulative_realized_pnl) - 1050.0).abs() < 0.01);
    }

    // ----------------------------------------------------------
    // Multi-market: positions are independent
    // ----------------------------------------------------------
    #[test]
    fn test_multi_market_independent() {
        let mut positions: HashMap<String, Position> = HashMap::new();

        fill(&mut positions, "BTC-USD", "buy", "50000.00", "1.00", "20.00", "0.00");
        fill(&mut positions, "ETH-USD", "sell", "2000.00", "10.00", "8.00", "0.00");

        assert_eq!(to_f64_s1(positions["BTC-USD"].quantity), 1.0);
        assert!((entry_price(&positions["BTC-USD"]) - 50000.0).abs() < 0.01);

        assert_eq!(to_f64_s1(positions["ETH-USD"].quantity), -10.0);
        assert!((entry_price(&positions["ETH-USD"]) - 2000.0).abs() < 0.01);
    }

    // ----------------------------------------------------------
    // Stress: many small fills averaging out
    // ----------------------------------------------------------
    #[test]
    fn test_many_small_fills() {
        let mut positions: HashMap<String, Position> = HashMap::new();

        // 100 fills of 1.0 each at prices $100..$199
        for i in 0..100 {
            let price = format!("{}.00", 100 + i);
            fill(&mut positions, "BTC-USD", "buy", &price, "1.00", "0.00", "0.00");
        }

        let pos = &positions["BTC-USD"];
        assert_eq!(to_f64_s1(pos.quantity), 100.0);
        // Average of 100..199 = 149.5
        assert!((entry_price(pos) - 149.5).abs() < 0.01);
    }

    // ----------------------------------------------------------
    // Flip with accumulated avg: verify old avg doesn't leak
    // ----------------------------------------------------------
    #[test]
    fn test_flip_clears_old_average() {
        let mut positions: HashMap<String, Position> = HashMap::new();

        // Build long with complex avg: buy 50 @ $100, buy 50 @ $200 → avg $150
        fill(&mut positions, "BTC-USD", "buy", "100.00", "50.00", "0.00", "0.00");
        fill(&mut positions, "BTC-USD", "buy", "200.00", "50.00", "0.00", "0.00");
        assert!((entry_price(&positions["BTC-USD"]) - 150.0).abs() < 0.01);

        // Flip to short by selling 120 @ $180 → short 20 @ $180
        fill(&mut positions, "BTC-USD", "sell", "180.00", "120.00", "0.00", "3000.00");

        let pos = &positions["BTC-USD"];
        assert_eq!(to_f64_s1(pos.quantity), -20.0);
        // Entry must be $180, NOT influenced by old $150 avg
        assert!((entry_price(pos) - 180.0).abs() < 0.01);
    }

    // ----------------------------------------------------------
    // Tiny quantities: precision at 8 decimal places
    // ----------------------------------------------------------
    #[test]
    fn test_tiny_quantity_precision() {
        let mut positions: HashMap<String, Position> = HashMap::new();

        // Buy 0.00010000 BTC @ $67000
        fill(&mut positions, "BTC-USD", "buy", "67000.00", "0.00010000", "0.00", "0.00");

        let pos = &positions["BTC-USD"];
        assert_eq!(pos.quantity, 10000); // 0.0001 * SCALE
        assert!((entry_price(pos) - 67000.0).abs() < 1.0);
    }
}
