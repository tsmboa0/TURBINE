//! Rendering: a sci-fi HUD boot splash and live dashboard. Dark canvas, an outer
//! console frame, rounded neon panels in a 2x2 grid, a compact gradient brand
//! band, and airy "big value" readouts. Short, dense, glanceable.

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Gauge, Paragraph};
use ratatui::Frame;

use turbine_core::types::{Congestion, LifecycleState};

use crate::app::{Dashboard, Pulse};
use crate::theme;

/// ANSI-shadow "TURBINE" wordmark.
const BANNER: [&str; 6] = [
    "████████╗██╗   ██╗██████╗ ██████╗ ██╗███╗   ██╗███████╗",
    "╚══██╔══╝██║   ██║██╔══██╗██╔══██╗██║████╗  ██║██╔════╝",
    "   ██║   ██║   ██║██████╔╝██████╔╝██║██╔██╗ ██║█████╗  ",
    "   ██║   ██║   ██║██╔══██╗██╔══██╗██║██║╚██╗██║██╔══╝  ",
    "   ██║   ╚██████╔╝██║  ██║██████╔╝██║██║ ╚████║███████╗",
    "   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚═════╝ ╚═╝╚═╝  ╚═══╝╚══════╝",
];

const TAGLINE: &str = "ULTRA · LOW · LATENCY  ◇  SMART · EXECUTION";

/// Compact fullwidth brand for the dashboard band (double-width glyphs render
/// larger/bolder than ASCII while still fitting on a single row).
const BRAND: &str = "ＴＵＲＢＩＮＥ";

/// Format a lamports value with thousands separators.
fn lamports(v: u64) -> String {
    let s = v.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    let bytes = s.as_bytes();
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(*b as char);
    }
    out
}

/// Centered sub-rect of `area`.
fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let w = width.min(area.width);
    let h = height.min(area.height);
    Rect {
        x: area.x + area.width.saturating_sub(w) / 2,
        y: area.y + area.height.saturating_sub(h) / 2,
        width: w,
        height: h,
    }
}

/// Paint the dark HUD canvas over the whole frame.
fn canvas(f: &mut Frame, area: Rect) {
    f.render_widget(Block::default().style(Style::default().bg(theme::BG)), area);
}

fn wordmark_lines() -> Vec<Line<'static>> {
    BANNER
        .iter()
        .enumerate()
        .map(|(i, l)| {
            Line::from(Span::styled(
                *l,
                Style::default().fg(theme::ramp(i as f64 / 5.0)).add_modifier(Modifier::BOLD),
            ))
            .alignment(Alignment::Center)
        })
        .collect()
}

/// A rounded neon panel with a bracketed header.
fn panel(title: &str) -> Block<'_> {
    Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Thick)
        .border_style(Style::default().fg(theme::NEON_DIM))
        .style(Style::default().bg(theme::BG))
        .title(Span::styled(format!(" {title} "), theme::neon_bold()))
}

pub fn dashboard(f: &mut Frame, app: &Dashboard) {
    let area = f.area();
    canvas(f, area);

    // Outer console frame with a 1-cell gutter from the terminal walls.
    let gutter = Layout::default()
        .margin(1)
        .constraints([Constraint::Min(0)])
        .split(area)[0];
    let frame = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Thick)
        .border_style(Style::default().fg(theme::NEON))
        .style(Style::default().bg(theme::BG));
    let inner = frame.inner(gutter);
    f.render_widget(frame, gutter);

    let v = Layout::vertical([
        Constraint::Length(1), // header (mode / uptime)
        Constraint::Length(1), // brand band
        Constraint::Length(1), // tagline
        Constraint::Min(6),    // 2x2 panel grid
        Constraint::Length(1), // footer
    ])
    .horizontal_margin(1)
    .split(inner);

    header(f, v[0], app);
    brand(f, v[1]);
    f.render_widget(
        Paragraph::new(Line::from(Span::styled(TAGLINE, Style::default().fg(theme::DIM))))
            .alignment(Alignment::Center),
        v[2],
    );

    // 2x2 grid — each panel gets ~half the width, so nothing clips.
    let body = Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)]).split(v[3]);
    let top = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).split(body[0]);
    let bot = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).split(body[1]);

    system_monitor(f, top[0], app);
    auction_board(f, top[1], app);
    live_status(f, bot[0], app);
    contention(f, bot[1], app);

    footer(f, v[4], app);
}

fn header(f: &mut Frame, area: Rect, app: &Dashboard) {
    let cols = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).split(area);

    let mode = if app.dry_run {
        Span::styled("DRY-RUN", Style::default().fg(theme::YELLOW).add_modifier(Modifier::BOLD))
    } else {
        Span::styled("LIVE", Style::default().fg(theme::GREEN).add_modifier(Modifier::BOLD))
    };
    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("[ ", Style::default().fg(theme::NEON_DIM)),
            Span::styled("MODE", theme::label()),
            Span::styled(" ] ", Style::default().fg(theme::NEON_DIM)),
            mode,
        ])),
        cols[0],
    );

    let secs = app.uptime_secs();
    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("UPTIME ", theme::label()),
            Span::styled(
                format!("{:02}:{:02}:{:02}", secs / 3600, (secs % 3600) / 60, secs % 60),
                theme::neon_bold(),
            ),
        ]))
        .alignment(Alignment::Right),
        cols[1],
    );
}

/// Compact centered brand band: a faint rule with the big, bold fullwidth
/// "TURBINE" wordmark in the middle.
fn brand(f: &mut Frame, area: Rect) {
    let w = area.width as usize;
    f.render_widget(
        Paragraph::new(Line::from(Span::styled("━".repeat(w), Style::default().fg(theme::GRID)))),
        area,
    );

    let big = Style::default().fg(theme::NEON).add_modifier(Modifier::BOLD);
    let spans = vec![
        Span::styled("  ⚡ ", big),
        Span::styled(BRAND, big),
        Span::styled(" ⚡  ", big),
    ];
    f.render_widget(Paragraph::new(Line::from(spans)).alignment(Alignment::Center), area);
}

fn system_monitor(f: &mut Frame, area: Rect, app: &Dashboard) {
    let block = panel("SYSTEM MONITOR");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let slot_style = if app.pulsing(Pulse::Slot) { theme::pulse() } else { theme::value() };
    let interval = app.slot_interval_ms.map(|m| format!("   Δ {m}ms")).unwrap_or_default();

    let leader = match (app.next_leader_slot, app.slots_until) {
        (Some(s), Some(n)) => {
            // Jito leads most slots, so the useful signal is "is the submission
            // window open?" (green READY) rather than a perpetual "in 1 slot".
            // During the rare non-Jito gap we show the live countdown in yellow.
            let status = if app.leader_ready {
                Span::styled(
                    "   ✓ READY",
                    Style::default().fg(theme::GREEN).add_modifier(Modifier::BOLD),
                )
            } else {
                Span::styled(format!("   gap · in {n}"), Style::default().fg(theme::YELLOW))
            };
            Line::from(vec![
                Span::styled(" LEADER   ", theme::label()),
                Span::styled(
                    format!("#{s}"),
                    if app.pulsing(Pulse::Leader) { theme::pulse() } else { theme::value() },
                ),
                status,
            ])
        }
        _ => Line::from(vec![Span::styled(" LEADER   ", theme::label()), Span::styled("—", theme::label())]),
    };

    let lines = vec![
        Line::from(vec![
            Span::styled(" SLOT     ", theme::label()),
            Span::styled(app.slot.to_string(), slot_style),
            Span::styled(interval, Style::default().fg(theme::DIM)),
        ]),
        leader,
        Line::from(vec![
            Span::styled(" GEYSER ", theme::label()),
            health_dot(app.geyser),
            Span::raw("     "),
            Span::styled("JITO ", theme::label()),
            health_dot(app.jito),
        ]),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

fn health_dot(up: bool) -> Span<'static> {
    if up {
        Span::styled("● UP", theme::dot(theme::GREEN))
    } else {
        Span::styled("● DOWN", theme::dot(theme::RED))
    }
}

fn auction_board(f: &mut Frame, area: Rect, app: &Dashboard) {
    let block = panel("JITO AUCTION · lamports");
    let inner = block.inner(area);
    f.render_widget(block, area);

    // Three percentiles per row so the whole spread + the live BID fit in 3
    // dense rows (never clipped on a short terminal). The percentile the engine
    // would currently bid is highlighted so the selection is obvious.
    let sel = app.bid_percentile.as_str();
    let p = |name: &'static str, v: u64, color: Color| {
        let selected = name == sel;
        let (label_style, val_style) = if selected {
            (theme::pulse(), theme::pulse())
        } else {
            (theme::label(), Style::default().fg(color).add_modifier(Modifier::BOLD))
        };
        vec![
            Span::styled(if selected { format!("▸{name} ") } else { format!(" {name} ") }, label_style),
            Span::styled(format!("{:<9}", lamports(v)), val_style),
        ]
    };

    let mut row1 = vec![Span::raw(" ")];
    row1.extend(p("P25", app.p25, theme::FG));
    row1.extend(p("P50", app.p50, theme::FG));
    row1.extend(p("P75", app.p75, theme::FG));

    let mut row2 = vec![Span::raw(" ")];
    row2.extend(p("P95", app.p95, theme::YELLOW));
    row2.extend(p("P99", app.p99, theme::RED));

    let (cong_txt, cong_color) = match app.bid_congestion {
        Congestion::Quiet => ("QUIET", theme::GREEN),
        Congestion::Moderate => ("MOD", theme::YELLOW),
        Congestion::Hot => ("HOT", theme::RED),
    };
    let tip_style = if app.pulsing(Pulse::Tip) { theme::pulse() } else { theme::neon_bold() };
    let signal = if app.watching {
        Span::styled(format!("● {cong_txt}"), Style::default().fg(cong_color).add_modifier(Modifier::BOLD))
    } else {
        Span::styled("◦ NO SIGNAL", theme::label())
    };
    let row3 = Line::from(vec![
        Span::styled(" BID ", Style::default().fg(theme::GREEN).add_modifier(Modifier::BOLD)),
        Span::styled(format!("{} ", app.bid_percentile), theme::neon_bold()),
        Span::styled(format!("{:<9}", lamports(app.bid_tip)), tip_style),
        signal,
    ]);

    let lines = vec![Line::from(row1), Line::from(row2), row3];
    f.render_widget(Paragraph::new(lines), inner);
}

fn contention(f: &mut Frame, area: Rect, app: &Dashboard) {
    let block = panel("CONTENTION");
    let inner = block.inner(area);
    f.render_widget(block, area);

    if app.accounts.is_empty() {
        f.render_widget(
            Paragraph::new(Line::from(Span::styled(
                " NO WATCHED ACCOUNTS CONFIGURED",
                theme::label(),
            ))),
            inner,
        );
        return;
    }

    let rows = inner.height as usize;
    let mut lines = Vec::new();
    for r in app.accounts.iter().take(rows) {
        let (color, fill, tag) = match r.level {
            Congestion::Quiet => (theme::GREEN, 1usize, "QUIET"),
            Congestion::Moderate => (theme::YELLOW, 3, "MOD"),
            Congestion::Hot => (theme::RED, 5, "HOT"),
        };
        let bar: String = "█".repeat(fill).chars().chain("░".repeat(5 - fill).chars()).collect();
        // Headline number: live write-lock contenders per slot (rounded), with the
        // lifetime total alongside. The z-score stays in the background (it only
        // decides the color tier below).
        let writers = r.writers.round() as u64;
        lines.push(Line::from(vec![
            Span::styled(format!(" {:<11}", r.account), Style::default().fg(theme::FG)),
            Span::styled(bar, Style::default().fg(color)),
            Span::styled(
                format!(" {writers:>3}/slot "),
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            ),
            Span::styled(format!("Σ{} ", compact_count(r.total)), theme::label()),
            Span::styled(tag, Style::default().fg(color).add_modifier(Modifier::BOLD)),
        ]));
    }
    f.render_widget(Paragraph::new(lines), inner);
}

/// Compact, glanceable integer: `1234 → 1.2k`, `2_500_000 → 2.5M`.
fn compact_count(n: u64) -> String {
    match n {
        0..=999 => n.to_string(),
        1_000..=999_999 => format!("{:.1}k", n as f64 / 1_000.0),
        _ => format!("{:.1}M", n as f64 / 1_000_000.0),
    }
}

fn live_status(f: &mut Frame, area: Rect, app: &Dashboard) {
    let block = panel("LIVE STATUS");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let last = app
        .last_bundle_state
        .map(state_label)
        .unwrap_or_else(|| Span::styled("—", theme::label()));

    let submission = if app.killed {
        Span::styled("KILLED", Style::default().fg(theme::RED).add_modifier(Modifier::BOLD))
    } else {
        Span::styled("ARMED", Style::default().fg(theme::GREEN).add_modifier(Modifier::BOLD))
    };

    let lines = vec![
        Line::from(vec![
            Span::styled(" IN-FLIGHT ", theme::label()),
            Span::styled(format!("{:<6}", app.in_flight), theme::value()),
            Span::styled("TRACKED ", theme::label()),
            Span::styled(app.tracked.to_string(), theme::value()),
        ]),
        Line::from(vec![
            Span::styled(" AI FIXES  ", theme::label()),
            Span::styled(format!("{:<6}", app.ai_decisions), theme::neon_bold()),
            Span::styled("LAST ", theme::label()),
            last,
        ]),
        Line::from(vec![Span::styled(" SUBMISSION ", theme::label()), submission]),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

fn state_label(s: LifecycleState) -> Span<'static> {
    let (txt, color) = match s {
        LifecycleState::Built => ("Built", theme::DIM),
        LifecycleState::Submitted => ("Submitted", theme::NEON),
        LifecycleState::Processed => ("Processed", theme::YELLOW),
        LifecycleState::Confirmed => ("Confirmed", theme::GREEN),
        LifecycleState::Finalized => ("Finalized", theme::GREEN),
        LifecycleState::Failed => ("Failed", theme::RED),
    };
    Span::styled(txt, Style::default().fg(color).add_modifier(Modifier::BOLD))
}

fn footer(f: &mut Frame, area: Rect, app: &Dashboard) {
    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(" Studio ", Style::default().fg(theme::GREEN).add_modifier(Modifier::BOLD)),
            Span::styled(
                &app.studio_url,
                Style::default().fg(theme::NEON).add_modifier(Modifier::UNDERLINED),
            ),
            Span::styled("   │   ", Style::default().fg(theme::GRID)),
            Span::styled("[Q]", theme::value()),
            Span::styled(" QUIT   ", theme::label()),
            Span::styled("[CTRL-C]", theme::value()),
            Span::styled(" STOP DAEMON", theme::label()),
        ])),
        area,
    );

    // Right-aligned diagonal striped HUD meter.
    let stripes: Vec<Span> = (0..14)
        .map(|i| Span::styled("▰", Style::default().fg(theme::ramp(i as f64 / 13.0))))
        .collect();
    f.render_widget(
        Paragraph::new(Line::from(stripes)).alignment(Alignment::Right),
        area,
    );
}

/// Boot splash: emblem + gradient wordmark + progressive loading bar + stage.
pub fn boot(f: &mut Frame, done: usize, total: usize, stage: &str, studio_url: &str) {
    let area = f.area();
    canvas(f, area);
    let block = centered(area, 64, 12);

    let rows = Layout::vertical([
        Constraint::Length(6), // wordmark
        Constraint::Length(1), // tagline
        Constraint::Length(1),
        Constraint::Length(1), // gauge
        Constraint::Length(1), // stage
        Constraint::Length(1),
        Constraint::Length(1), // studio
    ])
    .split(block);

    f.render_widget(Paragraph::new(wordmark_lines()), rows[0]);
    f.render_widget(
        Paragraph::new(Line::from(Span::styled(TAGLINE, Style::default().fg(theme::DIM))))
            .alignment(Alignment::Center),
        rows[1],
    );

    let ratio = (done as f64 / total as f64).clamp(0.0, 1.0);
    let gauge = Gauge::default()
        .gauge_style(Style::default().fg(theme::NEON).bg(theme::GRID))
        .ratio(ratio)
        .label(format!("{:>3}%", (ratio * 100.0) as u16));
    f.render_widget(gauge, centered(rows[3], 46, 1));

    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("◇ booting ", theme::label()),
            Span::styled(stage, theme::neon_bold()),
            Span::styled(" …", theme::label()),
        ]))
        .alignment(Alignment::Center),
        rows[4],
    );
    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("Studio ", theme::label()),
            Span::styled(studio_url, Style::default().fg(theme::NEON).add_modifier(Modifier::UNDERLINED)),
        ]))
        .alignment(Alignment::Center),
        rows[6],
    );
}
