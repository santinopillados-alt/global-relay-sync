import { useSync } from "./useSync";
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";
import { useState, useEffect } from "react";

const OP_COLORS = { C: "var(--accent-green)", U: "var(--accent-cyan)", D: "var(--accent-red)" };
const OP_LABELS = { C: "INSERT", U: "UPDATE", D: "DELETE" };

function StatCard({ label, value, unit = "", accent = "cyan" }) {
  const color = `var(--accent-${accent})`;
  return (
    <div style={{
      background: "var(--bg-card)", border: "1px solid var(--border)",
      borderTop: `2px solid ${color}`, borderRadius: "var(--radius-lg)",
      padding: "18px", display: "flex", flexDirection: "column", gap: "8px",
    }}>
      <span style={{ fontSize: "11px", color: "var(--text-secondary)", textTransform: "uppercase", letterSpacing: "0.08em" }}>
        {label}
      </span>
      <div style={{ display: "flex", alignItems: "baseline", gap: "4px" }}>
        <span style={{ fontFamily: "var(--font-mono)", fontSize: "28px", color, fontWeight: 700 }}>
          {value ?? "—"}
        </span>
        {unit && <span style={{ fontSize: "12px", color: "var(--text-muted)", fontFamily: "var(--font-mono)" }}>{unit}</span>}
      </div>
    </div>
  );
}

function EventRow({ event, isNew }) {
  const op = event.operation || event.type;
  const color = OP_COLORS[op] || "var(--text-secondary)";
  const time = new Date().toLocaleTimeString("es-AR");
  return (
    <div style={{
      padding: "8px 16px", borderBottom: "1px solid rgba(30,45,61,0.5)",
      display: "grid", gridTemplateColumns: "65px 70px 80px 1fr 60px",
      gap: "10px", alignItems: "center",
      animation: isNew ? "slide-in 0.2s ease" : undefined,
      fontFamily: "var(--font-mono)", fontSize: "11px",
    }}>
      <span style={{ color: "var(--text-muted)" }}>{time}</span>
      <span style={{
        color, background: `${color}18`, padding: "2px 6px",
        borderRadius: "3px", textAlign: "center", fontWeight: 700,
      }}>
        {OP_LABELS[op] || op}
      </span>
      <span style={{ color: "var(--text-secondary)" }}>orders</span>
      <span style={{ color: "var(--text-primary)" }}>id={event.record_id || "—"}</span>
      <span style={{ color: "var(--text-muted)", textAlign: "right" }}>
        {event.latency_ms != null ? `${event.latency_ms}ms` : ""}
      </span>
    </div>
  );
}

export default function App() {
  const { connected, stats, conflicts, events, sourceCount, targetCount } = useSync();
  const [latencyHistory, setLatencyHistory] = useState([]);

  useEffect(() => {
    if (stats?.avg_replication_latency_ms != null) {
      const time = new Date().toLocaleTimeString("es-AR", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
      setLatencyHistory(prev => [...prev.slice(-59), { time, value: Math.round(stats.avg_replication_latency_ms) }]);
    }
  }, [stats?.avg_replication_latency_ms]);

  const syncPct = sourceCount > 0 ? Math.round((targetCount / sourceCount) * 100) : 0;

  return (
    <div style={{ minHeight: "100vh", display: "flex", flexDirection: "column" }}>
      {/* HEADER */}
      <header style={{
        height: "52px", background: "var(--bg-surface)", borderBottom: "1px solid var(--border)",
        display: "flex", alignItems: "center", justifyContent: "space-between",
        padding: "0 24px", position: "sticky", top: 0, zIndex: 100,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: "14px" }}>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "14px", color: "var(--accent-cyan)", fontWeight: 700, letterSpacing: "0.1em" }}>
            GLOBAL<span style={{ color: "var(--text-muted)" }}>/</span>RELAY
          </span>
          <span style={{ width: "1px", height: "18px", background: "var(--border)" }} />
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "11px", color: "var(--text-muted)" }}>
            CDC SYNC PLATFORM
          </span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
          <div style={{
            width: "7px", height: "7px", borderRadius: "50%",
            background: connected ? "var(--accent-green)" : "var(--accent-red)",
            boxShadow: `0 0 8px ${connected ? "var(--accent-green)" : "var(--accent-red)"}`,
            animation: connected ? "pulse-dot 2s infinite" : undefined,
          }} />
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "10px", color: "var(--text-muted)" }}>
            {connected ? "LIVE" : "OFFLINE"}
          </span>
        </div>
      </header>

      <main style={{ flex: 1, padding: "20px 24px", display: "flex", flexDirection: "column", gap: "20px" }}>

        {/* STAT CARDS */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: "12px" }}>
          <StatCard label="Total replicados" value={stats?.total_events_processed ?? "—"} accent="cyan" />
          <StatCard label="Inserts" value={stats?.inserts ?? "—"} accent="green" />
          <StatCard label="Updates" value={stats?.updates ?? "—"} accent="yellow" />
          <StatCard label="Deletes" value={stats?.deletes ?? "—"} accent="red" />
          <StatCard label="Conflictos" value={stats?.conflicts_detected ?? "—"} accent="orange" />
          <StatCard label="Latencia avg" value={stats?.avg_replication_latency_ms ? Math.round(stats.avg_replication_latency_ms) : "—"} unit="ms" accent="purple" />
        </div>

        {/* SYNC STATUS */}
        <div style={{
          background: "var(--bg-card)", border: "1px solid var(--border)",
          borderRadius: "var(--radius-lg)", padding: "18px",
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "12px" }}>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "12px", color: "var(--accent-cyan)", letterSpacing: "0.06em" }}>
              SYNC STATUS
            </span>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "12px", color: syncPct === 100 ? "var(--accent-green)" : "var(--accent-yellow)" }}>
              {syncPct}% sincronizado
            </span>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr auto 1fr", gap: "16px", alignItems: "center" }}>
            <div style={{ background: "var(--bg-elevated)", border: "1px solid var(--border)", borderRadius: "var(--radius)", padding: "12px", textAlign: "center" }}>
              <div style={{ fontSize: "10px", color: "var(--text-muted)", marginBottom: "6px", fontFamily: "var(--font-mono)" }}>SOURCE DB</div>
              <div style={{ fontFamily: "var(--font-mono)", fontSize: "22px", color: "var(--accent-cyan)" }}>{sourceCount}</div>
              <div style={{ fontSize: "10px", color: "var(--text-muted)" }}>globalrelay</div>
            </div>
            <div style={{ textAlign: "center" }}>
              <div style={{ fontFamily: "var(--font-mono)", fontSize: "18px", color: "var(--accent-green)" }}>→</div>
              <div style={{ fontSize: "9px", color: "var(--text-muted)", marginTop: "4px" }}>CDC</div>
            </div>
            <div style={{ background: "var(--bg-elevated)", border: "1px solid var(--border)", borderRadius: "var(--radius)", padding: "12px", textAlign: "center" }}>
              <div style={{ fontSize: "10px", color: "var(--text-muted)", marginBottom: "6px", fontFamily: "var(--font-mono)" }}>TARGET DB</div>
              <div style={{ fontFamily: "var(--font-mono)", fontSize: "22px", color: "var(--accent-green)" }}>{targetCount}</div>
              <div style={{ fontSize: "10px", color: "var(--text-muted)" }}>globalrelay_replica</div>
            </div>
          </div>
          {/* Barra de progreso */}
          <div style={{ marginTop: "14px", height: "4px", background: "var(--border)", borderRadius: "2px", overflow: "hidden" }}>
            <div style={{
              width: `${syncPct}%`, height: "100%", borderRadius: "2px",
              background: syncPct === 100 ? "var(--accent-green)" : "var(--accent-cyan)",
              transition: "width 0.5s ease",
            }} />
          </div>
        </div>

        {/* LATENCY CHART */}
        <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: "var(--radius-lg)", padding: "18px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "16px" }}>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "12px", color: "var(--accent-cyan)", letterSpacing: "0.06em" }}>
              REPLICATION LATENCY (ms)
            </span>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "12px", color: "var(--accent-purple)" }}>
              {latencyHistory.length ? `${latencyHistory[latencyHistory.length - 1]?.value}ms` : "—"}
            </span>
          </div>
          <ResponsiveContainer width="100%" height={120}>
            <AreaChart data={latencyHistory} margin={{ top: 4, right: 4, bottom: 0, left: -20 }}>
              <defs>
                <linearGradient id="latGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="var(--accent-purple)" stopOpacity={0.3} />
                  <stop offset="100%" stopColor="var(--accent-purple)" stopOpacity={0} />
                </linearGradient>
              </defs>
              <XAxis dataKey="time" tick={{ fill: "var(--text-muted)", fontSize: 9, fontFamily: "monospace" }} axisLine={false} tickLine={false} interval="preserveStartEnd" />
              <YAxis tick={{ fill: "var(--text-muted)", fontSize: 9, fontFamily: "monospace" }} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={{ background: "var(--bg-elevated)", border: "1px solid var(--border-bright)", borderRadius: "6px", fontFamily: "monospace", fontSize: "11px" }} />
              <Area type="monotone" dataKey="value" stroke="var(--accent-purple)" strokeWidth={1.5} fill="url(#latGrad)" dot={false} isAnimationActive={false} />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* EVENT STREAM */}
        <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: "var(--radius-lg)", overflow: "hidden" }}>
          <div style={{ padding: "14px 16px", borderBottom: "1px solid var(--border)" }}>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "12px", color: "var(--accent-cyan)", letterSpacing: "0.06em" }}>
              CDC EVENT STREAM
            </span>
          </div>
          <div style={{ maxHeight: "300px", overflowY: "auto" }}>
            {events.length === 0 ? (
              <div style={{ padding: "32px", textAlign: "center", color: "var(--text-muted)", fontFamily: "var(--font-mono)", fontSize: "11px" }}>
                Esperando eventos CDC...
              </div>
            ) : (
              events.map((e, i) => <EventRow key={i} event={e} isNew={i === 0} />)
            )}
          </div>
        </div>

      </main>

      <footer style={{
        height: "32px", borderTop: "1px solid var(--border)",
        display: "flex", alignItems: "center", padding: "0 24px", gap: "20px",
      }}>
        <span style={{ fontFamily: "var(--font-mono)", fontSize: "9px", color: "var(--text-muted)", letterSpacing: "0.08em" }}>
          GLOBAL/RELAY v1.0.0
        </span>
        <span style={{ fontFamily: "var(--font-mono)", fontSize: "9px", color: "var(--text-muted)" }}>
          PostgreSQL WAL · Kafka · FastAPI · React
        </span>
        {connected && (
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "9px", color: "var(--accent-green)", marginLeft: "auto" }}>
            ● REPLICATING LIVE
          </span>
        )}
      </footer>
    </div>
  );
}