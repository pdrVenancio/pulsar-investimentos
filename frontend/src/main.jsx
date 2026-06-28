import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  Bell,
  Bug,
  CheckCircle2,
  Circle,
  PlugZap,
  Plus,
  Radio,
  Send,
  TrendingDown,
  TrendingUp,
  Trash2,
  TriangleAlert,
  Wifi,
  WifiOff,
  XCircle,
  Zap,
} from "lucide-react";
import "./styles.css";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";
const PULSAR_WS_BASE_URL =
  import.meta.env.VITE_PULSAR_WS_BASE_URL || "ws://localhost:8080";
const STORAGE_KEY = "pulsar-investments-subscriptions";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function decodeBase64Payload(payload) {
  try {
    const binary = atob(payload);
    const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
    return new TextDecoder().decode(bytes);
  } catch (error) {
    return `Falha ao decodificar payload: ${error.message}`;
  }
}

function parsePulsarMessage(event) {
  const envelope = JSON.parse(event.data);
  const decodedPayload = decodeBase64Payload(envelope.payload || "");
  try {
    return { envelope, decodedPayload, data: JSON.parse(decodedPayload) };
  } catch {
    return { envelope, decodedPayload, data: null };
  }
}

function buildAlertWebSocketUrl(clientId) {
  const subscriptionName = `ui-${clientId.slice(0, 8)}`;
  return `${PULSAR_WS_BASE_URL}/ws/v2/consumer/persistent/public/default/alerts-${clientId}/${subscriptionName}`;
}

function buildCepWebSocketUrl() {
  return `${PULSAR_WS_BASE_URL}/ws/v2/consumer/persistent/public/default/alerts-cep/ui-cep-global`;
}

function loadStoredSubscriptions() {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
  } catch {
    return [];
  }
}

function statusLabel(status) {
  if (status === "connected") return "conectado";
  if (status === "connecting") return "conectando";
  if (status === "error") return "erro";
  return "desconectado";
}

const PATTERN_LABELS = {
  consecutive_drops: "quedas consecutivas",
  consecutive_rises: "altas consecutivas",
  pct_drop_window: "queda % na janela",
};

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

function App() {
  // Modo A: formulário de preço alvo
  const [modeTab, setModeTab] = useState("price"); // "price" | "cep"

  const [priceForm, setPriceForm] = useState({ asset: "PETR4.SA", rule: "gte", value: "42" });
  const [cepForm, setCepForm] = useState({
    asset: "PETR4.SA",
    pattern: "consecutive_drops",
    count: "3",
    pct: "2.0",
    windowSecs: "300",
  });
  const [debugForm, setDebugForm] = useState({ asset: "PETR4.SA", price: "41.17" });

  const [subscriptions, setSubscriptions] = useState(loadStoredSubscriptions);
  const [alertsByClient, setAlertsByClient] = useState({});
  const [cepAlerts, setCepAlerts] = useState([]);
  const [cepStatus, setCepStatus] = useState("connecting");

  const [connectionStatus, setConnectionStatus] = useState({});
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isSendingDebug, setIsSendingDebug] = useState(false);
  const [notice, setNotice] = useState(null);

  const socketsRef = useRef({});
  const cepSocketRef = useRef(null);

  // Persiste subscriptions no localStorage
  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(subscriptions));
  }, [subscriptions]);

  // WebSockets por cliente (Modo A)
  useEffect(() => {
    const activeClientIds = new Set(subscriptions.map((s) => s.client_id));

    subscriptions.forEach((subscription) => {
      if (socketsRef.current[subscription.client_id]) return;

      setConnectionStatus((cur) => ({ ...cur, [subscription.client_id]: "connecting" }));

      const socket = new WebSocket(buildAlertWebSocketUrl(subscription.client_id));
      socketsRef.current[subscription.client_id] = socket;

      socket.onopen = () =>
        setConnectionStatus((cur) => ({ ...cur, [subscription.client_id]: "connected" }));

      socket.onmessage = (event) => {
        const parsed = parsePulsarMessage(event);
        const alert = parsed.data || { raw_payload: parsed.decodedPayload };
        setAlertsByClient((cur) => ({
          ...cur,
          [subscription.client_id]: [
            {
              id: `${parsed.envelope.messageId}-${Date.now()}`,
              received_at: new Date().toISOString(),
              envelope: parsed.envelope,
              ...alert,
            },
            ...(cur[subscription.client_id] || []),
          ].slice(0, 20),
        }));
        if (parsed.envelope.messageId && socket.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify({ messageId: parsed.envelope.messageId }));
        }
      };

      socket.onerror = () =>
        setConnectionStatus((cur) => ({ ...cur, [subscription.client_id]: "error" }));

      socket.onclose = () => {
        delete socketsRef.current[subscription.client_id];
        setConnectionStatus((cur) => ({ ...cur, [subscription.client_id]: "disconnected" }));
      };
    });

    Object.entries(socketsRef.current).forEach(([clientId, socket]) => {
      if (!activeClientIds.has(clientId)) {
        socket.close();
        delete socketsRef.current[clientId];
      }
    });
  }, [subscriptions]);

  // WebSocket global CEP (Modo B)
  useEffect(() => {
    if (cepSocketRef.current) return;

    const socket = new WebSocket(buildCepWebSocketUrl());
    cepSocketRef.current = socket;
    setCepStatus("connecting");

    socket.onopen = () => setCepStatus("connected");

    socket.onmessage = (event) => {
      const parsed = parsePulsarMessage(event);
      const alert = parsed.data || { raw_payload: parsed.decodedPayload };
      setCepAlerts((cur) => [
        {
          id: `cep-${Date.now()}`,
          received_at: new Date().toISOString(),
          ...alert,
        },
        ...cur,
      ].slice(0, 50));
      if (parsed.envelope.messageId && socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ messageId: parsed.envelope.messageId }));
      }
    };

    socket.onerror = () => setCepStatus("error");
    socket.onclose = () => {
      cepSocketRef.current = null;
      setCepStatus("disconnected");
    };

    return () => {
      socket.close();
      cepSocketRef.current = null;
    };
  }, []);

  const totalPriceAlerts = useMemo(
    () => Object.values(alertsByClient).reduce((t, a) => t + a.length, 0),
    [alertsByClient],
  );

  // Criar assinatura Modo A (preço alvo)
  async function createPriceSubscription(event) {
    event.preventDefault();
    setIsSubmitting(true);
    setNotice(null);
    try {
      const response = await fetch(`${API_BASE_URL}/subscriptions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          asset: priceForm.asset.trim().toUpperCase(),
          rule: priceForm.rule,
          value: Number(priceForm.value),
        }),
      });
      if (!response.ok) throw new Error((await response.text()) || `HTTP ${response.status}`);
      const created = await response.json();
      setSubscriptions((cur) => [
        {
          ...created,
          asset: priceForm.asset.trim().toUpperCase(),
          rule: priceForm.rule,
          value: Number(priceForm.value),
          mode: "price",
          created_at: new Date().toISOString(),
        },
        ...cur,
      ]);
      setNotice({ type: "success", text: "Monitoramento de preço cadastrado." });
    } catch (error) {
      setNotice({ type: "error", text: `Erro ao cadastrar: ${error.message}` });
    } finally {
      setIsSubmitting(false);
    }
  }

  // Criar assinatura Modo B (CEP)
  async function createCepSubscription(event) {
    event.preventDefault();
    setIsSubmitting(true);
    setNotice(null);
    try {
      const payload = {
        asset: cepForm.asset.trim().toUpperCase(),
        pattern: cepForm.pattern,
        count: Number(cepForm.count),
        pct: Number(cepForm.pct),
        window_secs: Number(cepForm.windowSecs),
      };
      const response = await fetch(`${API_BASE_URL}/cep-subscriptions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!response.ok) throw new Error((await response.text()) || `HTTP ${response.status}`);
      const created = await response.json();
      setSubscriptions((cur) => [
        {
          ...created,
          ...payload,
          mode: "cep",
          created_at: new Date().toISOString(),
        },
        ...cur,
      ]);
      setNotice({
        type: "success",
        text: "Filtro CEP registrado. A coleta do ativo foi iniciada.",
      });
    } catch (error) {
      setNotice({ type: "error", text: `Erro ao cadastrar CEP: ${error.message}` });
    } finally {
      setIsSubmitting(false);
    }
  }

  async function deleteSubscription(clientId) {
    const state = subscriptions.find((s) => s.client_id === clientId);
    setNotice(null);
    try {
      if (state?.mode === "cep") {
        const params = new URLSearchParams({
          subscription_id: state.subscription_id,
          asset: state.asset,
        });
        const response = await fetch(`${API_BASE_URL}/cep-subscriptions/${clientId}?${params}`, {
          method: "DELETE",
        });
        if (!response.ok && response.status !== 404)
          throw new Error((await response.text()) || `HTTP ${response.status}`);
      } else {
        const response = await fetch(`${API_BASE_URL}/subscriptions/${clientId}`, {
          method: "DELETE",
        });
        if (!response.ok && response.status !== 404)
          throw new Error((await response.text()) || `HTTP ${response.status}`);
      }
      setSubscriptions((cur) => cur.filter((s) => s.client_id !== clientId));
      setAlertsByClient((cur) => { const n = { ...cur }; delete n[clientId]; return n; });
      setNotice({ type: "success", text: "Monitoramento removido." });
    } catch (error) {
      setNotice({ type: "error", text: `Erro ao remover: ${error.message}` });
    }
  }

  async function sendDebugQuote(event) {
    event.preventDefault();
    setIsSendingDebug(true);
    setNotice(null);
    try {
      const response = await fetch(`${API_BASE_URL}/debug/raw-quotes`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          asset: debugForm.asset.trim().toUpperCase(),
          price: Number(debugForm.price),
        }),
      });
      if (!response.ok) throw new Error((await response.text()) || `HTTP ${response.status}`);
      setNotice({ type: "success", text: "Cotação de debug enviada." });
    } catch (error) {
      setNotice({ type: "error", text: `Erro no debug: ${error.message}` });
    } finally {
      setIsSendingDebug(false);
    }
  }

  const priceSubscriptions = subscriptions.filter((s) => s.mode !== "cep");
  const cepSubscriptions = subscriptions.filter((s) => s.mode === "cep");

  // Filtra alertas CEP pelos ativos monitorados (se houver filtros CEP cadastrados)
  const filteredCepAlerts = cepSubscriptions.length === 0
    ? cepAlerts
    : cepAlerts.filter((a) =>
        cepSubscriptions.some((s) => s.asset === a.asset && s.pattern === a.pattern),
      );

  return (
    <main className="app-shell">
      <section className="topbar">
        <div>
          <p className="eyebrow">Apache Pulsar + yFinance + Flink CEP</p>
          <h1>Alertas de ativos</h1>
        </div>
        <div className="summary-grid">
          <Metric icon={Radio} label="Monitoramentos" value={subscriptions.length} />
          <Metric icon={Bell} label="Alertas de preço" value={totalPriceAlerts} />
          <Metric icon={Zap} label="Alertas de padrão" value={filteredCepAlerts.length} />
          <Metric
            icon={cepStatus === "connected" ? Wifi : WifiOff}
            label="CEP Engine"
            value={statusLabel(cepStatus)}
            highlight={cepStatus === "connected" ? "green" : cepStatus === "error" ? "red" : null}
          />
        </div>
      </section>

      {notice && (
        <div className={`notice ${notice.type}`}>
          {notice.type === "success" ? <CheckCircle2 size={18} /> : <XCircle size={18} />}
          <span>{notice.text}</span>
        </div>
      )}

      <section className="workspace-grid">
        {/* Painel lateral esquerdo */}
        <div className="panel form-panel">
          {/* Tabs Modo A / Modo B */}
          <div className="mode-tabs">
            <button
              className={`mode-tab ${modeTab === "price" ? "active" : ""}`}
              onClick={() => setModeTab("price")}
            >
              <Bell size={15} /> Preço alvo
            </button>
            <button
              className={`mode-tab ${modeTab === "cep" ? "active" : ""}`}
              onClick={() => setModeTab("cep")}
            >
              <Zap size={15} /> Padrão CEP
            </button>
          </div>

          {modeTab === "price" ? (
            <>
              <div className="panel-heading" style={{ marginTop: 16 }}>
                <PlugZap size={20} />
                <h2>Novo monitoramento</h2>
              </div>
              <form onSubmit={createPriceSubscription} className="stacked-form">
                <label>
                  Ativo
                  <input
                    value={priceForm.asset}
                    onChange={(e) => setPriceForm((c) => ({ ...c, asset: e.target.value }))}
                    placeholder="PETR4.SA"
                    required
                  />
                </label>
                <label>
                  Regra
                  <select
                    value={priceForm.rule}
                    onChange={(e) => setPriceForm((c) => ({ ...c, rule: e.target.value }))}
                  >
                    <option value="gte">Maior ou igual a</option>
                    <option value="lte">Menor ou igual a</option>
                  </select>
                </label>
                <label>
                  Valor alvo (R$)
                  <input
                    type="number"
                    step="0.01"
                    value={priceForm.value}
                    onChange={(e) => setPriceForm((c) => ({ ...c, value: e.target.value }))}
                    required
                  />
                </label>
                <button type="submit" className="primary-button" disabled={isSubmitting}>
                  <Plus size={18} />
                  {isSubmitting ? "Cadastrando..." : "Cadastrar alerta"}
                </button>
              </form>
            </>
          ) : (
            <>
              <div className="panel-heading" style={{ marginTop: 16 }}>
                <Zap size={20} />
                <h2>Filtro de padrão</h2>
              </div>
              <div className="cep-info">
                O motor Flink detecta padrões temporais no mercado. Escolha o ativo e o padrão que deseja acompanhar.
              </div>
              <form onSubmit={createCepSubscription} className="stacked-form">
                <label>
                  Ativo
                  <input
                    value={cepForm.asset}
                    onChange={(e) => setCepForm((c) => ({ ...c, asset: e.target.value }))}
                    placeholder="PETR4.SA"
                    required
                  />
                </label>
                <label>
                  Padrão
                  <select
                    value={cepForm.pattern}
                    onChange={(e) => setCepForm((c) => ({ ...c, pattern: e.target.value }))}
                  >
                    <option value="consecutive_drops">N quedas consecutivas</option>
                    <option value="consecutive_rises">N altas consecutivas</option>
                    <option value="pct_drop_window">Queda % em janela de tempo</option>
                  </select>
                </label>

                {cepForm.pattern !== "pct_drop_window" && (
                  <label>
                    Nº de {cepForm.pattern === "consecutive_drops" ? "quedas" : "altas"}
                    <input
                      type="number"
                      min="2"
                      max="10"
                      value={cepForm.count}
                      onChange={(e) => setCepForm((c) => ({ ...c, count: e.target.value }))}
                      required
                    />
                  </label>
                )}

                {cepForm.pattern === "pct_drop_window" && (
                  <>
                    <label>
                      Queda mínima (%)
                      <input
                        type="number"
                        step="0.1"
                        min="0.1"
                        value={cepForm.pct}
                        onChange={(e) => setCepForm((c) => ({ ...c, pct: e.target.value }))}
                        required
                      />
                    </label>
                    <label>
                      Janela de tempo (segundos)
                      <input
                        type="number"
                        step="30"
                        min="60"
                        value={cepForm.windowSecs}
                        onChange={(e) => setCepForm((c) => ({ ...c, windowSecs: e.target.value }))}
                        required
                      />
                    </label>
                  </>
                )}

                <button type="submit" className="primary-button cep-button">
                  <Zap size={18} />
                  Monitorar padrão
                </button>
              </form>
            </>
          )}

          <div className="divider" />

          <div className="panel-heading compact">
            <Bug size={18} />
            <h2>Teste manual</h2>
          </div>
          <form onSubmit={sendDebugQuote} className="debug-form">
            <input
              value={debugForm.asset}
              onChange={(e) => setDebugForm((c) => ({ ...c, asset: e.target.value }))}
              placeholder="PETR4.SA"
              required
            />
            <input
              type="number"
              step="0.01"
              value={debugForm.price}
              onChange={(e) => setDebugForm((c) => ({ ...c, price: e.target.value }))}
              required
            />
            <button type="submit" className="secondary-button" disabled={isSendingDebug}>
              <Send size={16} />
              Enviar
            </button>
          </form>
        </div>

        {/* Área direita */}
        <section className="subscriptions-area">
          {/* Painel CEP global */}
          <CepAlertsPanel alerts={filteredCepAlerts} status={cepStatus} filters={cepSubscriptions} />

          {/* Cards Modo A */}
          {priceSubscriptions.length === 0 && cepSubscriptions.length === 0 ? (
            <div className="empty-state">
              <Activity size={28} />
              <h2>Nenhum monitoramento ativo</h2>
              <p>Cadastre um alerta de preço ou um filtro de padrão para começar.</p>
            </div>
          ) : (
            <>
              {priceSubscriptions.map((sub) => (
                <SubscriptionCard
                  key={sub.client_id}
                  subscription={sub}
                  status={connectionStatus[sub.client_id] || "connecting"}
                  alerts={alertsByClient[sub.client_id] || []}
                  onDelete={() => deleteSubscription(sub.client_id)}
                />
              ))}
              {cepSubscriptions.map((sub) => (
                <CepFilterCard
                  key={sub.client_id}
                  subscription={sub}
                  onDelete={() => deleteSubscription(sub.client_id)}
                />
              ))}
            </>
          )}
        </section>
      </section>
    </main>
  );
}

// ---------------------------------------------------------------------------
// Componentes
// ---------------------------------------------------------------------------

function Metric({ icon: Icon, label, value, highlight }) {
  return (
    <div className={`metric ${highlight ? `metric--${highlight}` : ""}`}>
      <Icon size={18} />
      <div>
        <strong>{value}</strong>
        <span>{label}</span>
      </div>
    </div>
  );
}

function CepAlertsPanel({ alerts, status, filters }) {
  const [expanded, setExpanded] = useState(true);

  return (
    <div className="panel cep-panel">
      <div className="cep-panel-header" onClick={() => setExpanded((v) => !v)}>
        <div className="panel-heading" style={{ marginBottom: 0 }}>
          <Zap size={20} />
          <h2>Alertas de padrão — Motor CEP (Flink)</h2>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span className={`socket-status ${status}`}>
            {status === "connected" ? <Wifi size={14} /> : <WifiOff size={14} />}
            {statusLabel(status)}
          </span>
          <span className="cep-count-badge">{alerts.length}</span>
          <span className="expand-toggle">{expanded ? "▲" : "▼"}</span>
        </div>
      </div>

      {expanded && (
        <div className="cep-alerts-body">
          {filters.length > 0 && (
            <div className="cep-active-filters">
              {filters.map((f) => (
                <span key={f.client_id} className="filter-chip">
                  {f.asset} · {PATTERN_LABELS[f.pattern]}
                </span>
              ))}
            </div>
          )}

          {alerts.length === 0 ? (
            <div className="quiet-row">
              <Circle size={12} />
              Aguardando padrões do motor Flink...
            </div>
          ) : (
            alerts.map((alert) => <CepAlertRow key={alert.id} alert={alert} />)
          )}
        </div>
      )}
    </div>
  );
}

function CepAlertRow({ alert }) {
  const isDropPattern =
    alert.pattern === "consecutive_drops" || alert.pattern === "pct_drop_window";
  const Icon = isDropPattern ? TrendingDown : TrendingUp;
  const colorClass = isDropPattern ? "cep-alert--red" : "cep-alert--green";

  return (
    <div className={`cep-alert-row ${colorClass}`}>
      <div className="cep-alert-left">
        <Icon size={20} />
        <div>
          <strong>{alert.asset}</strong>
          <span className="cep-pattern-label">{PATTERN_LABELS[alert.pattern] || alert.pattern}</span>
        </div>
      </div>

      <div className="cep-alert-center">
        {alert.prices && <Sparkline prices={alert.prices} down={isDropPattern} />}
        {alert.drop_pct != null && (
          <span className="cep-pct-badge">−{alert.drop_pct}%</span>
        )}
      </div>

      <div className="cep-alert-right">
        <span className="cep-last-price">
          {Number(alert.last_price).toLocaleString("pt-BR", {
            minimumFractionDigits: 2,
            maximumFractionDigits: 4,
          })}
        </span>
        <time>{new Date(alert.triggered_at || alert.received_at).toLocaleString("pt-BR")}</time>
      </div>
    </div>
  );
}

function Sparkline({ prices, down }) {
  if (!prices || prices.length < 2) return null;
  const min = Math.min(...prices);
  const max = Math.max(...prices);
  const range = max - min || 1;
  const W = 80, H = 28, pad = 3;

  const points = prices.map((p, i) => {
    const x = pad + (i / (prices.length - 1)) * (W - pad * 2);
    const y = H - pad - ((p - min) / range) * (H - pad * 2);
    return `${x},${y}`;
  }).join(" ");

  const color = down ? "#dc2626" : "#16a34a";

  return (
    <svg width={W} height={H} className="sparkline" viewBox={`0 0 ${W} ${H}`}>
      <polyline points={points} fill="none" stroke={color} strokeWidth="2" strokeLinejoin="round" />
      {prices.map((p, i) => {
        const x = pad + (i / (prices.length - 1)) * (W - pad * 2);
        const y = H - pad - ((p - min) / range) * (H - pad * 2);
        return <circle key={i} cx={x} cy={y} r="2.5" fill={color} />;
      })}
    </svg>
  );
}

function CepFilterCard({ subscription, onDelete }) {
  const patternText = PATTERN_LABELS[subscription.pattern] || subscription.pattern;
  const detail =
    subscription.pattern === "pct_drop_window"
      ? `≥ ${subscription.pct}% em ${subscription.window_secs}s`
      : `${subscription.count}× consecutivos`;

  return (
    <article className="subscription-card cep-filter-card">
      <header className="subscription-header">
        <div>
          <div className="asset-line">
            <h2>{subscription.asset}</h2>
            <span className="rule-pill rule-pill--cep">
              <Zap size={12} /> {patternText}
            </span>
            <span className="rule-pill">{detail}</span>
          </div>
          <p className="client-id">{subscription.client_id}</p>
        </div>
        <div className="card-actions">
          <span className="mode-badge">CEP · Flink</span>
          <button className="icon-button" onClick={onDelete} title="Remover">
            <Trash2 size={18} />
          </button>
        </div>
      </header>
    </article>
  );
}

function SubscriptionCard({ subscription, status, alerts, onDelete }) {
  const connected = status === "connected";
  const ruleText = subscription.rule === "gte" ? "maior ou igual a" : "menor ou igual a";

  return (
    <article className="subscription-card">
      <header className="subscription-header">
        <div>
          <div className="asset-line">
            <h2>{subscription.asset}</h2>
            <span className="rule-pill">
              {ruleText} {Number(subscription.value).toLocaleString("pt-BR")}
            </span>
          </div>
          <p className="client-id">{subscription.client_id}</p>
        </div>
        <div className="card-actions">
          <span className={`socket-status ${status}`}>
            {connected ? <Wifi size={16} /> : <WifiOff size={16} />}
            {statusLabel(status)}
          </span>
          <button className="icon-button" onClick={onDelete} title="Remover">
            <Trash2 size={18} />
          </button>
        </div>
      </header>

      <section className="alerts-list">
        <div className="alerts-heading">
          <Bell size={16} />
          <span>{alerts.length} alerta(s)</span>
        </div>
        {alerts.length === 0 ? (
          <div className="quiet-row">
            <Circle size={12} />
            Aguardando alerta para este cliente.
          </div>
        ) : (
          alerts.map((alert) => <AlertRow key={alert.id} alert={alert} />)
        )}
      </section>
    </article>
  );
}

function AlertRow({ alert }) {
  return (
    <div className="alert-row">
      <div>
        <strong>
          {Number(alert.price).toLocaleString("pt-BR", {
            minimumFractionDigits: 2,
            maximumFractionDigits: 4,
          })}
        </strong>
        <span>
          alvo {alert.rule} {Number(alert.target_value).toLocaleString("pt-BR")}
        </span>
      </div>
      <div className="alert-meta">
        <span>{alert.source || "unknown"}</span>
        <time>{new Date(alert.triggered_at || alert.received_at).toLocaleString("pt-BR")}</time>
      </div>
    </div>
  );
}

createRoot(document.getElementById("root")).render(<App />);
