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
  Trash2,
  Wifi,
  WifiOff,
  XCircle,
} from "lucide-react";
import "./styles.css";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";
const PULSAR_WS_BASE_URL =
  import.meta.env.VITE_PULSAR_WS_BASE_URL || "ws://localhost:8080";
const STORAGE_KEY = "pulsar-investments-subscriptions";

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
    return {
      envelope,
      decodedPayload,
      data: JSON.parse(decodedPayload),
    };
  } catch {
    return {
      envelope,
      decodedPayload,
      data: null,
    };
  }
}

function buildAlertWebSocketUrl(clientId) {
  const subscriptionName = `ui-${clientId.slice(0, 8)}`;
  return `${PULSAR_WS_BASE_URL}/ws/v2/consumer/persistent/public/default/alerts-${clientId}/${subscriptionName}`;
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

function App() {
  const [form, setForm] = useState({
    asset: "PETR4.SA",
    rule: "gte",
    value: "4",
  });
  const [debugForm, setDebugForm] = useState({
    asset: "PETR4.SA",
    price: "41.17",
  });
  const [subscriptions, setSubscriptions] = useState(loadStoredSubscriptions);
  const [alertsByClient, setAlertsByClient] = useState({});
  const [connectionStatus, setConnectionStatus] = useState({});
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isSendingDebug, setIsSendingDebug] = useState(false);
  const [notice, setNotice] = useState(null);
  const socketsRef = useRef({});

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(subscriptions));
  }, [subscriptions]);

  useEffect(() => {
    const activeClientIds = new Set(subscriptions.map((item) => item.client_id));

    subscriptions.forEach((subscription) => {
      const existingSocket = socketsRef.current[subscription.client_id];
      if (existingSocket) return;

      setConnectionStatus((current) => ({
        ...current,
        [subscription.client_id]: "connecting",
      }));

      const socket = new WebSocket(buildAlertWebSocketUrl(subscription.client_id));
      socketsRef.current[subscription.client_id] = socket;

      socket.onopen = () => {
        setConnectionStatus((current) => ({
          ...current,
          [subscription.client_id]: "connected",
        }));
      };

      socket.onmessage = (event) => {
        const parsed = parsePulsarMessage(event);
        const alert = parsed.data || {
          raw_payload: parsed.decodedPayload,
        };

        setAlertsByClient((current) => ({
          ...current,
          [subscription.client_id]: [
            {
              id: `${parsed.envelope.messageId}-${Date.now()}`,
              received_at: new Date().toISOString(),
              envelope: parsed.envelope,
              ...alert,
            },
            ...(current[subscription.client_id] || []),
          ].slice(0, 20),
        }));

        if (parsed.envelope.messageId && socket.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify({ messageId: parsed.envelope.messageId }));
        }
      };

      socket.onerror = () => {
        setConnectionStatus((current) => ({
          ...current,
          [subscription.client_id]: "error",
        }));
      };

      socket.onclose = () => {
        delete socketsRef.current[subscription.client_id];
        setConnectionStatus((current) => ({
          ...current,
          [subscription.client_id]: "disconnected",
        }));
      };
    });

    Object.entries(socketsRef.current).forEach(([clientId, socket]) => {
      if (!activeClientIds.has(clientId)) {
        socket.close();
        delete socketsRef.current[clientId];
      }
    });
  }, [subscriptions]);

  const totalAlerts = useMemo(
    () =>
      Object.values(alertsByClient).reduce(
        (total, clientAlerts) => total + clientAlerts.length,
        0,
      ),
    [alertsByClient],
  );

  async function createSubscription(event) {
    event.preventDefault();
    setIsSubmitting(true);
    setNotice(null);

    try {
      const response = await fetch(`${API_BASE_URL}/subscriptions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          asset: form.asset.trim().toUpperCase(),
          rule: form.rule,
          value: Number(form.value),
        }),
      });

      if (!response.ok) {
        const errorBody = await response.text();
        throw new Error(errorBody || `HTTP ${response.status}`);
      }

      const created = await response.json();
      setSubscriptions((current) => [
        {
          ...created,
          asset: form.asset.trim().toUpperCase(),
          rule: form.rule,
          value: Number(form.value),
          created_at: new Date().toISOString(),
        },
        ...current,
      ]);
      setNotice({ type: "success", text: "Monitoramento cadastrado." });
    } catch (error) {
      setNotice({ type: "error", text: `Erro ao cadastrar: ${error.message}` });
    } finally {
      setIsSubmitting(false);
    }
  }

  async function deleteSubscription(clientId) {
    setNotice(null);

    try {
      const response = await fetch(`${API_BASE_URL}/subscriptions/${clientId}`, {
        method: "DELETE",
      });

      if (!response.ok && response.status !== 404) {
        const errorBody = await response.text();
        throw new Error(errorBody || `HTTP ${response.status}`);
      }

      setSubscriptions((current) =>
        current.filter((item) => item.client_id !== clientId),
      );
      setAlertsByClient((current) => {
        const next = { ...current };
        delete next[clientId];
        return next;
      });
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

      if (!response.ok) {
        const errorBody = await response.text();
        throw new Error(errorBody || `HTTP ${response.status}`);
      }

      setNotice({ type: "success", text: "Cotação de debug enviada." });
    } catch (error) {
      setNotice({ type: "error", text: `Erro no debug: ${error.message}` });
    } finally {
      setIsSendingDebug(false);
    }
  }

  return (
    <main className="app-shell">
      <section className="topbar">
        <div>
          <p className="eyebrow">Apache Pulsar + yFinance</p>
          <h1>Alertas de ativos</h1>
        </div>
        <div className="summary-grid">
          <Metric icon={Radio} label="Monitoramentos" value={subscriptions.length} />
          <Metric icon={Bell} label="Alertas recebidos" value={totalAlerts} />
        </div>
      </section>

      {notice && (
        <div className={`notice ${notice.type}`}>
          {notice.type === "success" ? <CheckCircle2 size={18} /> : <XCircle size={18} />}
          <span>{notice.text}</span>
        </div>
      )}

      <section className="workspace-grid">
        <div className="panel form-panel">
          <div className="panel-heading">
            <PlugZap size={20} />
            <h2>Novo monitoramento</h2>
          </div>

          <form onSubmit={createSubscription} className="stacked-form">
            <label>
              Ativo
              <input
                value={form.asset}
                onChange={(event) =>
                  setForm((current) => ({ ...current, asset: event.target.value }))
                }
                placeholder="PETR4.SA"
                required
              />
            </label>

            <label>
              Regra
              <select
                value={form.rule}
                onChange={(event) =>
                  setForm((current) => ({ ...current, rule: event.target.value }))
                }
              >
                <option value="gte">Maior ou igual</option>
                <option value="lte">Menor ou igual</option>
              </select>
            </label>

            <label>
              Valor alvo
              <input
                type="number"
                step="0.01"
                value={form.value}
                onChange={(event) =>
                  setForm((current) => ({ ...current, value: event.target.value }))
                }
                required
              />
            </label>

            <button type="submit" className="primary-button" disabled={isSubmitting}>
              <Plus size={18} />
              {isSubmitting ? "Cadastrando..." : "Cadastrar"}
            </button>
          </form>

          <div className="divider" />

          <div className="panel-heading compact">
            <Bug size={18} />
            <h2>Teste manual</h2>
          </div>

          <form onSubmit={sendDebugQuote} className="debug-form">
            <input
              value={debugForm.asset}
              onChange={(event) =>
                setDebugForm((current) => ({ ...current, asset: event.target.value }))
              }
              placeholder="PETR4.SA"
              required
            />
            <input
              type="number"
              step="0.01"
              value={debugForm.price}
              onChange={(event) =>
                setDebugForm((current) => ({ ...current, price: event.target.value }))
              }
              required
            />
            <button type="submit" className="secondary-button" disabled={isSendingDebug}>
              <Send size={16} />
              Enviar
            </button>
          </form>
        </div>

        <section className="subscriptions-area">
          {subscriptions.length === 0 ? (
            <div className="empty-state">
              <Activity size={28} />
              <h2>Nenhum monitoramento ativo</h2>
              <p>Cadastre um ativo para abrir um WebSocket independente de alertas.</p>
            </div>
          ) : (
            subscriptions.map((subscription) => (
              <SubscriptionCard
                key={subscription.client_id}
                subscription={subscription}
                status={connectionStatus[subscription.client_id] || "connecting"}
                alerts={alertsByClient[subscription.client_id] || []}
                onDelete={() => deleteSubscription(subscription.client_id)}
              />
            ))
          )}
        </section>
      </section>
    </main>
  );
}

function Metric({ icon: Icon, label, value }) {
  return (
    <div className="metric">
      <Icon size={18} />
      <div>
        <strong>{value}</strong>
        <span>{label}</span>
      </div>
    </div>
  );
}

function SubscriptionCard({ subscription, status, alerts, onDelete }) {
  const connected = status === "connected";
  const ruleText = subscription.rule === "gte" ? "maior ou igual" : "menor ou igual";

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
