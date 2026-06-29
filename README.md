# Pulsar Investimentos

Backend em Python que monitora preços de ativos com yFinance e envia alertas específicos por cliente via Apache Pulsar. Suporta dois modos de alerta: regras pontuais de preço via Pulsar Functions e detecção de padrões temporais via CEP Worker.

## Arquitetura

- `api`: serviço FastAPI que cria e remove assinaturas, faz deploy de Pulsar Functions via Admin REST API e expõe rotas de debug.
- `ingestor`: consome o tópico `desired-assets`, consulta o yFinance e publica cotações brutas em `raw-opportunities`. Mantém uma única task de polling por ativo, compartilhada entre todos os clientes que assinam aquele ativo.
- `pulsar_function`: recebe cotações brutas do `raw-opportunities`, compara com a regra do cliente (gte/lte) e retorna alertas para `alerts-{client_id}`. Uma instância é deployada por cliente em tempo de execução via Admin REST API.
- `cep_flink`: worker de Complex Event Processing que detecta padrões temporais (quedas consecutivas, altas consecutivas, queda percentual em janela de tempo) e publica alertas em `alerts-cep`.
- `pulsar`: Apache Pulsar standalone com Admin API e WebSocket proxy.

Não há banco de dados. O estado em tempo de execução é mantido apenas em memória e nos próprios tópicos Pulsar.

## Tópicos

```text
desired-assets        API → ingestor (subscribe/unsubscribe de ativos)
raw-opportunities     ingestor/debug → Pulsar Functions e CEP Worker
cep-subscriptions     API → CEP Worker (registro de padrões)
function-logs         mensagens de debug das Pulsar Functions
alerts-{client_id}    Pulsar Function → alerta pontual do cliente
alerts-cep            CEP Worker → alertas de padrão temporal
```

## Serviços Locais

Com o stack Docker rodando, use estas URLs base no Postman:

```text
HTTP API:                    http://localhost:8000
Pulsar Admin / WS Proxy:     http://localhost:8080
Frontend:                    http://localhost:3000
```

Para rodar em duas ou três máquinas, veja [DEPLOYMENT.md](./DEPLOYMENT.md).

---

## Alertas Pontuais (Pulsar Function)

Dispara quando o preço de um ativo cruza um valor fixo definido pelo cliente.

### Valores de Regra

```text
lte = menor ou igual
gte = maior ou igual
```

### 1. Criar uma Assinatura

```text
POST http://localhost:8000/subscriptions
Content-Type: application/json
```

```json
{
  "asset": "PETR4.SA",
  "rule": "gte",
  "value": 40.0
}
```

Resposta esperada:

```json
{
  "client_id": "20a4538d-8cc8-4810-8c2f-ee06c729ee36",
  "subscription_id": "b17b506e-62f6-4a6a-8522-a61e712f3a64",
  "alert_topic": "persistent://public/default/alerts-20a4538d-8cc8-4810-8c2f-ee06c729ee36"
}
```

Copie o `client_id` retornado.

### 2. Verificar o Status da Function

```text
GET http://localhost:8000/debug/functions/{client_id}
```

Verifique estes campos na resposta:

```json
{
  "function_name": "alert-20a4538d-8cc8-4810-8c2f-ee06c729ee36",
  "expected_input_topic": "persistent://public/default/raw-opportunities",
  "expected_alert_log_topic": "persistent://public/default/function-logs"
}
```

Se `runtime_status` contiver erros, a Function não está saudável.

### 3. Abrir o WebSocket de Alertas do Cliente

```text
ws://localhost:8080/ws/v2/consumer/persistent/public/default/alerts-{client_id}/postman-alert-sub
```

Clique em `Connect`. Este WebSocket recebe o alerta final retornado pela Pulsar Function.

### 4. Abrir o WebSocket de Debug da Function

```text
ws://localhost:8080/ws/v2/consumer/persistent/public/default/function-logs/postman-debug-sub
```

Este WebSocket recebe os estágios de execução da `AlertFunction`:

```text
stage=received          → mensagem chegou do raw-opportunities
stage=asset_filter      → comparou ativo recebido com ativo configurado
stage=ignored_asset     → ativo não corresponde, descartado
stage=comparison        → comparou price com target
stage=alert_returned    → regra satisfeita, alerta emitido
stage=no_alert_returned → ativo correspondeu mas regra não foi satisfeita
```

Se você ver `alert_returned` em `function-logs` mas nada em `alerts-{client_id}`, a Function funcionou corretamente — o problema está na URL ou na assinatura do WebSocket.

### 5. Publicar uma Cotação Bruta Manual

Use esta rota para testar a Function sem aguardar o intervalo de polling do yFinance:

```text
POST http://localhost:8000/debug/raw-quotes
Content-Type: application/json
```

```json
{
  "asset": "PETR4.SA",
  "price": 41.17
}
```

Após isso, verifique os dois WebSockets:

- `function-logs`: deve receber os estágios de debug.
- `alerts-{client_id}`: deve receber um alerta se a regra foi satisfeita.

### 6. Testar com Polling Real do yFinance

Após criar uma assinatura, o ingestor começa a consultar o yFinance para o ativo solicitado. O intervalo de polling é de **60 segundos** por ativo. Se o preço retornado satisfizer a regra, o WebSocket de alertas do cliente receberá um alerta.

### 7. Remover a Assinatura

```text
DELETE http://localhost:8000/subscriptions/{client_id}
```

Resposta esperada: `204 No Content`

Remove a Pulsar Function do cliente e instrui o ingestor a cancelar o polling desse ativo (se não houver outros clientes assinando o mesmo ativo).

---

## Alertas de Padrão Temporal (CEP Worker)

Dispara quando o histórico de preços de um ativo exibe um padrão recorrente — sem depender de um valor fixo de threshold.

### Padrões Disponíveis

| Padrão | Descrição | Parâmetros |
|---|---|---|
| `consecutive_drops` | N quedas consecutivas de preço | `count` (padrão: 3) |
| `consecutive_rises` | N altas consecutivas de preço | `count` (padrão: 3) |
| `pct_drop_window` | Queda percentual dentro de uma janela de tempo | `pct` (padrão: 2.0%), `window_secs` (padrão: 300s) |

### 1. Criar uma Assinatura CEP

```text
POST http://localhost:8000/cep-subscriptions
Content-Type: application/json
```

Exemplo — 3 quedas consecutivas em PETR4:

```json
{
  "asset": "PETR4.SA",
  "pattern": "consecutive_drops",
  "count": 3
}
```

Exemplo — queda de 2% ou mais nos últimos 5 minutos em VALE3:

```json
{
  "asset": "VALE3.SA",
  "pattern": "pct_drop_window",
  "pct": 2.0,
  "window_secs": 300
}
```

Resposta esperada:

```json
{
  "client_id": "a1b2c3d4-...",
  "subscription_id": "e5f6g7h8-...",
  "alert_topic": "persistent://public/default/alerts-cep"
}
```

> Diferente das assinaturas pontuais, todos os alertas CEP chegam no mesmo tópico `alerts-cep`, não em tópicos individuais por cliente.

### 2. Abrir o WebSocket de Alertas CEP

```text
ws://localhost:8080/ws/v2/consumer/persistent/public/default/alerts-cep/postman-cep-sub
```

### 3. Remover uma Assinatura CEP

```text
DELETE http://localhost:8000/cep-subscriptions/{client_id}
```

---

## Payloads WebSocket no Postman

As mensagens WebSocket do Pulsar chegam neste formato:

```json
{
  "messageId": "CA0QDSAAMAE=",
  "payload": "eyJhc3NldCI6ICJQRVRSNC5TQSIsICJwcmljZSI6IDQxLjE3fQ==",
  "publishTime": "2026-06-27T14:03:57.972Z",
  "redeliveryCount": 0
}
```

O campo `payload` é Base64. Decodifique para ver o conteúdo real.

Payload de alerta pontual após decodificação:

```json
{
  "asset": "PETR4.SA",
  "price": 41.17,
  "rule": "gte",
  "target_value": 40.0,
  "client_id": "20a4538d-8cc8-4810-8c2f-ee06c729ee36",
  "triggered_at": "2026-06-27T14:03:57.963917Z"
}
```

Payload de alerta CEP após decodificação:

```json
{
  "asset": "PETR4.SA",
  "client_id": "a1b2c3d4-...",
  "subscription_id": "e5f6g7h8-...",
  "last_price": 37.80,
  "triggered_at": "2026-06-27T14:05:00.000Z",
  "source": "cep-worker",
  "pattern": "consecutive_drops",
  "count": 3,
  "prices": [38.50, 38.10, 37.80]
}
```

---

## Resumo das Rotas

```text
POST   /subscriptions                    Cria assinatura pontual (Pulsar Function)
DELETE /subscriptions/{client_id}        Remove assinatura pontual
GET    /debug/functions/{client_id}      Status da Pulsar Function
POST   /debug/raw-quotes                 Publica cotação bruta manualmente

POST   /cep-subscriptions               Cria assinatura de padrão temporal (CEP)
DELETE /cep-subscriptions/{client_id}   Remove assinatura CEP
```

```text
ws://localhost:8080/ws/v2/consumer/persistent/public/default/alerts-{client_id}/postman-alert-sub
ws://localhost:8080/ws/v2/consumer/persistent/public/default/function-logs/postman-debug-sub
ws://localhost:8080/ws/v2/consumer/persistent/public/default/alerts-cep/postman-cep-sub
```