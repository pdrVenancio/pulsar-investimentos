# Pulsar Investments

Backend em Python que monitora preços de ativos com yFinance e envia alertas específicos por cliente via Apache Pulsar e Pulsar Functions.

## Arquitetura

- `api`: serviço FastAPI que cria/remove assinaturas, implanta/remove Pulsar Functions e expõe rotas de debug.
- `ingestor`: consome `desired-assets`, consulta o yFinance e publica cotações brutas em `raw-opportunities`.
- `pulsar_function`: recebe cotações brutas, compara com a regra do cliente e retorna alertas para `alerts-{client_id}`.
- `pulsar`: Apache Pulsar standalone com Admin API e WebSocket proxy.

Não há banco de dados. O estado em tempo de execução é mantido apenas em memória.

## Serviços Locais

Com o stack Docker rodando, use estas URLs base no Postman:

```text
HTTP API: http://localhost:8000
Pulsar Admin / WebSocket Proxy: http://localhost:8080
FrontEnd: http://localhost:3000
```

Para rodar em duas ou tres maquinas, veja [DEPLOYMENT.md](./DEPLOYMENT.md).

## Valores de Regra

Use o campo `rule` para escolher a comparação:

```text
lte = menor ou igual
gte = maior ou igual
```

Exemplos:

```json
{
  "asset": "PETR4.SA",
  "rule": "lte",
  "value": 38.5
}
```

```json
{
  "asset": "PETR4.SA",
  "rule": "gte",
  "value": 40.0
}
```

## Fluxo de Testes no Postman

### 1. Criar uma Assinatura

Crie uma requisição HTTP no Postman:

```text
POST http://localhost:8000/subscriptions
```

Headers:

```text
Content-Type: application/json
```

Body:

```json
{
  "asset": "PETR4.SA",
  "rule": "gte",
  "value": 4.0
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

Use esta rota para confirmar que a Pulsar Function foi criada e está em execução:

```text
GET http://localhost:8000/debug/functions/{client_id}
```

Exemplo:

```text
GET http://localhost:8000/debug/functions/20a4538d-8cc8-4810-8c2f-ee06c729ee36
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

Crie uma requisição `WebSocket` no Postman:

```text
ws://localhost:8080/ws/v2/consumer/persistent/public/default/alerts-{client_id}/postman-alert-sub
```

Exemplo:

```text
ws://localhost:8080/ws/v2/consumer/persistent/public/default/alerts-20a4538d-8cc8-4810-8c2f-ee06c729ee36/postman-alert-sub
```

Clique em `Connect`.

Este WebSocket recebe o alerta final retornado pela Pulsar Function.

### 4. Abrir o WebSocket de Debug da Function

Crie outra requisição `WebSocket` no Postman:

```text
ws://localhost:8080/ws/v2/consumer/persistent/public/default/function-logs/postman-debug-sub
```

Clique em `Connect`.

Este WebSocket recebe mensagens de debug da `AlertFunction`, incluindo:

```text
stage=received
stage=asset_filter
stage=comparison
stage=alert_returned
stage=no_alert_returned
```

### 5. Publicar uma Cotação Bruta Manual

Use esta rota para testar a Function sem aguardar o yFinance:

```text
POST http://localhost:8000/debug/raw-quotes
```

Headers:

```text
Content-Type: application/json
```

Body:

```json
{
  "asset": "PETR4.SA",
  "price": 41.17
}
```

Resposta esperada:

```json
{
  "raw_topic": "persistent://public/default/raw-opportunities",
  "payload": {
    "asset": "PETR4.SA",
    "price": 41.17,
    "timestamp": "2026-06-27T14:03:57.963917Z"
  }
}
```

Após isso, verifique os dois WebSockets:

- `function-logs`: deve receber os estágios de debug.
- `alerts-{client_id}`: deve receber um alerta se a regra foi satisfeita.

### 6. Testar com Polling Real do yFinance

Após criar uma assinatura, o ingestor começa a consultar o yFinance para o ativo solicitado.

O intervalo de polling é de 60 segundos por ativo. Se o preço real retornado pelo yFinance satisfizer a regra, o WebSocket de alertas do cliente receberá um alerta.

Use o WebSocket de debug para confirmar que a Function está recebendo e comparando as cotações reais.

### 7. Remover a Assinatura

Crie uma requisição HTTP:

```text
DELETE http://localhost:8000/subscriptions/{client_id}
```

Exemplo:

```text
DELETE http://localhost:8000/subscriptions/20a4538d-8cc8-4810-8c2f-ee06c729ee36
```

Resposta esperada:

```text
204 No Content
```

Isso remove a Pulsar Function do cliente e instrui o ingestor a cancelar a assinatura desse cliente no ativo.

## Payloads WebSocket no Postman

As mensagens WebSocket do Pulsar chegam neste formato:

```json
{
  "messageId": "CA0QDSAAMAE=",
  "payload": "eyJhc3NldCI6ICJQRVRSNC5TQSIsICJwcmljZSI6IDQxLjE3fQ==",
  "properties": {
    "__pfn_input_topic__": "persistent://public/default/raw-opportunities"
  },
  "publishTime": "2026-06-27T14:03:57.972Z",
  "redeliveryCount": 0
}
```

A mensagem real está dentro de `payload`, codificada em Base64. Decodifique `payload` para ver o alerta ou o log de debug.

Payload do alerta do cliente após decodificação Base64:

```json
{
  "asset": "PETR4.SA",
  "price": 41.17,
  "rule": "gte",
  "target_value": 4.0,
  "client_id": "20a4538d-8cc8-4810-8c2f-ee06c729ee36",
  "triggered_at": "2026-06-27T14:03:57.963917Z"
}
```

Payload de debug da Function após decodificação Base64:

```text
[2026-06-27 14:03:57 +0000] [INFO]: ALERT_FUNCTION_DEBUG {"stage": "comparison", "price": 41.17, "target": 4.0, "rule": "gte", "gte_result": true, "lte_result": false, "matched": true}
```

## Significado dos Estágios de Debug

Use o campo `stage` do payload de debug decodificado:

- `received`: a Function recebeu uma mensagem de `raw-opportunities`.
- `asset_filter`: a Function comparou o ativo recebido com o ativo configurado.
- `ignored_asset`: o ativo não corresponde, nenhum alerta é retornado.
- `comparison`: a Function comparou `price` com `target`.
- `alert_returned`: a regra foi satisfeita e um alerta foi retornado para `alerts-{client_id}`.
- `no_alert_returned`: o ativo correspondeu, mas a regra não foi satisfeita.

Se você ver `alert_returned` em `function-logs` mas nada em `alerts-{client_id}`, a comparação funcionou corretamente e o problema restante está na URL ou na assinatura do WebSocket do cliente.

## Tópicos Importantes

```text
desired-assets      API -> demanda de assinaturas do ingestor
raw-opportunities   ingestor/API de debug -> Pulsar Function
function-logs       mensagens de debug da Pulsar Function
alerts-{client_id}  Pulsar Function -> alertas do cliente
```

## Resumo das Rotas

```text
POST   http://localhost:8000/subscriptions
GET    http://localhost:8000/debug/functions/{client_id}
POST   http://localhost:8000/debug/raw-quotes
DELETE http://localhost:8000/subscriptions/{client_id}
```

```text
ws://localhost:8080/ws/v2/consumer/persistent/public/default/function-logs/postman-debug-sub
ws://localhost:8080/ws/v2/consumer/persistent/public/default/alerts-{client_id}/postman-alert-sub
```
