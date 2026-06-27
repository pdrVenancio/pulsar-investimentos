# Deploy Distribuido

Este projeto pode rodar em uma, duas ou tres maquinas. O ponto mais importante e nunca usar `localhost` entre maquinas diferentes. Use sempre o IP ou DNS da maquina que hospeda cada servico.

## Papeis

- `middleware`: Apache Pulsar, Admin API e WebSocket Proxy.
- `backend`: API FastAPI e ingestor.
- `frontend`: interface React.

## Portas Necessarias

Na maquina `middleware`:

```text
6650  Pulsar broker
8080  Pulsar Admin API e WebSocket Proxy
```

Na maquina `backend`:

```text
8000  API HTTP
```

Na maquina `frontend`:

```text
3000  React/Vite
```

## Opcao A: Uma Maquina So

Use o Compose principal:

```powershell
docker compose up --build
```

Acesse:

```text
http://localhost:3000
```

## Opcao B: Duas Maquinas

Modelo:

```text
Maquina 1: frontend
Maquina 2: backend + middleware
```

### Maquina 2: Backend + Middleware

Crie um `.env` nesta maquina:

```text
PULSAR_ADVERTISED_ADDRESS=IP_OU_DNS_DA_MAQUINA_2
PULSAR_WEB_ADVERTISED_ADDRESS=IP_OU_DNS_DA_MAQUINA_2
PULSAR_URL=pulsar://IP_OU_DNS_DA_MAQUINA_2:6650
PULSAR_ADMIN_URL=http://IP_OU_DNS_DA_MAQUINA_2:8080
CORS_ORIGINS=http://IP_OU_DNS_DA_MAQUINA_1:3000
```

Suba middleware:

```powershell
docker compose --env-file .env -f docker-compose.middleware.yml up --build
```

Em outro terminal, suba backend:

```powershell
docker compose --env-file .env -f docker-compose.backend.yml up --build
```

### Maquina 1: Frontend

Crie um `.env` nesta maquina:

```text
VITE_API_BASE_URL=http://IP_OU_DNS_DA_MAQUINA_2:8000
VITE_PULSAR_WS_BASE_URL=ws://IP_OU_DNS_DA_MAQUINA_2:8080
```

Suba frontend:

```powershell
docker compose --env-file .env -f docker-compose.frontend.yml up --build
```

Acesse:

```text
http://IP_OU_DNS_DA_MAQUINA_1:3000
```

## Opcao C: Tres Maquinas

Modelo:

```text
Maquina 1: middleware
Maquina 2: backend
Maquina 3: frontend
```

### Maquina 1: Middleware

Crie um `.env`:

```text
PULSAR_ADVERTISED_ADDRESS=IP_OU_DNS_DA_MAQUINA_1
PULSAR_WEB_ADVERTISED_ADDRESS=IP_OU_DNS_DA_MAQUINA_1
```

Suba:

```powershell
docker compose --env-file .env -f docker-compose.middleware.yml up --build
```

### Maquina 2: Backend

Crie um `.env`:

```text
PULSAR_URL=pulsar://IP_OU_DNS_DA_MAQUINA_1:6650
PULSAR_ADMIN_URL=http://IP_OU_DNS_DA_MAQUINA_1:8080
CORS_ORIGINS=http://IP_OU_DNS_DA_MAQUINA_3:3000
```

Suba:

```powershell
docker compose --env-file .env -f docker-compose.backend.yml up --build
```

### Maquina 3: Frontend

Crie um `.env`:

```text
VITE_API_BASE_URL=http://IP_OU_DNS_DA_MAQUINA_2:8000
VITE_PULSAR_WS_BASE_URL=ws://IP_OU_DNS_DA_MAQUINA_1:8080
```

Suba:

```powershell
docker compose --env-file .env -f docker-compose.frontend.yml up --build
```

Acesse:

```text
http://IP_OU_DNS_DA_MAQUINA_3:3000
```

## Por Que O `advertisedAddress` Importa

O cliente Pulsar conecta em `PULSAR_URL`, mas o broker devolve metadados com o endereco anunciado do broker. Se o Pulsar anunciar `localhost`, `pulsar` ou o hostname interno do container, outra maquina nao consegue conectar.

Por isso, na maquina do middleware, configure:

```text
PULSAR_ADVERTISED_ADDRESS=IP_OU_DNS_PUBLICO_DO_MIDDLEWARE
PULSAR_WEB_ADVERTISED_ADDRESS=IP_OU_DNS_PUBLICO_DO_MIDDLEWARE
```

## Teste Rapido Pelo Front

1. Abra o frontend.
2. Cadastre um monitoramento.
3. O frontend chama a API em `VITE_API_BASE_URL`.
4. A API cria a Pulsar Function no middleware via `PULSAR_ADMIN_URL`.
5. O ingestor publica em `raw-opportunities` via `PULSAR_URL`.
6. A Function publica em `alerts-{client_id}`.
7. O navegador recebe pelo WebSocket em `VITE_PULSAR_WS_BASE_URL`.
