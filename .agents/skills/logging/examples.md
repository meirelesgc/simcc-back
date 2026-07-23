# Biblioteca de Exemplos de Logging

Esta seção contém exemplos de código e os respectivos JSONs gerados no arquivo de logs para referência rápida.

---

## 1. Nova Rota HTTP (Geração Automática)

### Código do Router:
```python
from fastapi import APIRouter

router = APIRouter()

@router.get('/ping')
async def ping():
    return {'pong': True}
```

### JSON Produzido na Entrada (automaticamente):
```json
{
  "timestamp": "2026-07-22T17:23:33.444004Z",
  "level": "info",
  "application": "simcc",
  "environment": "development",
  "hostname": "srv-01",
  "category": "http",
  "event": "request.received",
  "message": "Request received: GET /ping",
  "request_id": "f6eeb62f-5e38-467c-a7c5-eebacb4725a7",
  "duration": null,
  "data": {
    "route": "/ping",
    "method": "GET",
    "user_id": null,
    "error_message": null
  }
}
```

### JSON Produzido na Saída (automaticamente):
```json
{
  "timestamp": "2026-07-22T17:23:33.445204Z",
  "level": "info",
  "application": "simcc",
  "environment": "development",
  "hostname": "srv-01",
  "category": "http",
  "event": "request.finished",
  "message": "Request finished: GET /ping",
  "request_id": "f6eeb62f-5e38-467c-a7c5-eebacb4725a7",
  "duration": 1.2,
  "data": {
    "route": "/ping",
    "method": "GET",
    "user_id": null,
    "error_message": null
  }
}
```

---

## 2. Nova Rotina CLI (Com Uso do Wrapper)

### Código do Script (`scripts/routines/sync_xml.py`):
```python
def main():
    # Processa os XMLs do CNPq
    print("Processando...")

if __name__ == '__main__':
    main()
```

### Execução via Terminal:
`python scripts/routines/run_routine.py sync_xml.py`

### JSON de Início da Rotina:
```json
{
  "timestamp": "2026-07-22T17:20:26.522185Z",
  "level": "info",
  "application": "simcc",
  "environment": "development",
  "hostname": "srv-01",
  "category": "routine",
  "event": "routine.started",
  "message": "Routine started: sync_xml",
  "request_id": null,
  "duration": null,
  "data": {
    "routine_name": "sync_xml",
    "error_message": null
  }
}
```

### JSON de Conclusão da Rotina:
```json
{
  "timestamp": "2026-07-22T17:20:28.522185Z",
  "level": "info",
  "application": "simcc",
  "environment": "development",
  "hostname": "srv-01",
  "category": "routine",
  "event": "routine.finished",
  "message": "Routine finished: sync_xml",
  "request_id": null,
  "duration": 2000.0,
  "data": {
    "routine_name": "sync_xml",
    "error_message": null
  }
}
```

---

## 3. Erro de Banco de Dados Interceptado (SQLAlchemy)

### Código do Repositório:
```python
async def fetch_invalid(session):
    # Executa consulta que gera erro de tabela não existente
    await session.execute(text("SELECT * FROM non_existent"))
```

### JSON Produzido no arquivo de log (automaticamente):
```json
{
  "timestamp": "2026-07-22T17:22:45.319268Z",
  "level": "error",
  "application": "simcc",
  "environment": "development",
  "hostname": "srv-01",
  "category": "database",
  "event": "query.error",
  "message": "Database query error in operation: database.query on db: test",
  "request_id": "f6eeb62f-5e38-467c-a7c5-eebacb4725a7",
  "duration": 3.32,
  "data": {
    "operation_name": "database.query",
    "database_name": "test",
    "error_message": "relation \"non_existent\" does not exist",
    "sql": null
  }
}
```
*(Nota: O SQL `SELECT * FROM non_existent` é omitido/retorna `null` no log pois o LOG_LEVEL da aplicação está configurado como INFO em produção).*

---

## 4. Registro de Novo Evento Customizado (Categoria `system`)

### 1. Adicionar constante em `constants.py`:
```python
class LogEvent(str, Enum):
    ...
    AUTH_LOGIN_FAILED = 'auth.login_failed'
```

### 2. Adicionar função helper em `events.py`:
```python
def auth_login_failed(email: str, reason: str, **kwargs) -> None:
    message = f"Failed login attempt for email: {email} (Reason: {reason})"
    logger.warning(
        LogEvent.AUTH_LOGIN_FAILED,
        message=message,
        category=LogCategory.SYSTEM,
        email=email,
        reason=reason,
        **kwargs
    )
```

### 3. Chamada no código de autenticação:
```python
from simcc.core.logging.events import auth_login_failed

if not verify_password(password, user.hash):
    auth_login_failed(email=user.email, reason="wrong_password")
```

### 4. JSON Produzido:
```json
{
  "timestamp": "2026-07-22T17:25:12.301294Z",
  "level": "warning",
  "application": "simcc",
  "environment": "development",
  "hostname": "srv-01",
  "category": "system",
  "event": "auth.login_failed",
  "message": "Failed login attempt for email: user@domain.com (Reason: wrong_password)",
  "request_id": "a9a3b02c-4919-482a-adab-92ba3df1d182",
  "duration": null,
  "data": {
    "route": "/api/v1/auth/login",
    "method": "POST",
    "user_id": null,
    "routine_name": null,
    "email": "user@domain.com",
    "reason": "wrong_password"
  }
}
```
*(Nota: Logs da categoria `system` contêm dinamicamente metadados herdados do contexto ativo, como `route` e `method` se preenchidos, além de chaves livres como `email` e `reason`).*
