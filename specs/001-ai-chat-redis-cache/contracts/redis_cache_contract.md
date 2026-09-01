# Interface Contract: Redis Cache & Namespaces

## 1. Convenção de Nomes de Chaves (Namespaces)

Todas as chaves no Redis devem seguir o padrão:
`simcc:{subsystem}:{context}:{identifier}`

### Chaves de IA
- **Plano de Consulta**: `simcc:ai:plan:{hash_sha256(normalized_question)}`
- **Chat em Lote**: `simcc:ai:chat:batch:{hash_sha256(normalized_query_and_filters)}`
- **Chat Streaming**: `simcc:ai:chat:stream:{hash_sha256(normalized_query_and_filters)}`
- **Embeddings de Texto**: `simcc:ai:emb:{hash_sha256(normalized_text)}`

## 2. Estrutura dos Valores Serializados

### Chat em Lote (`simcc:ai:chat:batch:...`)
- **Tipo Redis**: `string` (JSON UTF-8)
- **TTL Padrão**: 3600 segundos (1 hora, parametrizável)
- **Schema**:
```json
{
  "answer": "string",
  "intent": "string",
  "filters_extracted": {},
  "researchers": [],
  "productions": [],
  "sources": [],
  "cached_at": "2026-09-01T12:00:00Z"
}
```

### Chat Streaming (`simcc:ai:chat:stream:...`)
- **Tipo Redis**: `string` (JSON array de eventos)
- **TTL Padrão**: 3600 segundos
- **Schema**:
```json
[
  {"type": "metadata", "data": {...}},
  {"type": "delta", "content": "..."},
  {"type": "done"}
]
```

## 3. Comportamento de Falha (Graceful Degradation)

- Timeout de conexão: 1.0 segundo.
- Em caso de falha de conexão (`ConnectionError`, `TimeoutError`, `RedisError`), o `CacheService` captura a exceção, registra log de aviso estruturado e retorna `None` (cache miss), permitindo que a aplicação processe a requisição normalmente via banco/LLM.
