# Interface Contract: AI Chat Endpoints (MarIA)

## 1. POST `/ai/chat/ask` (Batch JSON)

### Request
```json
{
  "query": "Quais pesquisadores da UFBA trabalham com inteligência artificial?",
  "session_id": "sess_123456"
}
```

### Response 200 OK
```json
{
  "answer": "Na **UFBA**, encontramos pesquisadores de destaque atuando nas áreas de Inteligência Artificial e Ciência da Computação...\n\n- **Dr. Exemplo**: Foco em redes neurais e visão computacional.",
  "intent": "researcher_search",
  "filters_extracted": {
    "institutions": ["UFBA"],
    "production_types": []
  },
  "researchers": [
    {
      "id": "uuid-1",
      "name": "Dr. Exemplo",
      "institution": "Universidade Federal da Bahia",
      "institution_acronym": "UFBA",
      "lattes_id": "1234567890",
      "abstract": "Possui graduação em...",
      "semantic_content": "Inteligência Artificial..."
    }
  ],
  "productions": [],
  "sources": [
    "Dr. Exemplo (UFBA)"
  ]
}
```

### Response 503 Service Unavailable (Sem OPENAI_API_KEY ou Provedor Indisponível)
```json
{
  "detail": "O serviço de inteligência artificial está temporariamente indisponível ou não configurado."
}
```

---

## 2. POST `/ai/chat/ask/stream` (Server-Sent Events)

### Request
`Content-Type: application/json`
```json
{
  "query": "Patentes registradas na área de biotecnologia na Bahia",
  "session_id": "sess_789012"
}
```

### Streaming Sequence (`text/event-stream`)

#### Event 1: `metadata`
```text
data: {"type":"metadata","message_id":"sess_789012","data":{"intent":"production_search","filters":{"production_types":["PATENT"]},"researchers":[],"productions":[{"id":"pat-1","title":"Processo de extração...","type":"PATENT","year":"2023"}],"sources":["Processo de extração... [PATENT] (2023)"]}}

```

#### Event 2..N: `delta`
```text
data: {"type":"delta","message_id":"sess_789012","content":"Foram identificadas "}

data: {"type":"delta","message_id":"sess_789012","content":"patentes relevantes no ecossistema baiano..."}

```

#### Event Final: `done`
```text
data: {"type":"done","message_id":"sess_789012"}

```

#### Event em Caso de Erro: `error`
```text
data: {"type":"error","message_id":"sess_789012","code":"ai_unavailable","message":"O serviço de IA está temporariamente indisponível."}

```
