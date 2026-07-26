# Antipadrões de Logging a Evitar

Este guia serve como revisão técnica para agentes de IA e desenvolvedores prevenirem regressões no sistema de observabilidade.

---

## 1. Importar ou Chamar o Structlog Diretamente no Código de Negócio
*   **Por que é ruim**: Acopla o código de negócio com a biblioteca de log específica. Se decidirmos trocar o Structlog no futuro, teremos que alterar centenas de arquivos.
*   **Correção**: Use apenas o `logger` exposto por `from simcc.core.logging import logger` ou chame funções de `events.py`.

---

## 2. Registrar Instruções SQL Completas em Logs de Produção
*   **Por que é ruim**: Expõe parâmetros sensíveis (como hashes de senhas, e-mails, dados pessoais) e causa sobrecarga de armazenamento.
*   **Correção**: Garanta que o SQL completo seja anexado apenas sob nível `DEBUG` e de forma automatizada via os listeners do SQLAlchemy em `config.py`.

---

## 3. Declarar ou Enviar Eventos Usando Strings Cruas
*   **Por que é ruim**: Leva a inconsistências de nomenclatura (ex: um desenvolvedor usa `request_received` e outro usa `request.received`), quebrando filtros de busca no painel.
*   **Correção**: Registre novos eventos no enum `LogEvent` (`constants.py`) e crie uma função correspondente em `events.py`.

---

## 4. Gerar ou Duplicar o `request_id` Manualmente
*   **Por que é ruim**: Cria duplicidade e dificulta a correlação sequencial dos logs da mesma requisição.
*   **Correção**: Deixe que o middleware HTTP ou o wrapper de rotinas preencham e gerenciem o ciclo de vida do `request_id` dinamicamente usando `ContextVars`.

---

## 5. Colocar Chaves de Dados Dinâmicos fora do Dicionário `data`
*   **Por que é ruim**: Suja a raiz do JSON de logs, que deve ser mantido estritamente constante sob a versão do schema. Chaves dinâmicas na raiz quebram as tabelas dos coletores de logs estruturados.
*   **Correção**: Passe dados dinâmicos como keywords extras (que o processador moverá automaticamente para `data`) ou use explicitamente o parâmetro `data={...}` nas funções auxiliares.

---

## 6. Criar Novas Categorias Sem Documentar
*   **Por que é ruim**: Categorias desconhecidas poluem os filtros dos dashboards.
*   **Correção**: Mapeie qualquer nova categoria no enum `LogCategory` em `constants.py` e documente a sua utilização em `events.md`.
