# Diretrizes de Desenvolvimento SIMCC

A submissão de código exige conformidade com os documentos de referência. O acoplamento ou a transposição entre camadas causa a rejeição da implementação.

## Regras de Arquitetura

1. **Isolamento de Camadas**: O fluxo de requisições possui direção única. `Routers` invocam `Services`, `Services` orquestram `Repositories`, e `Repositories` executam `Queries`. O acesso ao banco de dados fora das instâncias de `Repositories` ou `Queries` constitui quebra de padrão. Consulte @docs/architecture.md .
2. **Filtros**: A inserção de parâmetros de busca exige o modelo de declaração de filtros. Consulte @docs/adding_filters.md .
3. **Testes**: A fusão de código no repositório depende da inclusão das categorias de testes Tier A e Tier B. Consulte @docs/testing.md .
4. **Inteligência Artificial**: A comunicação com modelos de linguagem e provedores ocorre por injeção de dependência. Consulte @docs/ai_architecture.md .

## Restrições de Escopo

1. **Modelos de Banco de Dados**: O agente **NUNCA** deve alterar os modelos do banco de dados (`src/simcc/core/db/model.py`) ou criar/modificar arquivos de migração (`migrations/versions/`). Mudanças estruturais no esquema do banco de dados estão fora do escopo de atuação do CLI e devem ser feitas manualmente.

## Regras de Ambiente e Execução

1. **Dependências**: O Poetry controla o ecossistema do projeto. A execução de comandos e scripts exige a instrução `poetry run` como prefixo.
2. **Validação de Código**: A execução da suíte de testes deve anteceder submissões de alterações em rotas, lógicas de serviço ou consultas de banco.
3. **Automação de Testes**: Utilize o comando `poetry run task test` para acionar a validação do código e obter a métrica de cobertura de arquivos (`pytest -s -x --cov=src/simcc -vv`).