# Arquitetura do Projeto

O SIMCC segue uma estrutura de camadas para garantir a separação de responsabilidades e escalabilidade.

## Camadas
1.  **Controllers (Routers)**: Gerenciam as rotas da API e injeção de dependências.
2.  **Services**: Orquestram a lógica de negócio e chamadas aos repositórios.
3.  **Repositories**: Camada fina para acesso a dados. Delegam queries complexas para os Query Objects.
4.  **Query Objects**: Classes especializadas em construir queries SQL dinâmicas (localizadas em `src/simcc/queries/`).

## Padrão Query Object
Para evitar que os Repositories se tornem "classes deus" gigantes, cada grande consulta dinâmica possui seu próprio objeto.

-   `ResearcherSearchQuery`: Gerencia a busca unificada de pesquisadores.
-   `AcademicDegreeMetricsQuery` & `GreatAreaMetricsQuery`: Gerenciam métricas analíticas.

Vantagem: Isolamento do SQL dinâmico e controle estrito sobre aliases de tabelas (ex: `rp.city`, `i.name`), facilitando refatorações de JOINS.

## Contratos de Paginação e Ordenação
Padronizamos a forma como a API lida com grandes volumes de dados através de schemas comuns em `src/simcc/schemas/common.py`.

### PaginationParams
-   `page`: Número da página (inicia em 1).
-   `lenght`: Quantidade de itens por página.
-   **Limite de Segurança**: O parâmetro `lenght` possui um limite máximo de **500 itens** por página para garantir a performance do banco de dados.

### SortParams
-   `sort_by`: Campo para ordenação.
-   `sort_order`: Direção (`asc` ou `desc`).
