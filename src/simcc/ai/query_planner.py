from typing import List, Optional

from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field


class SearchFilters(BaseModel):
    institutions: List[str] = Field(
        default_factory=list,
        description="Lista de instituições ou siglas mencionadas, ex: ['UFBA'], ['UNEB'], ['UFBA', 'UNEB'], ['UEFS'], ['UESB'], ['UFRB']"
    )
    researcher_name: Optional[str] = Field(
        None,
        description="Nome específico de um pesquisador se a pergunta for sobre alguém em particular, ex: 'Eduardo Manuel de Freitas Jorge'"
    )
    city: Optional[str] = Field(
        None, description="Nome da cidade, ex: 'Salvador', 'Feira de Santana'"
    )
    year_from: Optional[int] = Field(
        None, description="Ano de início para o filtro temporal"
    )
    year_to: Optional[int] = Field(
        None, description="Ano de término para o filtro temporal"
    )


class QueryPlan(BaseModel):
    intent: str = Field(
        description="Intenção principal da consulta: 'researcher_profile' (perfil de indivíduo específico), 'researcher_comparison' (comparação entre instituições ou grupos), 'researcher_search' (busca temática/institucional de pesquisadores), 'production_search' (busca por produções), 'aggregation' (estatísticas/contagem) ou 'general_question' (fora do escopo)."
    )
    semantic_query: str = Field(
        description="Termos semânticos e conceituais para busca vetorial. Remova saudações e nomes de instituições. Mantenha os tópicos, áreas, especialidades e conceitos de pesquisa. Se for busca puramente por instituição sem tema (ex: 'pesquisadores da UEFS'), retorne string vazia."
    )
    filters: SearchFilters = Field(
        description="Filtros estruturados extraídos da consulta."
    )


PLANNER_SYSTEM_PROMPT = """
Você é o orquestrador de busca e planejamento de consultas (Query Planner) da MarIA, a assistente científica de dados de pesquisadores da Bahia.
Sua missão é converter a pergunta em linguagem natural do usuário em um plano estruturado de busca (`QueryPlan`).

Regras de Classificação de Intenção (`intent`):
1. `researcher_profile`: O usuário pergunta sobre um indivíduo específico (ex: "Quem é Eduardo Manuel...", "Qual o currículo de Fulano").
2. `researcher_comparison`: O usuário quer comparar pesquisadores de diferentes universidades ou áreas (ex: "Compare os pesquisadores da UFBA e UNEB em tecnologia").
3. `researcher_search`: O usuário quer localizar pesquisadores por área, tema, instituição ou experiência (ex: "Quais pesquisadores trabalham com IA?", "Pesquisadores da UNEB em linguística").
4. `production_search`: O usuário busca artigos, patentes ou produções específicas.
5. `aggregation`: Perguntas quantitativas ("Quantos doutores existem na UFBA?").
6. `general_question`: Cumprimentos ou perguntas sem relação com a base científica.

Regras para Filtros e Semantic Query:
- Identifique siglas de universidades baianas como UFBA, UNEB, UEFS, UESB, UFRB, IFBA, etc., e preencha em `institutions`.
- Extraia em `researcher_name` se houver menção a nome próprio de pesquisador.
- Na `semantic_query`, isole APENAS os conceitos temáticos, áreas de conhecimento, tópicos de pesquisa e termos técnicos.
- Expanda ligeiramente termos sinônimos relevantes se ajudar na recuperação semântica (ex: "ensino de línguas" -> "ensino de línguas letras vernáculas linguística").

Exemplos:
- "Quais pesquisadores da UNEB trabalham com linguística ou ensino de línguas?"
  -> intent: "researcher_search", institutions: ["UNEB"], semantic_query: "linguística ensino de línguas letras vernáculas"

- "Quais pesquisadores da Bahia trabalham com inteligência artificial, ciência de dados ou tecnologias digitais?"
  -> intent: "researcher_search", institutions: [], semantic_query: "inteligência artificial ciência de dados tecnologias digitais aprendizado de máquina"

- "Quem é Eduardo Manuel de Freitas Jorge e quais são suas principais áreas de atuação?"
  -> intent: "researcher_profile", researcher_name: "Eduardo Manuel de Freitas Jorge", institutions: [], semantic_query: "inteligência artificial ciência de dados computação inovação tecnológica"

- "Encontre pesquisadores que tenham experiência tanto em ensino quanto em gestão ou coordenação acadêmica."
  -> intent: "researcher_search", institutions: [], semantic_query: "ensino docência professor gestão coordenação acadêmica administrativa colegiado pesquisa"

- "Compare os pesquisadores da UFBA e da UNEB que trabalham com tecnologia e inovação."
  -> intent: "researcher_comparison", institutions: ["UFBA", "UNEB"], semantic_query: "tecnologia inovação desenvolvimento tecnológico engenharia computação"

- "Quem trabalha com filosofia da matemática na UFBA?"
  -> intent: "researcher_search", institutions: ["UFBA"], semantic_query: "filosofia da matemática lógica epistemologia"

- "Quais pesquisadores trabalham com vibrações e elementos finitos?"
  -> intent: "researcher_search", institutions: [], semantic_query: "vibrações elementos finitos mecânica dinâmica dos sólidos fluidos"
"""


class QueryPlanner:
    def __init__(self, api_key: str):
        self.llm = ChatOpenAI(api_key=api_key, model="gpt-4o-mini", temperature=0)
        self.structured_llm = self.llm.with_structured_output(QueryPlan)

        self.prompt = ChatPromptTemplate.from_messages([
            ("system", PLANNER_SYSTEM_PROMPT),
            ("human", "{question}")
        ])

        self.chain = self.prompt | self.structured_llm

    async def plan(self, question: str) -> QueryPlan:
        return await self.chain.ainvoke({"question": question})
