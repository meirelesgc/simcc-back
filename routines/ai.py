from langchain_openai import ChatOpenAI

from simcc.config import Settings
from simcc.repositories import conn

model = ChatOpenAI(api_key=Settings().OPENAI_API_KEY)
SCRIPT_SQL = """
    SELECT
        r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
        r.graduation, r.last_update AS lattes_update,
        REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
        i.image AS image_university, i.name AS university,
        rp.articles, rp.book_chapters, rp.book, rp.patent,
        rp.software, rp.brand, opr.h_index, opr.relevance_score,
        opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
        opr.openalex, r.classification, r.status, r.institution_id
    FROM researcher r
        LEFT JOIN institution i ON i.id = r.institution_id
        LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
        LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
    WHERE abstract_ai IS NULL
    """

result = conn.select(SCRIPT_SQL)

for _, researcher_data in enumerate(result):
    researcher_id = researcher_data.get('id')
    researcher_name = researcher_data.get('name', 'N/A')
    researcher_abstract = researcher_data.get(
        'abstract', 'Sem resumo disponível.'
    )
    researcher_area = researcher_data.get('area', 'N/A')
    researcher_university = researcher_data.get('university', 'N/A')
    researcher_graduation = researcher_data.get('graduation', 'N/A')
    lattes_id = researcher_data.get('lattes_id')

    SCRIPT_LAST_PRODUCTIONS = f"""
        SELECT
            bp.title,
            bp.type,
            bp.year
        FROM bibliographic_production bp
        WHERE bp.researcher_id = '{researcher_id}'
        ORDER BY bp.year DESC, bp.created_at DESC
        LIMIT 3
    """
    last_productions = conn.select(SCRIPT_LAST_PRODUCTIONS)

    SCRIPT_PROF_EXPERIENCE = f"""
        SELECT
            rpe.enterprise,
            rpe.start_year,
            rpe.end_year,
            rpe.employment_type,
            rpe.functional_classification
        FROM researcher_professional_experience rpe
        WHERE rpe.researcher_id = '{researcher_id}'
        ORDER BY rpe.start_year DESC
        LIMIT 3
    """
    professional_experiences = conn.select(SCRIPT_PROF_EXPERIENCE)

    SCRIPT_EDUCATION = f"""
        SELECT
            e.degree,
            e.education_name,
            e.institution,
            e.education_start,
            e.education_end
        FROM education e
        WHERE e.researcher_id = '{researcher_id}'
        ORDER BY e.education_end DESC NULLS FIRST, e.education_start DESC NULLS FIRST
    """
    education_history = conn.select(SCRIPT_EDUCATION)

    prompt = f"""
    Por favor, forneça um resumo abrangente do(a) pesquisador(a) a seguir, integrando seu perfil acadêmico, produções bibliográficas recentes, experiências profissionais e trajetória educacional.

    Nome do Pesquisador: {researcher_name}
    Universidade: {researcher_university}
    Maior Titulação: {researcher_graduation}
    Área(s) de Pesquisa: {researcher_area}
    ID Lattes: {lattes_id}
    Resumo: {researcher_abstract}

    Produções Bibliográficas Recentes:
    """
    if last_productions:
        for prod in last_productions:
            prompt += f'- Título: {prod.get("title", "N/A")}, Tipo: {prod.get("type", "N/A")}, Ano: {prod.get("year", "N/A")}\n'

    prompt += '\nExperiência Profissional:\n'
    if professional_experiences:
        for exp in professional_experiences:
            prompt += f'- Instituição: {exp.get("enterprise", "N/A")}, Função: {exp.get("functional_classification", "N/A")}, Início: {exp.get("start_year", "N/A")}, Fim: {exp.get("end_year", "Atual")}\n'

    prompt += '\nFormação Acadêmica:\n'
    if education_history:
        for edu in education_history:
            prompt += f'- Grau: {edu.get("degree", "N/A")} em {edu.get("education_name", "N/A")} pela {edu.get("institution", "N/A")} ({edu.get("education_start", "N/A")} - {edu.get("education_end", "Atual")})\n'

    prompt += '\nO resumo deve ser detalhado, fornecendo uma visão holística da carreira e das contribuições do(a) pesquisador(a), com foco em sua área de atuação, principais conquistas e envolvimento atual. O texto deve ter aproximadamente de 700 palavras..'

    response = model.invoke(prompt)
    if response.content:
        SCRIPT_SQL = """
            UPDATE researcher SET abstract_ai = %(abstract_ai)s
            WHERE id = %(id)s
            """
    print(_)
    conn.exec(SCRIPT_SQL, {'id': researcher_id, 'abstract_ai': response.content})
