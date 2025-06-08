from langchain_openai import ChatOpenAI

from simcc.config import Settings
from simcc.repositories import conn


def list_graduate_programs():
    SCRIPT_SQL = """
        SELECT gpr.researcher_id AS id,
               JSONB_AGG(JSONB_BUILD_OBJECT(
                   'graduate_program_id', gp.graduate_program_id,
                   'name',gp.name
               )) AS graduate_programs
        FROM graduate_program_researcher gpr
        LEFT JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        GROUP BY gpr.researcher_id
    """
    return conn.select(SCRIPT_SQL)


def list_research_groups():
    SCRIPT_SQL = """
        SELECT r.id AS id,
               JSONB_AGG(JSONB_BUILD_OBJECT(
                   'group_id', rg.id, 'name', rg.name, 'area', rg.area,
                   'year', rg.year, 'institution_name', rg.institution_name
               )) AS research_groups
        FROM researcher r
        INNER JOIN research_group rg ON rg.second_leader_id = r.id OR rg.first_leader_id = r.id
        GROUP BY r.id
    """
    return conn.select(SCRIPT_SQL)


def list_foment_data():
    SCRIPT_SQL = """
        SELECT s.researcher_id AS id,
               JSONB_AGG(JSONB_BUILD_OBJECT(
                   'id', s.id, 'modality_name', s.modality_name,
                   'call_title', s.call_title,
                   'funding_program_name', s.funding_program_name,
                   'institute_name', s.institute_name
               )) AS subsidy
        FROM foment s
        GROUP BY s.researcher_id
    """
    return conn.select(SCRIPT_SQL)


def list_departament_data():
    SCRIPT_SQL = """
        SELECT dpr.researcher_id AS id,
               JSONB_AGG(JSONB_BUILD_OBJECT(
                   'dep_nom', dp.dep_nom, 'dep_sigla', dp.dep_sigla
               )) AS departments
        FROM ufmg.departament_researcher dpr
        LEFT JOIN ufmg.departament dp ON dpr.dep_id = dp.dep_id
        GROUP BY dpr.researcher_id;
    """
    return conn.select(SCRIPT_SQL)


def list_user_data():
    SCRIPT_SQL = """
        SELECT u.lattes_id,
               JSONB_BUILD_OBJECT(
                   'linkedin', u.linkedin, 'email', u.email,
                   'visible_email', u.visible_email
               ) AS user
        FROM admin.users u
        WHERE u.lattes_id IS NOT NULL;
    """
    return conn.select(SCRIPT_SQL)


def list_ufmg_data():
    SCRIPT_SQL = """
        SELECT researcher_id AS id, full_name, gender,
               job_title, academic_unit, department_name
        FROM ufmg.researcher;
    """
    return conn.select(SCRIPT_SQL)


print('Carregando dados contextuais...')
graduate_programs_map = {
    item['id']: item['graduate_programs'] for item in list_graduate_programs()
}
research_groups_map = {
    item['id']: item['research_groups'] for item in list_research_groups()
}
foment_map = {item['id']: item['subsidy'] for item in list_foment_data()}
department_map = {
    item['id']: item['departments'] for item in list_departament_data()
}
user_map = {item['lattes_id']: item['user'] for item in list_user_data()}
ufmg_map = {item['id']: item for item in list_ufmg_data()}
print('Dados carregados.')


model = ChatOpenAI(api_key=Settings().OPENAI_API_KEY)

SCRIPT_SQL_RESEARCHERS = """
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
researchers_to_process = conn.select(SCRIPT_SQL_RESEARCHERS)

print(f'Encontrados {len(researchers_to_process)} pesquisadores para processar.')

for i, researcher_data in enumerate(researchers_to_process):
    researcher_id = researcher_data.get('id')
    lattes_id = researcher_data.get('lattes_id')

    print(
        f'Processando pesquisador {i + 1}/{len(researchers_to_process)}: ID {researcher_id}'
    )

    researcher_name = researcher_data.get('name', 'N/A')
    researcher_abstract = researcher_data.get(
        'abstract', 'Sem resumo disponível.'
    )
    researcher_area = researcher_data.get('area', 'N/A')
    researcher_university = researcher_data.get('university', 'N/A')
    researcher_graduation = researcher_data.get('graduation', 'N/A')

    last_productions = conn.select(f"""
        SELECT bp.title, bp.type, bp.year FROM bibliographic_production bp
        WHERE bp.researcher_id = '{researcher_id}'
        ORDER BY bp.year DESC, bp.created_at DESC LIMIT 3
    """)

    professional_experiences = conn.select(f"""
        SELECT rpe.enterprise, rpe.start_year, rpe.end_year,
               rpe.functional_classification
        FROM researcher_professional_experience rpe
        WHERE rpe.researcher_id = '{researcher_id}'
        ORDER BY rpe.start_year DESC LIMIT 3
    """)

    education_history = conn.select(f"""
        SELECT e.degree, e.education_name, e.institution,
               e.education_start, e.education_end
        FROM education e WHERE e.researcher_id = '{researcher_id}'
        ORDER BY e.education_end DESC NULLS FIRST, e.education_start DESC NULLS FIRST
    """)

    researcher_grad_programs = graduate_programs_map.get(researcher_id, [])
    researcher_groups = research_groups_map.get(researcher_id, [])
    researcher_foment = foment_map.get(researcher_id, [])
    researcher_departments = department_map.get(researcher_id, [])
    researcher_user_info = user_map.get(lattes_id, {})
    researcher_ufmg_info = ufmg_map.get(researcher_id, {})

    prompt = f"""
    Por favor, elabore um resumo biográfico detalhado e coeso sobre o(a) pesquisador(a) a seguir, integrando todas as informações fornecidas para criar uma narrativa holística de sua carreira.

    **Perfil Principal:**
    - **Nome do Pesquisador:** {researcher_name}
    - **Universidade Principal:** {researcher_university}
    - **Maior Titulação:** {researcher_graduation}
    - **Área(s) de Pesquisa (Lattes):** {researcher_area}
    - **ID Lattes:** {lattes_id}
    - **Resumo (fornecido pelo pesquisador):** {researcher_abstract}
    """

    if researcher_departments:
        prompt += '\n**Afiliação Departamental:**\n'
        for dept in researcher_departments:
            prompt += f'- Departamento: {dept.get("dep_nom", "N/A")} ({dept.get("dep_sigla", "N/A")})\n'

    if researcher_grad_programs:
        prompt += '\n**Vínculo com Programas de Pós-Graduação:**\n'
        for prog in researcher_grad_programs:
            prompt += f'- Programa: {prog.get("name", "N/A")}\n'

    if researcher_ufmg_info and researcher_ufmg_info.get('job_title'):
        prompt += '\n**Informações Institucionais (UFMG):**\n'
        prompt += f'- Cargo: {researcher_ufmg_info.get("job_title", "N/A")}\n'
        prompt += f'- Unidade Acadêmica: {researcher_ufmg_info.get("academic_unit", "N/A")}\n'

    if last_productions:
        prompt += '\n**Produções Bibliográficas Mais Recentes:**\n'
        for prod in last_productions:
            prompt += f'- Título: {prod.get("title", "N/A")}, Tipo: {prod.get("type", "N/A")}, Ano: {prod.get("year", "N/A")}\n'

    if professional_experiences:
        prompt += '\n**Experiências Profissionais Recentes:**\n'
        for exp in professional_experiences:
            prompt += f'- Instituição: {exp.get("enterprise", "N/A")}, Função: {exp.get("functional_classification", "N/A")}, Período: {exp.get("start_year", "N/A")} - {exp.get("end_year", "Atual")}\n'

    if education_history:
        prompt += '\n**Trajetória Educacional:**\n'
        for edu in education_history:
            prompt += f'- Grau: {edu.get("degree", "N/A")} em {edu.get("education_name", "N/A")} pela {edu.get("institution", "N/A")} ({edu.get("education_start", "N/A")} - {edu.get("education_end", "Atual")})\n'

    if researcher_groups:
        prompt += '\n**Liderança em Grupos de Pesquisa:**\n'
        for group in researcher_groups:
            prompt += f'- Nome do Grupo: {group.get("name", "N/A")}, Área: {group.get("area", "N/A")}, Ano de Formação: {group.get("year", "N/A")}\n'

    if researcher_foment:
        prompt += '\n**Projetos de Fomento e Bolsas Recebidos:**\n'
        for item in researcher_foment:
            prompt += f'- Programa de Fomento: {item.get("funding_program_name", "N/A")}, Modalidade: {item.get("modality_name", "N/A")}, Chamada: {item.get("call_title", "N/A")}\n'

    if researcher_user_info and researcher_user_info.get('linkedin'):
        prompt += '\n**Contato Profissional:**\n'
        prompt += f'- LinkedIn: {researcher_user_info.get("linkedin")}\n'

    prompt += """
    \n**Instrução para Geração do Resumo:**
    Com base em TODOS os dados acima, construa um texto dissertativo e bem estruturado de aproximadamente 700 palavras. O resumo deve:
    1. Apresentar o(a) pesquisador(a), sua afiliação principal e sua área de especialização.
    2. Apresentar sua trajetória acadêmica e profissional, conectando suas experiências com suas linhas de pesquisa.
    3. Destacar suas contribuições científicas, mencionando os tipos de produção mais recentes.
    4. Incluir seu papel em programas de pós-graduação e a liderança de grupos de pesquisa, se houver.
    5. Mencionar o envolvimento em projetos financiados e bolsas, se aplicável, como um indicador de reconhecimento e impacto.
    6. Manter um tom formal e acadêmico, fornecendo uma visão que seja útil para a comunidade científica, potenciais colaboradores e o público em geral.
    7. Escreva de forma o mais objetiva possivel, evite juizo de valor ou elogios ao pesquisador, você deve ser estritamente descritivo.
    8. Não deve existir nenhum elogio direto ao pesquisador, é apenas a descrição do mesmo
    """

    try:
        response = model.invoke(prompt)
        if response.content:
            SCRIPT_UPDATE = """
                UPDATE researcher SET abstract_ai = %(abstract_ai)s
                WHERE id = %(id)s
            """
            conn.exec(
                SCRIPT_UPDATE,
                {'id': researcher_id, 'abstract_ai': response.content},
            )
            print(
                f'Resumo gerado e salvo com sucesso para o pesquisador ID: {researcher_id}.'
            )
        else:
            print(
                f'A resposta do modelo estava vazia para o pesquisador ID: {researcher_id}.'
            )
    except Exception as e:
        print(
            f'Ocorreu um erro ao processar o pesquisador ID {researcher_id}: {e}'
        )

print('Processamento concluído.')
