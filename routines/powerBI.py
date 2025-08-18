import os
from csv import QUOTE_ALL
from datetime import datetime

import nltk
import pandas as pd

from simcc.repositories import conn, conn_admin

PATH = 'storage/powerBI'


def dim_titulacao():
    print('Dimensão da Tabela Titulação!')


def fat_area_specialty():
    SCRIPT_SQL = """
        SELECT DISTINCT asp.id AS area_specialty_id, researcher_id,
            asp.name AS area_specialty
        FROM researcher_area_expertise r
        INNER JOIN area_specialty asp ON asp.id = r.area_specialty_id;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_area_specialty.csv')
    csv.to_csv(csv_path)


def fat_great_area():
    SCRIPT_SQL = """
        SELECT gae.id AS great_area_id, researcher_id,
            REPLACE(gae.name, '_', ' ') AS name
        FROM great_area_expertise gae
            LEFT JOIN researcher_area_expertise r
                ON gae.id = r.great_area_expertise_id;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_great_area.csv')
    csv.to_csv(csv_path)


def dim_area_specialty():
    SCRIPT_SQL = """
        SELECT id, REPLACE(name, '_', ' ') AS name FROM area_specialty;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_area_specialty.csv')
    csv.to_csv(csv_path)


def dim_great_area():
    SCRIPT_SQL = """
        SELECT id, REPLACE(name, '_', ' ') AS name
        FROM great_area_expertise;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_great_area.csv')
    csv.to_csv(csv_path)


def fat_openalex_researcher():
    SCRIPT_SQL = """
        SELECT researcher_id, h_index, relevance_score, works_count,
            cited_by_count, i10_index, scopus, orcid, openalex
        FROM openalex_researcher;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_openalex_researcher.csv')
    csv.to_csv(csv_path)


def researcher_area_leader():
    SCRIPT_SQL = """
        SELECT id AS researcher_id, unnest(string_to_array(extra_field, ';')) AS area_leader
        FROM researcher;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'researcher_area_leader.csv')
    csv.to_csv(csv_path)


def fat_openalex_article():
    SCRIPT_SQL = """
        SELECT article_id, article_institution, issn, authors_institution,
            abstract, authors, language, citations_count, pdf, landing_page_url,
            keywords
        FROM public.openalex_article;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_openalex_article.csv')
    csv.to_csv(csv_path)


def dim_area_leader():
    SCRIPT_SQL = """
        SELECT DISTINCT extra_field AS area_leader
        FROM researcher;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_area_leader.csv')
    csv.to_csv(csv_path)


def npai():
    print('Imagem NPAI!')


def iapos():
    print('Imagem NPAI!')


def dim_city():
    SCRIPT_SQL = """
        SELECT c.id AS city_id, c.name
        FROM city c;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_city.csv')
    csv.to_csv(csv_path)


def ufmg_researcher():
    SCRIPT_SQL = """
        SELECT
        FROM ufmg.researcher;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    columns = [
        'researcher_id',
        'full_name',
        'gender',
        'status_code',
        'work_regime',
        'job_class',
        'job_title',
        'job_rank',
        'job_reference_code',
        'academic_degree',
        'organization_entry_date',
        'last_promotion_date',
        'employment_status_description',
        'department_name',
        'career_category',
        'academic_unit',
        'unit_code',
        'function_code',
        'position_code',
        'leadership_start_date',
        'leadership_end_date',
        'current_function_name',
        'function_location',
        'registration_number',
        'ufmg_registration_number',
        'semester_reference',
    ]
    csv = csv.reindex(columns, axis='columns', fill_value=0)
    csv_path = os.path.join(PATH, 'ufmg_researcher.csv')
    csv.to_csv(csv_path)


def DimensaoAno():
    print('Dimensão da Tabela Ano!')


def DimensaoTipoProducao():
    print('Dimensão da Tabela TipoProducao!')


def platform_image():
    print('Dimensão da Tabela Platform Image!')


def Qualis():
    print('Dimensão da Tabela Qualis!')


def dim_departament():
    SCRIPT_SQL = """
        SELECT dep_id, dep_nom, 'Escola de Engenharia' AS institution,
            'b24e043a-c6ff-446a-a85a-14d9f944a729' AS institution_id
        FROM ufmg.departament;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    columns = ['dep_id', 'dep_nom', 'institution', 'institution_id']
    csv = csv.reindex(columns, axis='columns', fill_value=0)
    csv_path = os.path.join(PATH, 'dim_departament.csv')
    csv.to_csv(csv_path)


def data():
    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d %H:%M:%S.%f')
    csv = pd.DataFrame({'data': [date_str]})
    csv_path = os.path.join(PATH, 'data.csv')
    csv.to_csv(csv_path)
    return date_str


def cimatec_graduate_program_student():
    SCRIPT_SQL = """
        SELECT researcher_id, graduate_program_id,
            EXTRACT(YEAR FROM CURRENT_DATE) AS year
        FROM graduate_program_student
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'cimatec_graduate_program_student.csv')
    csv.to_csv(csv_path)


def graduate_program_student_year_unnest():
    SCRIPT_SQL = """
        SELECT graduate_program_id, researcher_id, unnest(year) AS year
        FROM graduate_program_student;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'graduate_program_student_year_unnest.csv')
    csv.to_csv(csv_path)


def dim_graduate_program_acronym():
    SCRIPT_SQL = """
        SELECT graduate_program_id, acronym, name
        FROM graduate_program;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_graduate_program_acronym.csv')
    csv.to_csv(csv_path)


def graduate_program_researcher_year_unnest():
    SCRIPT_SQL = """
        SELECT graduate_program_id, researcher_id, unnest(year) AS year
        FROM graduate_program_researcher;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'graduate_program_researcher_year_unnest.csv')
    csv.to_csv(csv_path)


def dim_departament_technician():
    SCRIPT_SQL = """
        SELECT dep_id, technician_id
        FROM ufmg.departament_technician;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    print(csv)
    columns = ['dep_id', 'technician_id']
    csv.reindex(columns, axis='columns', fill_value=0)
    csv_path = os.path.join(PATH, 'dim_departament_technician.csv')
    csv.to_csv(csv_path)


def dim_departament_researcher():
    SCRIPT_SQL = """
        SELECT dep_id, researcher_id
        FROM ufmg.departament_researcher;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    columns = ['dep_id', 'researcher_id']
    csv = csv.reindex(columns, axis='columns', fill_value=0)
    csv_path = os.path.join(PATH, 'dim_departament_researcher.csv')
    csv.to_csv(csv_path)


def fat_group_leaders():
    SCRIPT_SQL = """
        SELECT id, name, institution, first_leader, first_leader_id,
            second_leader, second_leader_id, area, census,
            start_of_collection, end_of_collection, group_identifier, year,
            institution_name, category
        FROM research_group
        WHERE 1 = 1
            AND first_leader_id IS NOT NULL
            OR second_leader_id IS NOT NULL
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv.rename(columns={'area': 'AREA', 'year': 'YEAR'}, inplace=True)
    csv_path = os.path.join(PATH, 'fat_group_leaders.csv')
    csv.to_csv(csv_path)


def dim_research_group():
    SCRIPT_SQL = """
        SELECT rg.id AS group_id, TRANSLATE(rg.name, '"', '') AS group_name,
            rg.area, i.id AS institution_id
        FROM public.research_group rg
        RIGHT JOIN institution i ON rg.institution ILIKE '%' || i.acronym || '%';
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_research_group.csv')
    csv.to_csv(csv_path)


def dim_category_level_code():
    SCRIPT_SQL = """
        SELECT DISTINCT category_level_code
        FROM foment
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_category_level_code.csv')
    csv.to_csv(csv_path)


def fat_foment():
    SCRIPT_SQL = """
        SELECT r.id AS researcher_id, r.institution_id, modality_code,
            modality_name, category_level_code, funding_program_name,
            aid_quantity, scholarship_quantity
        FROM public.foment s
            LEFT JOIN researcher r ON r.id = s.researcher_id
        WHERE s.researcher_id IS NOT NULL;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_foment.csv')
    csv.to_csv(csv_path)


def fat_production_tecnical_year_novo_csv_db():
    SCRIPT_SQL = """
        SELECT DISTINCT
            title, development_year::int AS year, 'PATENTE' AS TYPE,
            p.researcher_id, c.id AS city_id, r.institution_id AS institution_id,
            unaccent(LOWER(title)) AS sanitized_title, p.id
        FROM patent p, researcher r, researcher_production rp, city c
        WHERE 1 = 1
            AND r.id = p.researcher_id
            AND rp.researcher_id = p.researcher_id
            AND rp.city = c.name

        UNION

        SELECT DISTINCT
            title, s.year AS year, 'SOFTWARE' AS TYPE, s.researcher_id, c.id,
            r.institution_id, unaccent(LOWER(title)) AS sanitized_title, s.id
        FROM software s, researcher r, researcher_production rp, city c
        WHERE 1 = 1
            AND r.id = s.researcher_id
            AND rp.researcher_id = s.researcher_id
            AND rp.city = c.name

        UNION

        SELECT DISTINCT
            title, b.year AS year, 'MARCA' AS TYPE, b.researcher_id, c.id,
            r.institution_id, unaccent(LOWER(title)) AS sanitized_title, b.id
        FROM brand b, researcher r, researcher_production rp, city c
        WHERE 1 = 1
            AND r.id = b.researcher_id
            AND rp.researcher_id = b.researcher_id
            AND rp.city = c.name

        UNION

        SELECT DISTINCT
            title, b.year AS year, 'RELATÓRIO TÉCNICO' AS TYPE, b.researcher_id,
            c.id, r.institution_id, unaccent(LOWER(title)) AS sanitized_title,
            b.id
        FROM research_report b, researcher r, researcher_production rp, city c
        WHERE 1 = 1
            AND r.id = b.researcher_id
            AND rp.researcher_id = b.researcher_id
            AND rp.city = c.name
        """

    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_production_tecnical_year_novo_csv_db.csv')
    csv.to_csv(csv_path)


def dim_institution():
    SCRIPT_SQL = """
        SELECT i.id AS institution_id, name, acronym
        FROM  institution i;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_institution.csv')
    csv.to_csv(csv_path)


def researcher_city():
    SCRIPT_SQL = """
        SELECT researcher_id, city
        FROM researcher_production;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'researcher_city.csv')
    csv.to_csv(csv_path)


def dim_researcher(origin: str):
    SCRIPT_SQL = f"""
        SELECT r.name AS researcher, r.id AS researcher_id,
            TO_CHAR(r.last_update,'dd/mm/yyyy') AS last_update,
            r.graduation AS graduation, r.institution_id, r.docente,
            regexp_replace(r.abstract, E'[\\n\\r]+', ' - ', 'g' ) AS abstract,
            '{origin}ResearcherData/Image?researcher_id=' || r.id AS image,
            r.orcid
        FROM researcher r
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        WITH unified_data AS (
            SELECT researcher_id, translate(title,'-\.:,;''', ' ') AS title
            FROM bibliographic_production
            UNION ALL
            SELECT researcher_id, translate(title,'-\.:,;''', ' ') AS title
            FROM patent
            UNION ALL
            SELECT researcher_id, translate(title,'-\.:,;''', ' ') AS title
            FROM brand
            UNION ALL
            SELECT researcher_id, translate(title,'-\.:,;''', ' ') AS title
            FROM event_organization
            UNION ALL
            SELECT researcher_id, translate(title,'-\.:,;''', ' ') AS title
            FROM software
        ),
        word_split AS (
            SELECT researcher_id, unnest(string_to_array(lower(regexp_replace(title, '[^a-zA-Z0-9\s]', '', 'g')), ' ')) AS word
            FROM unified_data
        ),
        word_count AS (
            SELECT researcher_id, word, COUNT(*) AS frequency
            FROM word_split
            WHERE word <> ''
            GROUP BY researcher_id, word
        ),
        ranked_words AS (
            SELECT researcher_id, word, frequency, RANK() OVER (PARTITION BY researcher_id ORDER BY frequency DESC) AS rank
            FROM word_count
        )
        SELECT researcher_id, STRING_AGG(word, ' | ') AS list_of_words
        FROM ranked_words
        WHERE 1 = 1
            AND rank <= 20
            AND CHAR_LENGTH(word) > 3
            AND TRIM(word) <> ALL(%(stopwords)s)
        GROUP BY researcher_id
        ORDER BY researcher_id;
        """  # noqa: E501

    result = conn.select(SCRIPT_SQL, parameters)
    list_words = pd.DataFrame(result)

    csv = csv.merge(list_words, on='researcher_id', how='left')
    csv_path = os.path.join(PATH, 'dim_researcher.csv')
    csv.to_csv(csv_path)


def fat_simcc_bibliographic_production():
    SCRIPT_SQL = """
        SELECT DISTINCT
            title, b.type as tipo, b.researcher_id, year, i.id AS institution_id,
            bar.qualis, bar.periodical_magazine_name, bar.jcr, bar.jcr_link,
            c.id AS city_id, b.id AS bibliographic_production_id,
            unaccent(LOWER(title)) AS sanitized_title, b.id
        FROM bibliographic_production b
        LEFT JOIN bibliographic_production_article bar
            ON b.id = bar.bibliographic_production_id, researcher r
        LEFT JOIN  institution i
            ON r.institution_id = i.id
        LEFT JOIN city c
            ON r.city_id = c.id
        WHERE 1 = 1
            AND b.researcher_id IS NOT NULL
            AND r.id =  b.researcher_id
        ORDER BY
            YEAR desc;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_simcc_bibliographic_production.csv')
    csv.to_csv(csv_path)


def production_tecnical_year():
    SCRIPT_SQL = """
        SELECT researcher_id, title, development_year::int AS YEAR,
            'PATENT' as type
        FROM patent
        UNION
        SELECT researcher_id,title,YEAR,'SOFTWARE'
        from software
        UNION
        SELECT researcher_id,title,YEAR,'BRAND'
        from brand
        UNION
        SELECT researcher_id,title,YEAR,'REPORT'
        from research_report
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'production_tecnical_year.csv')
    csv.to_csv(csv_path)


def researcher():
    SCRIPT_SQL = """
        SELECT r.name AS researcher, r.id AS researcher_id,
            TO_CHAR(r.last_update,'dd/mm/yyyy') AS last_update,
            r.graduation AS graduation, r.lattes_id, extra_field AS area_leader
        FROM  researcher r;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'researcher.csv')
    csv.to_csv(csv_path)


def article_qualis_year_institution():
    SCRIPT_SQL = """
        SELECT DISTINCT
            title, bar.qualis, bar.jcr, year, i.acronym as institution,
            rd.city as city, bar.jcr_link
        FROM bibliographic_production b, bibliographic_production_article bar,
            periodical_magazine pm, researcher r, institution i,
            researcher_address rd
        WHERE 1 = 1
            AND pm.id = bar.periodical_magazine_id
            AND r.id =  b.researcher_id
            AND r.institution_id = i.id
            AND b.id = bar.bibliographic_production_id
            AND rd.researcher_id = r.id
        GROUP BY title, bar.qualis, bar.jcr, year, i.acronym, rd.city,
            bar.jcr_link
        ORDER BY bar.qualis desc
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'article_qualis_year_institution.csv')
    csv.to_csv(csv_path)


def production_researcher():
    SCRIPT_SQL = """
        SELECT r.name AS researcher, r.id AS researcher_id,
            rp.articles AS articles, rp.book_chapters AS book_chapters,
            rp.book AS book, rp.work_in_event AS work_in_event,
            rp.great_area AS great_area, rp.area_specialty AS area_specialty,
            r.graduation as graduation
        FROM researcher_production rp, researcher r
        WHERE r.id = rp.researcher_id
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'production_researcher.csv')
    csv.to_csv(csv_path)


def article_qualis_year():
    SCRIPT_SQL = """
        SELECT DISTINCT
            title, bar.qualis, year, r.id AS researcher_id,
            r.name AS researcher, i.name AS institution, c.name AS city,
            pm.name AS name_magazine, pm.issn AS issn, bar.jcr as jcr,
            bar.jcr_link as jcr_link, b.type as type
        FROM bibliographic_production b
            LEFT JOIN (bibliographic_production_article bar
                LEFT JOIN periodical_magazine pm
                    ON pm.id = bar.periodical_magazine_id)
                ON b.id = bar.bibliographic_production_id,
            researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN city c ON c.id = r.city_id
        WHERE r.id =  b.researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'article_qualis_year.csv')
    csv.to_csv(csv_path)


def production_year_distinct():
    SCRIPT_SQL = """
        SELECT DISTINCT
            year, title, type AS tipo, i.acronym AS institution
        FROM bibliographic_production AS b, institution i, researcher r
        WHERE 1 = 1
            AND r.id = b.researcher_id
            AND r.institution_id = i.id
        GROUP BY year, title, tipo, i.acronym
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'production_year_distinct.csv')
    csv.to_csv(csv_path)


def production_year():
    SCRIPT_SQL = """
        SELECT DISTINCT
            b.title,
            b.type AS tipo,
            b.researcher_id,
            b.year,
            i.name AS institution
        FROM bibliographic_production AS b
        LEFT JOIN researcher r ON r.id = b.researcher_id
        LEFT JOIN institution i ON i.id = r.institution_id
        WHERE b.researcher_id IS NOT NULL
        GROUP BY b.title, b.type, b.researcher_id, b.year, i.name
        ORDER BY b.year, b.type DESC;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'production_year.csv')
    csv.to_csv(csv_path)


def production_coauthors_csv_db():
    SCRIPT_SQL = """
        SELECT COUNT(*) + 1 AS qtd, a.doi, a.title, ba.qualis, a.year,
            gp.graduate_program_id, gp.year AS year_pos, a.type
        FROM bibliographic_production a
            LEFT JOIN bibliographic_production_article ba
                ON a.id = ba.bibliographic_production_id,
            bibliographic_production b, graduate_program_researcher gp
        WHERE 1 = 1
            AND (a.doi = b.doi OR a.title = b.title)
            AND a.researcher_id = gp.researcher_id
            AND b.researcher_id = gp.researcher_id
        GROUP BY a.doi, a.title, ba.qualis, a.year, gp.graduate_program_id,
            gp.year,a."type"
        HAVING COUNT(*) > 1
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'production_coauthors_csv_db.csv')
    csv.to_csv(csv_path)


def fat_researcher_ind_prod():
    SCRIPT_SQL = """
        SELECT researcher_id, YEAR, ind_prod_article, ind_prod_book,
            ind_prod_book_chapter, ind_prod_granted_patent,
            ind_prod_not_granted_patent, ind_prod_software, ind_prod_report,
            ind_prod_guidance
        FROM researcher_ind_prod;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_researcher_ind_prod.csv')
    csv.to_csv(
        csv_path,
        decimal=',',
        sep=';',
        index=False,
        encoding='UTF8',
        float_format=None,
    )


def graduate_program_ind_prod():
    SCRIPT_SQL = """
        SELECT graduate_program_id, YEAR, ind_prod_article, ind_prod_book,
            ind_prod_book_chapter, ind_prod_granted_patent,
            ind_prod_not_granted_patent, ind_prod_software, ind_prod_report,
            ind_prod_guidance
        FROM graduate_program_ind_prod;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'graduate_program_ind_prod.csv')
    csv.to_csv(
        csv_path,
        decimal=',',
        sep=';',
        index=False,
        encoding='UTF8',
        float_format=None,
    )


def researcher_production_novo_csv_db():
    SCRIPT_SQL = """
        SELECT title, qualis, year, r.id as researcher_id, r.name as researcher,
            bar.periodical_magazine_name as name_magazine, issn AS issn,
            jcr as jcr, jcr_link, b.type as type
        FROM bibliographic_production b
            LEFT JOIN  bibliographic_production_article bar
                ON b.id = bar.bibliographic_production_id,
                researcher r
        WHERE r.id = b.researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'researcher_production_novo_csv_db.csv')
    csv.to_csv(csv_path)


def article_distinct_novo_csv_db():
    SCRIPT_SQL = """
        SELECT DISTINCT
            title, bar.qualis, bar.jcr, b.year as year,
            gp.graduate_program_id as graduate_program_id, b.year as year_pos,
            periodical_magazine.name AS type
        FROM bibliographic_production b
            LEFT JOIN bibliographic_production_article bar
                ON b.id = bar.bibliographic_production_id
            LEFT JOIN periodical_magazine
                ON periodical_magazine.id = bar.periodical_magazine_id,
            researcher r, graduate_program_researcher gpr, graduate_program gp
        WHERE gpr.graduate_program_id = gp.graduate_program_id
            AND gpr.researcher_id = r.id
            AND r.id = b.researcher_id
            AND b.year::INT = ANY(gpr.year)
            AND b.type = 'ARTICLE'
            --- AND gpr.type_ = 'PERMANENTE'
        ORDER BY qualis desc
    """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'article_distinct_novo_csv_db.csv')
    csv.to_csv(csv_path)


def production_distinct_novo_csv_db():
    SCRIPT_SQL = """
        SELECT DISTINCT
            title, qualis,jcr, b.year AS year,
            gp.graduate_program_id AS graduate_program_id, b.year as year_pos,
            b.type AS type
        FROM bibliographic_production b
            LEFT JOIN  bibliographic_production_article bar
                ON b.id = bar.bibliographic_production_id,
            researcher r, graduate_program_researcher gpr, graduate_program gp
        WHERE gpr.graduate_program_id = gp.graduate_program_id
            AND gpr.researcher_id = r.id
            AND r.id = b.researcher_id
            AND b.year::INT = ANY(gpr.year)
        order by qualis desc
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'production_distinct_novo_csv_db.csv')
    csv.to_csv(csv_path)


def cimatec_graduate_program_researcher():
    SCRIPT_SQL = """
    SELECT researcher_id, graduate_program_id,
        EXTRACT(YEAR FROM CURRENT_DATE) AS year, type_
    FROM graduate_program_researcher;
    """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'cimatec_graduate_program_researcher.csv')
    csv.to_csv(csv_path)


def cimatec_graduate_program():
    SCRIPT_SQL = """
        SELECT gp.graduate_program_id, gp.code, gp.name, gp.area, gp.modality,
            gp.type, gp.rating, i.id AS institution_id, i.name AS institution,
            gp.city
        FROM graduate_program gp
            LEFT JOIN institution i
                ON i.id = gp.institution_id
        WHERE visible IS True
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'cimatec_graduate_program.csv')
    csv.to_csv(csv_path)


def dim_research_project():
    SCRIPT_SQL = """
        SELECT id, researcher_id, start_year, end_year, agency_code, agency_name,
            TRANSLATE(project_name, ',', ' ') AS project_name, status, nature,
            number_undergraduates, TRANSLATE(description, ',', ' ') AS description,
            number_specialists, number_academic_masters, number_phd
        FROM research_project;
    """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)

    csv['start_year'] = (
        pd.to_numeric(csv['start_year'], errors='coerce').fillna(0).astype(int)
    )
    csv['end_year'] = (
        pd.to_numeric(csv['end_year'], errors='coerce').fillna(0).astype(int)
    )

    csv_path = os.path.join(PATH, 'dim_research_project.csv')
    csv.to_csv(csv_path, encoding='utf-8')


def fat_research_project_foment():
    SCRIPT_SQL = """
        SELECT project_id, agency_name, agency_code, nature
        FROM research_project_foment;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_research_project_foment.csv')
    csv.to_csv(csv_path)


def dim_bibliographic_production_terms():
    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        WITH unified_data AS (
            SELECT id, 'BIBLIOGRAPHIC_PRODUCTION' AS type_,
                translate(title,'-\.:,;''', ' ') AS title
            FROM bibliographic_production
        ),
        word_split AS (
            SELECT id, type_,
                unnest(
                string_to_array(
                lower(
                regexp_replace(title, '[^a-zA-Z0-9\s]', '', 'g')), ' ')) AS word
            FROM unified_data
        ),
        word_count AS (
            SELECT id, type_, word, COUNT(*) AS frequency
            FROM word_split
            WHERE word <> ''
            GROUP BY id, type_, word
        ),
        ranked_words AS (
            SELECT id, type_, word, frequency,
            RANK() OVER (PARTITION BY id ORDER BY frequency DESC) AS rank
            FROM word_count
        )
        SELECT id, type_, UNNEST(ARRAY_AGG(ranked_words.word)) AS term
        FROM ranked_words
        WHERE 1 = 1
            AND rank <= 20
            AND CHAR_LENGTH(word) > 3
            AND TRIM(word) <> ALL(%(stopwords)s)
        GROUP BY id, type_
        ORDER BY id;
        """
    result = conn.select(SCRIPT_SQL, parameters)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_bibliographic_production_terms.csv')
    csv.to_csv(csv_path)


def dim_tecnical_production_terms():
    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        WITH unified_data AS (
            SELECT id, 'PATENT' AS type_,
                translate(title,'-\.:,;''', ' ') AS title
            FROM patent
            UNION ALL
            SELECT id, 'BRAND', translate(title,'-\.:,;''', ' ') AS title
            FROM brand
            UNION ALL
            SELECT id, 'SOFTWARE', translate(title,'-\.:,;''', ' ') AS title
            FROM software
        ),
        word_split AS (
            SELECT id, type_,
                unnest(
                string_to_array(
                lower(
                regexp_replace(title, '[^a-zA-Z0-9\s]', '', 'g')), ' ')) AS word
            FROM unified_data
        ),
        word_count AS (
            SELECT id, type_, word, COUNT(*) AS frequency
            FROM word_split
            WHERE word <> ''
            GROUP BY id, type_, word
        ),
        ranked_words AS (
            SELECT id, type_, word, frequency,
                RANK() OVER (PARTITION BY id ORDER BY frequency DESC) AS rank
            FROM word_count
        )
        SELECT id, type_, UNNEST(ARRAY_AGG(ranked_words.word)) AS term
        FROM ranked_words
        WHERE 1 = 1
            AND rank <= 20
            AND CHAR_LENGTH(word) > 3
            AND TRIM(word) <> ALL(%(stopwords)s)
        GROUP BY id, type_
        ORDER BY id;
        """
    result = conn.select(SCRIPT_SQL, parameters)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_tecnical_production_terms.csv')
    csv.to_csv(csv_path)


def dim_logs_routine():
    SCRIPT_SQL = """
        SELECT unnest(enum_range(NULL::routine_type)) AS routine_type;
        """
    result = conn.select(SCRIPT_SQL)

    columns = ['routine_type']
    csv = pd.DataFrame(result, columns=columns)
    csv_path = os.path.join(PATH, 'dim_logs_routine.csv')
    csv.to_csv(csv_path)


def fat_logs_routine():
    SCRIPT_SQL = """
        SELECT DISTINCT ON (type) type, error, detail,
            created_at
        FROM logs.routine
        ORDER BY type, created_at DESC;
        """
    result = conn.select(SCRIPT_SQL)

    columns = ['type', 'error', 'detail', 'created_at']
    csv = pd.DataFrame(result, columns=columns)
    csv_path = os.path.join(PATH, 'fat_logs_routine.csv')
    csv.to_csv(csv_path)


def fat_event_organization():
    SCRIPT_SQL = """
        SELECT id, title, promoter_institution, nature,
            researcher_id, local, duration_in_weeks, year
        FROM public.event_organization;
        """

    result = conn.select(SCRIPT_SQL)

    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_event_organization.csv')
    csv.to_csv(csv_path)


def fat_participation_events():
    SCRIPT_SQL = """
        SELECT id, title, event_name, nature, form_participation,
            type_participation, researcher_id, year
        FROM public.participation_events;
        """

    result = conn.select(SCRIPT_SQL)

    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_participation_events.csv')
    csv.to_csv(csv_path)


def materialized_vision():
    SCRIPT_SQL = r"""
        SELECT id AS researcher_id, abstract AS search_term,
            UNACCENT(LOWER(TRANSLATE(abstract, $$-\".:,;'$$, ' ')))
            AS normalized_search_term, 'ABSTRACT' AS type,
            EXTRACT(YEAR FROM last_update) AS year
        FROM researcher
            UNION
        SELECT researcher_id, title AS search_term,
            UNACCENT(LOWER(TRANSLATE(title, $$-\.":,;'$$, ' ')))
            AS normalized_search_term, 'PATENT' AS type, development_year::INT
        FROM patent
            UNION
        SELECT researcher_id, title AS search_term,
            UNACCENT(LOWER(TRANSLATE(title, $$-\.":,;'$$, ' ')))
            AS normalized_search_term, type::VARCHAR, year_
        FROM bibliographic_production
            UNION
        SELECT researcher_id, title AS search_term,
            UNACCENT(LOWER(TRANSLATE(title, $$-\.:",;'$$, ' ')))
            AS normalized_search_term, 'REPORT' AS type, year
        FROM research_report
            UNION
        SELECT researcher_id, title AS search_term,
            UNACCENT(LOWER(TRANSLATE(title, $$-\.:,";'$$, ' ')))
                AS normalized_search_term, 'SOFTWARE' AS type, year
        FROM software
            UNION
        SELECT researcher_id, title AS search_term,
            UNACCENT(LOWER(TRANSLATE(title, $$-\.":,;'$$, ' ')))
            AS normalized_search_term, 'GUIDANCE' AS type, year
        FROM guidance
            UNION
        SELECT researcher_id, title AS search_term,
            UNACCENT(LOWER(TRANSLATE(title, $$-\.":,;'$$, ' ')))
            AS normalized_search_term, 'BRAND' AS type, year
        FROM brand
            UNION
        SELECT researcher_id, title AS search_term,
            UNACCENT(LOWER(TRANSLATE(title, $$-\.":,;'$$, ' ')))
            AS normalized_search_term, 'EVENT_ORGANIZATION' AS type,
            year
        FROM public.event_organization
            UNION
        SELECT researcher_id, project_name AS search_term,
            UNACCENT(LOWER(TRANSLATE(project_name, $$-\".:,;'$$, ' ')))
            AS normalized_search_term, 'RESEARCH_PROJECT' AS type, start_year
        FROM public.research_project;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'materialized_vision.csv')
    csv.to_csv(csv_path, index=True, quoting=QUOTE_ALL, encoding='utf-8-sig')


def fat_article_keyword_():
    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        WITH unified_data AS (
            SELECT bp.title AS title_, REGEXP_REPLACE(TRANSLATE(bp.title, $$-\.":,;'$$, ' '), '<[^>]*>', '', 'g') AS title, bp.researcher_id
            FROM bibliographic_production bp
            WHERE type = 'ARTICLE'
        ),
        split_words AS (
            SELECT title_ AS title, regexp_split_to_table(title, '\s+') AS word, researcher_id
            FROM unified_data
        ),
        normalized_words AS (
            SELECT title, lower(trim(word)) AS word, researcher_id
            FROM split_words
            WHERE word <> '' AND CHAR_LENGTH(word) > 3 AND lower(trim(word)) <> ALL(%(stopwords)s)
        )
        SELECT title, word, researcher_id AS researcher_id
        FROM normalized_words
        ORDER BY title
        """

    result = conn.select(SCRIPT_SQL, parameters)

    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_article_keyword_.csv')
    csv.to_csv(csv_path)


def dim_article_keyword():
    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        WITH unified_data AS (
            SELECT REGEXP_REPLACE(TRANSLATE(title, $$-\.":,;'$$, ' '), '<[^>]*>', '', 'g') AS title
            FROM bibliographic_production
            WHERE type = 'ARTICLE'
        ),
        split_words AS (
            SELECT regexp_split_to_table(title, '\s+') AS word
            FROM unified_data
        ),
        normalized_words AS (
            SELECT lower(trim(word)) AS word, COUNT(*) AS frequency
            FROM split_words
            WHERE word <> '' AND CHAR_LENGTH(word) > 3 AND lower(trim(word)) <> ALL(%(stopwords)s)
            GROUP BY word
            HAVING COUNT(*) > 5
        )
        SELECT word, frequency
        FROM normalized_words
        ORDER BY frequency DESC;
        """

    result = conn.select(SCRIPT_SQL, parameters)

    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_article_keyword.csv')
    csv.to_csv(csv_path)


def fat_article_co_authorship():
    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        SELECT title, UNNEST(ARRAY_AGG(researcher_id)) AS researcher_id
        FROM bibliographic_production
        WHERE type = 'ARTICLE'
        GROUP BY title
        HAVING COUNT(*) > 1
        ORDER BY title
        """

    result = conn.select(SCRIPT_SQL, parameters)

    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_article_co_authorship.csv')
    csv.to_csv(csv_path)


def fat_keywords_cooccurrences():
    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        WITH unified_data AS (
            SELECT REGEXP_REPLACE(TRANSLATE(title, $$-\.":,;'$$, ' '), '<[^>]*>', '', 'g') AS title
            FROM bibliographic_production
            WHERE type = 'ARTICLE'
        ),
        tokenized_titles AS (
            SELECT ROW_NUMBER() OVER () AS title_id,
                regexp_split_to_array(lower(trim(REGEXP_REPLACE(TRANSLATE(title, $$-\.":,;'$$, ' '), '<[^>]*>', '', 'g'))), '\s+') AS words
            FROM unified_data
        ),
        filtered_tokens AS (
            SELECT title_id,
                unnest(words) AS word
            FROM tokenized_titles
        ),
        cleaned_tokens AS (
            SELECT title_id,
                lower(trim(word)) AS word
            FROM filtered_tokens
            WHERE word <> '' AND CHAR_LENGTH(word) > 3 AND lower(trim(word)) <> ALL(%(stopwords)s)
        ),
        cooccurrences AS (
            SELECT LEAST(a.word, b.word) AS word1,
                GREATEST(a.word, b.word) AS word2
            FROM cleaned_tokens a
            JOIN cleaned_tokens b
                ON a.title_id = b.title_id AND a.word <> b.word
        ),
        cooc_count AS (
            SELECT word1, word2, COUNT(*) AS frequency
            FROM cooccurrences
            GROUP BY word1, word2
        )

        SELECT word1, word2, frequency
        FROM cooc_count
        ORDER BY frequency DESC
        """

    result = conn.select(SCRIPT_SQL, parameters)

    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_keywords_cooccurrences.csv')
    csv.to_csv(csv_path)


def fat_co_authorship():
    SCRIPT_SQL = r"""
        WITH co_articles AS (
            SELECT title, ARRAY_AGG(researcher_id) AS co_authors
            FROM bibliographic_production
            WHERE type = 'ARTICLE'
            GROUP BY title
        ),
        co_authors AS (
            SELECT co_articles.title, bp.researcher_id, UNNEST(co_authors) AS co_author
            FROM bibliographic_production bp
            INNER JOIN co_articles ON co_articles.title = bp.title
        )
        SELECT co_authors.title, co_authors.researcher_id, co_authors.co_author
        FROM co_authors
        WHERE co_authors.researcher_id != co_authors.co_author
        """

    result = conn.select(SCRIPT_SQL)

    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_co_authorship.csv')
    csv.to_csv(csv_path)


def _guidance():
    SCRIPT_SQL = """
        SELECT
            gt.id,
            r_student.lattes_id AS student_lattes_id,
            r_supervisor.lattes_id AS supervisor_lattes_id,
            r_co.lattes_id AS co_supervisor_lattes_id,
            gt.graduate_program_id,
            gt.start_date,
            gt.planned_date_project,
            gt.done_date_project,
            gt.planned_date_qualification,
            gt.done_date_qualification,
            gt.planned_date_conclusion,
            gt.done_date_conclusion,
            r_student.name AS student_name,
            r_supervisor.name AS supervisor_name,
            r_co.name AS co_name,
            gp.type AS type
        FROM public.guidance_tracking gt
        LEFT JOIN researcher r_student
            ON r_student.researcher_id = gt.student_researcher_id
        LEFT JOIN researcher r_supervisor
            ON r_supervisor.researcher_id = gt.supervisor_researcher_id
        LEFT JOIN researcher r_co
            ON r_co.researcher_id = gt.co_supervisor_researcher_id
        LEFT JOIN graduate_program gp
            ON gp.graduate_program_id = gt.graduate_program_id
        WHERE deleted_at IS NULL
    """
    guidance = conn_admin.select(SCRIPT_SQL)
    guidance = pd.DataFrame(
        guidance,
        columns=[
            'id',
            'student_lattes_id',
            'supervisor_lattes_id',
            'co_supervisor_lattes_id',
            'graduate_program_id',
            'start_date',
            'planned_date_project',
            'done_date_project',
            'planned_date_qualification',
            'done_date_qualification',
            'planned_date_conclusion',
            'done_date_conclusion',
            'student_name',
            'supervisor_name',
            'co_name',
            'type',
        ],
    )

    SCRIPT_SQL = """
        SELECT id AS researcher_id, lattes_id
        FROM researcher
    """
    researchers = conn.select(SCRIPT_SQL)
    researchers = pd.DataFrame(
        researchers, columns=['researcher_id', 'lattes_id']
    )

    guidance = (
        guidance.merge(
            researchers,
            left_on='student_lattes_id',
            right_on='lattes_id',
            how='left',
        )
        .rename(columns={'researcher_id': 'student_researcher_id'})
        .drop(columns=['lattes_id'])
    )

    guidance = (
        guidance.merge(
            researchers,
            left_on='supervisor_lattes_id',
            right_on='lattes_id',
            how='left',
        )
        .rename(columns={'researcher_id': 'supervisor_researcher_id'})
        .drop(columns=['lattes_id'])
    )

    guidance = (
        guidance.merge(
            researchers,
            left_on='co_supervisor_lattes_id',
            right_on='lattes_id',
            how='left',
        )
        .rename(columns={'researcher_id': 'co_supervisor_researcher_id'})
        .drop(columns=['lattes_id'])
    )

    csv = guidance.drop(
        columns=[
            'student_lattes_id',
            'supervisor_lattes_id',
            'co_supervisor_lattes_id',
        ]
    )
    return csv


def supervisor():
    df_original = _guidance()
    df_original['year'] = pd.to_datetime(
        df_original['planned_date_conclusion']
    ).dt.year
    df_original = df_original[['year', 'supervisor_researcher_id']]

    df_expanded = pd.DataFrame(
        [
            (supervisor, year)
            for supervisor, group in df_original.groupby(
                'supervisor_researcher_id'
            )
            for year in range(group['year'].min(), group['year'].max() + 1)
        ],
        columns=['supervisor_researcher_id', 'year'],
    )

    end_counts = (
        df_original.groupby(['supervisor_researcher_id', 'year'])
        .size()
        .reset_index(name='ended_in_year')
    )

    csv = pd.merge(
        df_expanded,
        end_counts,
        on=['supervisor_researcher_id', 'year'],
        how='left',
    ).fillna(0)

    csv = csv.sort_values(
        by=['supervisor_researcher_id', 'year'], ascending=[True, False]
    )
    csv['count'] = (
        csv.groupby('supervisor_researcher_id')['ended_in_year']
        .cumsum()
        .astype(int)
    )

    csv = csv[['supervisor_researcher_id', 'year', 'count']]
    csv = csv.sort_values(by=['supervisor_researcher_id', 'year']).reset_index(
        drop=True
    )
    csv_path = os.path.join(PATH, 'supervisor.csv')
    csv.to_csv(csv_path)


def guidance():
    today = datetime.now().date()
    csv: pd.DataFrame = _guidance()
    csv = csv.rename(columns={'type': 'program_type'})

    def peding_days(row):
        delays = []
        if row['done_date_conclusion'] is None:
            if row['planned_date_conclusion'] < today:
                days = (today - row['planned_date_conclusion']).days
                delays.append(days)
        if row['done_date_qualification'] is None:
            if row['planned_date_qualification'] < today:
                days = (today - row['planned_date_qualification']).days
                delays.append(days)
        if row['done_date_project'] is None:
            if row['planned_date_project'] < today:
                days = (today - row['planned_date_project']).days
                delays.append(days)
        if delays:
            return max(delays)
        return (row['planned_date_conclusion'] - today).days

    def peding_days_(row):
        delays = []
        if row['done_date_conclusion'] is None:
            if row['planned_date_conclusion'] < today:
                days = (today - row['planned_date_conclusion']).days
                delays.append(days)
        if row['done_date_qualification'] is None:
            if row['planned_date_qualification'] < today:
                days = (today - row['planned_date_qualification']).days
                delays.append(days)
        if row['done_date_project'] is None:
            if row['planned_date_project'] < today:
                days = (today - row['planned_date_project']).days
                delays.append(days)
        if delays:
            return max(delays)
        return 0

    def pending(row):
        if peding_days_(row) > 0:
            return 'EM ATRASO'
        return 'EM DIA'

    def type_(row):
        if row['done_date_project'] is None:
            return 'PROJETO'
        if row['done_date_qualification'] is None:
            return 'QUALIFICAÇÃO'
        if row['done_date_conclusion'] is None:
            return 'CONCLUSÃO'
        return 'FINALIZADO'

    csv['peding_days'] = csv.apply(peding_days, axis=1)
    csv['peding'] = csv.apply(pending, axis=1)
    csv['type'] = csv.apply(type_, axis=1)
    csv['days_offset'] = csv.apply(
        lambda row: (
            (row['done_date_conclusion'] - row['start_date'])
            if pd.notnull(row['done_date_conclusion'])
            else (row['planned_date_conclusion'] - row['start_date'])
            if pd.notnull(row['planned_date_conclusion'])
            else None
        ),
        axis=1,
    )
    csv['days_offset'] = csv['days_offset'].dt.days
    csv_path = os.path.join(PATH, 'guidance.csv')
    csv.to_csv(csv_path, index=True, quoting=QUOTE_ALL, encoding='utf-8-sig')


def guidance_per_year():
    csv: pd.DataFrame = _guidance()
    csv = csv.rename(columns={'type': 'program_type'})

    def type_(row):
        t = []
        if row['done_date_project'] is None:
            return ['PROJETO']
        if row['done_date_project'] is not None:
            t.append('PROJETO')
        if row['done_date_qualification'] is not None:
            t.append('QUALIFICAÇÃO')
            if row['done_date_conclusion'] is None:
                t.append('CONCLUIDO')
        if row['done_date_conclusion'] is not None:
            t = ['FINALIZADO']
        return t

    def status(row):
        if row['type'] == 'FINALIZADO':
            return 'FINALIZADO'
        return 'EM CURSO'

    def status_(row):
        if row['type'] == 'PROJETO':
            return (
                'REALIZADO'
                if row['done_date_project'] is not None
                else 'EM ANDAMENTO'
            )
        if row['type'] == 'QUALIFICAÇÃO':
            return (
                'REALIZADO'
                if row['done_date_qualification'] is not None
                else 'EM ANDAMENTO'
            )
        if row['type'] == 'CONCLUIDO':
            return (
                'REALIZADO'
                if row['done_date_conclusion'] is not None
                else 'EM ANDAMENTO'
            )
        if row['type'] == 'FINALIZADO':
            return 'REALIZADO'
        return 'EM ANDAMENTO'

    def year_(row):
        if row['type'] == 'PROJETO':
            date = row['done_date_project'] or row['planned_date_project']
            return date.year if date is not None else None
        if row['type'] == 'QUALIFICAÇÃO':
            date = (
                row['done_date_qualification']
                or row['planned_date_qualification']
            )
            return date.year if date is not None else None
        if row['type'] in {'CONCLUIDO', 'FINALIZADO'}:
            date = row['done_date_conclusion'] or row['planned_date_conclusion']
            return date.year if date is not None else None
        return None

    csv['type'] = csv.apply(type_, axis=1)
    csv = csv.explode('type')
    csv['status'] = csv.apply(status, axis=1)
    csv['status_'] = csv.apply(status_, axis=1)
    csv['year'] = csv.apply(year_, axis=1)
    csv = csv.sort_values(by='student_name')
    columns = [
        'id',
        'graduate_program_id',
        'student_name',
        'student_researcher_id',
        'supervisor_name',
        'supervisor_researcher_id',
        'co_name',
        'co_supervisor_researcher_id',
        'type',
        'status_',
        'year',
    ]
    csv_path = os.path.join(PATH, 'guidance_per_year.csv')
    csv = csv[columns]
    csv.to_csv(csv_path, index=True, quoting=QUOTE_ALL, encoding='utf-8-sig')


if __name__ == '__main__':
    for directory in [PATH]:
        if not os.path.exists(directory):
            os.makedirs(directory)
    dim_titulacao()
    fat_area_specialty()
    fat_great_area()
    dim_area_specialty()
    dim_great_area()
    fat_openalex_researcher()
    researcher_area_leader()
    fat_openalex_article()
    dim_area_leader()
    npai()
    iapos()
    dim_city()
    ufmg_researcher()
    DimensaoAno()
    DimensaoTipoProducao()
    platform_image()
    Qualis()
    dim_departament()
    data()
    cimatec_graduate_program_student()
    graduate_program_student_year_unnest()
    dim_graduate_program_acronym()
    graduate_program_researcher_year_unnest()
    dim_departament_technician()
    dim_departament_researcher()
    fat_group_leaders()
    dim_research_group()
    dim_category_level_code()
    fat_foment()
    fat_production_tecnical_year_novo_csv_db()
    dim_institution()
    researcher_city()
    dim_researcher('xpto')
    fat_simcc_bibliographic_production()
    production_tecnical_year()
    researcher()
    article_qualis_year_institution()
    production_researcher()
    article_qualis_year()
    production_year_distinct()
    production_year()
    production_coauthors_csv_db()
    fat_researcher_ind_prod()
    graduate_program_ind_prod()
    researcher_production_novo_csv_db()
    article_distinct_novo_csv_db()
    production_distinct_novo_csv_db()
    cimatec_graduate_program_researcher()
    cimatec_graduate_program()
    dim_research_project()
    fat_research_project_foment()
    dim_bibliographic_production_terms()
    dim_tecnical_production_terms()
    dim_logs_routine()
    fat_logs_routine()
    fat_event_organization()
    fat_participation_events()
    materialized_vision()
    fat_article_keyword_()
    dim_article_keyword()
    fat_article_co_authorship()
    fat_keywords_cooccurrences()
    guidance()
    supervisor()
    guidance_per_year()
