import time
from uuid import uuid4

import pandas as pd
from sqlalchemy import text
from unidecode import unidecode

from simcc.core.db.database import get_sync_session
from simcc.core.logging import get_logger

logger = get_logger('routines')

barema = {
    'A1': 1,
    'A2': 0.875,
    'A3': 0.75,
    'A4': 0.625,
    'B1': 0.5,
    'B2': 0.375,
    'B3': 0.25,
    'B4': 0.125,
    'C': 0,
    'SQ': 0,
    'BOOK': 1,
    'BOOK_CHAPTER': 0.25,
    'SOFTWARE': 0.25,
    'PATENT_GRANTED': 1,
    'PATENT_NOT_GRANTED': 0.25,
    'REPORT': 0.25,
    'TESE DE DOUTORADO CONCLUIDA': 0.5,
    'TESE DE DOUTORADO EM ANDAMENTO': 0.25,
    'DISSERTACAO DE MESTRADO CONCLUIDA': 0.25,
    'DISSERTACAO DE MESTRADO EM ANDAMENTO': 0.125,
    'INICIACAO CIENTIFICA CONCLUIDA': 0.125,
    'INICIACAO CIENTIFICA EM ANDAMENTO': 0.1,
}


def article_indprod(session):
    SCRIPT_SQL = text("""
        SELECT year_ AS year, qualis, COUNT(*) AS count_article, researcher_id
        FROM bibliographic_production bp
        RIGHT JOIN bibliographic_production_article bpa
            ON bp.id = bpa.bibliographic_production_id
        GROUP BY year_, qualis, researcher_id;
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return [{'year': 0000, 'researcher_id': uuid4(), 'article_prod': 0}]

    columns = ['year', 'qualis', 'count_article', 'researcher_id']
    articles = pd.DataFrame(result, columns=columns)

    articles['article_prod'] = (
        articles['qualis'].map(barema) * articles['count_article']
    )
    articles = (
        articles
        .groupby(['year', 'researcher_id'])['article_prod']
        .sum()
        .reset_index()
    )

    columns = ['year', 'researcher_id', 'article_prod']
    articles = articles[columns]
    articles['year'] = articles['year'].astype(int)
    return articles.to_dict(orient='records')


def book_indprod(session):
    SCRIPT_SQL = text("""
        SELECT year, COUNT(*) AS count_book, researcher_id
        FROM bibliographic_production bp
        WHERE type = 'BOOK'
        GROUP BY year, researcher_id;
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return []
    books = pd.DataFrame(result)

    books['book_prod'] = books['count_book'] * barema.get('BOOK', 0)

    columns = ['year', 'researcher_id', 'book_prod']
    books = books[columns]
    books['year'] = books['year'].astype(int)
    return books.to_dict(orient='records')


def book_chapter_indprod(session):
    SCRIPT_SQL = text("""
        SELECT year, COUNT(*) AS count_book_chapter, researcher_id
        FROM bibliographic_production bp
        WHERE type = 'BOOK_CHAPTER'
        GROUP BY year, researcher_id;
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return []
    book_chapter = pd.DataFrame(result)

    book_chapter['book_chapter_prod'] = book_chapter[
        'count_book_chapter'
    ] * barema.get('BOOK_CHAPTER', 0)

    book_chapter['year'] = book_chapter['year'].astype(int)
    return book_chapter[
        ['year', 'researcher_id', 'book_chapter_prod']
    ].to_dict(orient='records')


def patent_indprod(session):
    SCRIPT_SQL = text("""
        SELECT development_year AS year, 'PATENT_GRANTED' AS granted,
            researcher_id, COUNT(*) as count_patent
        FROM patent p
        WHERE grant_date IS NOT NULL
        GROUP BY development_year, researcher_id

        UNION

        SELECT development_year AS year, 'PATENT_NOT_GRANTED' AS granted,
            researcher_id, COUNT(*) as count_patent
        FROM patent p
        WHERE grant_date IS NULL
        GROUP BY development_year, researcher_id
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return []
    columns = ['year', 'granted', 'researcher_id', 'count_patent']
    patent = pd.DataFrame(result, columns=columns)

    patent['patent_prod'] = (
        patent['granted'].map(barema) * patent['count_patent']
    )
    columns = ['patent_prod', 'year', 'researcher_id']
    patent = patent[columns]
    patent = (
        patent
        .groupby(['researcher_id', 'year'])['patent_prod']
        .sum()
        .reset_index()
    )
    patent['year'] = patent['year'].astype(int)
    return patent.to_dict(orient='records')


def software_indprod(session):
    SCRIPT_SQL = text("""
        SELECT year, COUNT(*) AS software_count, researcher_id
        FROM software
        GROUP BY year, researcher_id;
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return []
    columns = ['year', 'software_count', 'researcher_id']
    software = pd.DataFrame(result, columns=columns)
    software['software_prod'] = software['software_count'] * barema.get(
        'SOFTWARE', 0
    )
    columns = ['software_prod', 'year', 'researcher_id']
    software = software[columns]
    software['year'] = software['year'].astype(int)
    return software.to_dict(orient='records')


def report_indprod(session):
    SCRIPT_SQL = text("""
        SELECT year, COUNT(*) AS report_count, researcher_id
        FROM research_report
        GROUP BY year, researcher_id;
    """)

    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return [{'year': 0000, 'researcher_id': uuid4(), 'report_prod': 0}]

    report = pd.DataFrame(result)
    report['report_prod'] = report['report_count'] * barema.get('REPORT', 0)
    columns = ['year', 'researcher_id', 'report_prod']
    report = report[columns]
    report['year'] = report['year'].astype(int)
    return report.to_dict(orient='records')


def guidance_indprod(session):
    SCRIPT_SQL = text("""
        SELECT year, nature || ' ' || status AS nature_status,
            COUNT(*) AS guidance_count, researcher_id
        FROM guidance
        GROUP BY year, nature_status, researcher_id;
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return []
    guidance = pd.DataFrame(result)

    def normalise(nature_status):
        return unidecode(nature_status).upper()

    guidance['nature_status'] = guidance['nature_status'].apply(normalise)
    guidance['guidance_prod'] = (
        guidance['nature_status'].map(barema) * guidance['guidance_count']
    )
    guidance = (
        guidance
        .groupby(['year', 'researcher_id'])['guidance_prod']
        .sum()
        .reset_index()
    )
    columns = ['year', 'researcher_id', 'guidance_prod']
    guidance = guidance[columns]
    guidance['year'] = guidance['year'].astype(int)
    return guidance.to_dict(orient='records')


def list_researchers(session):
    SCRIPT_SQL = text("""
        SELECT id AS researcher_id
        FROM public.researcher;
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    return result


def list_programs(session):
    SCRIPT_SQL = text("""
        SELECT graduate_program_id, researcher_id, year
        FROM graduate_program_researcher
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    return result


def main():
    session = next(get_sync_session())
    start_time = time.perf_counter()
    logger.info('graduate_program_indprod_routine_started')

    try:
        current_year = 2026
        YEAR = range(2008, current_year + 1)
        history = pd.DataFrame(YEAR, columns=['year'])

        researchers = list_researchers(session)
        if not researchers:
            raise ValueError('No researchers found')
        researchers = pd.DataFrame(researchers)

        researchers = researchers.merge(history, how='cross')

        on = ['researcher_id', 'year']

        articles = pd.DataFrame(
            article_indprod(session),
            columns=['researcher_id', 'year', 'article_prod'],
        )
        researchers = researchers.merge(articles, on=on, how='left')

        books = pd.DataFrame(
            book_indprod(session),
            columns=['researcher_id', 'year', 'book_prod'],
        )
        researchers = researchers.merge(books, on=on, how='left')

        book_chapter = pd.DataFrame(
            book_chapter_indprod(session),
            columns=['researcher_id', 'year', 'book_chapter_prod'],
        )
        researchers = researchers.merge(book_chapter, on=on, how='left')

        software = pd.DataFrame(
            software_indprod(session),
            columns=['researcher_id', 'year', 'software_prod'],
        )
        researchers = researchers.merge(software, on=on, how='left')

        patent = pd.DataFrame(
            patent_indprod(session),
            columns=['researcher_id', 'year', 'patent_prod'],
        )
        researchers = researchers.merge(patent, on=on, how='left')

        report = pd.DataFrame(
            report_indprod(session),
            columns=['researcher_id', 'year', 'report_prod'],
        )
        researchers = researchers.merge(report, on=on, how='left')

        guidance = pd.DataFrame(guidance_indprod(session))
        if not guidance.empty:
            researchers = researchers.merge(guidance, on=on, how='left')
        else:
            researchers['guidance_prod'] = 0

        programs = list_programs(session)
        if not programs:
            raise ValueError('No programs found')
        programs = pd.DataFrame(programs)

        history_program = (
            programs[['graduate_program_id']]
            .drop_duplicates()
            .merge(history, how='cross')
        )

        programs = history_program.merge(
            programs, on=['graduate_program_id', 'year'], how='left'
        )

        programs = programs.merge(
            researchers, on=['researcher_id', 'year'], how='left'
        )

        programs = programs.drop(columns=['researcher_id'])

        programs = (
            programs
            .groupby(['graduate_program_id', 'year'])
            .sum()
            .reset_index()
            .sort_values(by=['graduate_program_id', 'year'])
        )

        programs = programs.fillna(0)

        session.execute(text('DELETE FROM graduate_program_ind_prod;'))

        query_insert = text("""
            INSERT INTO graduate_program_ind_prod (
                graduate_program_id, year,
                ind_prod_article, ind_prod_book, ind_prod_book_chapter,
                ind_prod_software, ind_prod_granted_patent,
                ind_prod_not_granted_patent, ind_prod_report, ind_prod_guidance
            ) VALUES (
                :graduate_program_id, :year,
                :article_prod, :book_prod, :book_chapter_prod,
                :software_prod, :patent_prod, :patent_prod_not,
                :report_prod, :guidance_prod
            );
        """)

        params = []
        for _, row in programs.iterrows():
            params.append({
                'graduate_program_id': str(row['graduate_program_id']),
                'year': int(row['year']),
                'article_prod': float(row['article_prod']),
                'book_prod': float(row['book_prod']),
                'book_chapter_prod': float(row['book_chapter_prod']),
                'software_prod': float(row['software_prod']),
                'patent_prod': float(row['patent_prod']),
                'patent_prod_not': float(row['patent_prod']),
                'report_prod': float(row['report_prod']),
                'guidance_prod': float(row['guidance_prod']),
            })

        BATCH_SIZE = 5000
        for i in range(0, len(params), BATCH_SIZE):
            batch = params[i : i + BATCH_SIZE]
            session.execute(query_insert, batch)

        session.commit()
        duration = time.perf_counter() - start_time
        logger.info(
            'graduate_program_indprod_routine_finished_successfully',
            duration=f'{duration:.2f}s',
        )
    except Exception as e:
        session.rollback()
        duration = time.perf_counter() - start_time
        logger.error(
            'graduate_program_indprod_routine_failed',
            error=str(e),
            duration=f'{duration:.2f}s',
        )


if __name__ == '__main__':
    main()
