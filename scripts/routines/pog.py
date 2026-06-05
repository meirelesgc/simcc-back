import argparse
import time

from sqlalchemy import text

from simcc.core.db.database import get_sync_session
from simcc.core.logging import get_logger

logger = get_logger('routines')


def main(researcher_id: str | None = None):
    session = next(get_sync_session())
    start_time = time.perf_counter()
    logger.info('pog_routine_started')

    rid = {'researcher_id': researcher_id}

    try:
        if researcher_id:
            session.execute(
                text(
                    r"UPDATE bibliographic_production SET year = NULL WHERE YEAR !~ '^\d+$' AND researcher_id = CAST(:researcher_id AS UUID);"
                ),
                rid,
            )
        else:
            session.execute(
                text(
                    r"UPDATE bibliographic_production SET year = NULL WHERE YEAR !~ '^\d+$';"
                )
            )

        session.execute(
            text(
                "UPDATE bibliographic_production_article ba SET qualis = 'A4' WHERE ba.issn = '17412242';"
            )
        )

        if researcher_id:
            session.execute(
                text(
                    'UPDATE researcher SET docente = true WHERE id = CAST(:researcher_id AS UUID) AND id IN (SELECT researcher_id FROM graduate_program_researcher);'
                ),
                rid,
            )
        else:
            session.execute(
                text(
                    'UPDATE researcher SET docente = true WHERE id IN (SELECT researcher_id FROM graduate_program_researcher);'
                )
            )

        session.execute(
            text("""
            UPDATE bibliographic_production_article p
            SET jcr=(subquery.jif2019), jcr_link=url_revista
            FROM (SELECT jif2019, eissn, url_revista FROM JCR) AS subquery
            WHERE translate(subquery.eissn,'-','') = p.issn
        """)
        )
        session.execute(
            text("""
            UPDATE bibliographic_production_article p
            SET jcr = (subquery.jif2019), jcr_link=url_revista
            FROM (SELECT jif2019, issn, url_revista FROM JCR) AS subquery
            WHERE translate(subquery.issn,'-','') = p.issn;
        """)
        )

        if researcher_id:
            session.execute(
                text(
                    'UPDATE bibliographic_production SET YEAR_ = YEAR::INTEGER WHERE researcher_id = CAST(:researcher_id AS UUID)'
                ),
                rid,
            )
            session.execute(
                text(
                    "UPDATE bibliographic_production SET title = translate(title, '''', ' ') WHERE researcher_id = CAST(:researcher_id AS UUID)"
                ),
                rid,
            )
        else:
            session.execute(
                text('UPDATE bibliographic_production SET YEAR_ = YEAR::INTEGER')
            )
            session.execute(
                text(
                    "UPDATE bibliographic_production SET title = translate(title, '''', ' ')"
                )
            )

        session.execute(
            text("""
            UPDATE periodical_magazine pm
            SET JCR = jcr.JIF2019
            FROM jcr
            WHERE REPLACE(jcr.issn, '-', '') = pm.issn
            AND pm.issn IS NOT NULL;
        """)
        )

        if researcher_id:
            session.execute(
                text(
                    "UPDATE researcher_production SET great_area_ = STRING_TO_ARRAY(great_area, ';') WHERE researcher_id = CAST(:researcher_id AS UUID);"
                ),
                rid,
            )
            session.execute(
                text("""
                WITH ranked AS (
                    SELECT id, ROW_NUMBER() OVER (PARTITION BY researcher_id, enterprise, start_year, end_year ORDER BY id) AS rn
                    FROM public.researcher_professional_experience
                    WHERE researcher_id = CAST(:researcher_id AS UUID)
                )
                DELETE FROM public.researcher_professional_experience WHERE id IN (SELECT id FROM ranked WHERE rn > 1);
            """),
                rid,
            )
            session.execute(
                text(
                    "UPDATE guidance SET title = regexp_replace(title, '<a[^>]*>|</a>', '', 'gi') WHERE researcher_id = CAST(:researcher_id AS UUID);"
                ),
                rid,
            )
        else:
            session.execute(
                text(
                    "UPDATE researcher_production SET great_area_ = STRING_TO_ARRAY(great_area, ';');"
                )
            )
            session.execute(
                text("""
                WITH ranked AS (
                    SELECT id, ROW_NUMBER() OVER (PARTITION BY researcher_id, enterprise, start_year, end_year ORDER BY id) AS rn
                    FROM public.researcher_professional_experience
                )
                DELETE FROM public.researcher_professional_experience WHERE id IN (SELECT id FROM ranked WHERE rn > 1);
            """)
            )
            session.execute(
                text(
                    "UPDATE guidance SET title = regexp_replace(title, '<a[^>]*>|</a>', '', 'gi');"
                )
            )

        session.execute(
            text("""
            UPDATE research_group rg SET second_leader_id = r.id FROM researcher r WHERE rg.second_leader = r.name;
            UPDATE research_group rg SET first_leader = r.id FROM researcher r WHERE rg.first_leader = r.name;
        """)
        )

        session.commit()
        duration = time.perf_counter() - start_time
        logger.info(
            'pog_routine_finished_successfully', duration=f'{duration:.2f}s'
        )
    except Exception as e:
        session.rollback()
        duration = time.perf_counter() - start_time
        logger.error(
            'pog_routine_failed', error=str(e), duration=f'{duration:.2f}s'
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--researcher-id', type=str, default=None)
    args = parser.parse_args()
    main(researcher_id=args.researcher_id)
