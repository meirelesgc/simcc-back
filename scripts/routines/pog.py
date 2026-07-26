import argparse
import time

from sqlalchemy import text

from simcc.core.db.database import get_sync_session



items_found = 0
items_succeeded = 0
items_failed = 0


def main(researcher_ids=None, lattes_ids=None):
    global items_found, items_succeeded, items_failed
    session = next(get_sync_session())
    start_time = time.perf_counter()
    items_found = 13

    try:
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
            UPDATE research_group rg SET first_leader_id = r.id FROM researcher r WHERE rg.first_leader = r.name;
        """)
        )

        session.commit()
        items_succeeded = 13
        items_failed = 0
        duration = time.perf_counter() - start_time
    except Exception as e:
        items_succeeded = 0
        items_failed = 13
        session.rollback()
        duration = time.perf_counter() - start_time


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--researcher-ids',
        nargs='+',
        type=str,
        default=None,
    )
    parser.add_argument(
        '--lattes-ids',
        nargs='+',
        type=str,
        default=None,
    )
    args = parser.parse_args()

    main(researcher_ids=args.researcher_ids, lattes_ids=args.lattes_ids)
