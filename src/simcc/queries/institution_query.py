from simcc.queries.base import BaseQuery
from simcc.repositories import tools


class InstitutionQuery(BaseQuery):
    def __init__(self, session, institution_id=None):
        super().__init__(session)
        self.institution_id = institution_id

    def build_sql(self) -> str:
        filters = ''
        if self.institution_id:
            self.params['institution_id'] = self.institution_id
            filters = 'AND i.id = :institution_id'

        return f"""
            WITH researcher_count AS (
              SELECT institution_id, COUNT(DISTINCT id)
                AS count_r
              FROM researcher
              GROUP BY institution_id
            ),
            graduate_program_count AS (
              SELECT institution_id, COUNT(DISTINCT graduate_program_id)
                AS count_gp
              FROM graduate_program
              GROUP BY institution_id
            ),
            graduate_program_researcher_count AS (
              SELECT gp.institution_id, SUM(gpr.count_r) AS count_gpr
              FROM graduate_program gp
              LEFT JOIN (
                SELECT graduate_program_id, COUNT(DISTINCT researcher_id)
                  AS count_r
                FROM graduate_program_researcher
                GROUP BY graduate_program_id
              ) gpr ON gpr.graduate_program_id = gp.graduate_program_id
              GROUP BY gp.institution_id
            ),
            graduate_program_student_count AS (
              SELECT gp.institution_id, SUM(gps.count_s) AS count_gps
              FROM graduate_program gp
              LEFT JOIN (
                SELECT graduate_program_id, COUNT(DISTINCT researcher_id)
                  AS count_s
                FROM graduate_program_student
                GROUP BY graduate_program_id
              ) gps ON gps.graduate_program_id = gp.graduate_program_id
              GROUP BY gp.institution_id
            ),
            ufmg_researcher_count AS (
              SELECT r.institution_id, COUNT(ur.researcher_id) AS count_d
              FROM ufmg.researcher ur
              LEFT JOIN researcher r ON r.id = ur.researcher_id
              GROUP BY r.institution_id
            ),
            technician_count AS (
              SELECT COUNT(*) AS count_t FROM ufmg.technician
            ),
            researchers AS (
              WITH ranked AS (
              SELECT lattes_id, institution_id, ROW_NUMBER() OVER (PARTITION BY institution_id ORDER BY random()) AS rn
              FROM researcher
              )
              SELECT institution_id, ARRAY_AGG(lattes_id) AS researchers_list
              FROM ranked
              WHERE rn <= 20
              GROUP BY institution_id
            )
            SELECT i.name, i.id, COALESCE(r.count_r, 0) AS count_r,
              COALESCE(gp.count_gp, 0) AS count_gp, COALESCE(gpr.count_gpr, 0)
              AS count_gpr, COALESCE(gps.count_gps, 0) AS count_gps,
              COALESCE(d.count_d, 0) AS count_d, COALESCE(t.count_t, 0)
              AS count_t, i.acronym, COALESCE(rl.researchers_list, ARRAY[]::TEXT[]) AS researchers
            FROM institution i
              LEFT JOIN researcher_count r
                ON r.institution_id = i.id
              LEFT JOIN graduate_program_count gp
                ON gp.institution_id = i.id
              LEFT JOIN graduate_program_researcher_count gpr
                ON gpr.institution_id = i.id
              LEFT JOIN graduate_program_student_count gps
                ON gps.institution_id = i.id
              LEFT JOIN ufmg_researcher_count d
                ON d.institution_id = i.id
              LEFT JOIN researchers rl
                ON rl.institution_id = i.id
              CROSS JOIN technician_count t
            WHERE 1 = 1
              AND i.acronym IS NOT NULL
              {filters};
        """


class RtMetricsQuery(BaseQuery):
    def __init__(self, session, entity_type: str):
        super().__init__(session)
        self.entity_type = entity_type  # 'researcher' or 'technician'

    def build_sql(self) -> str:
        table = f'ufmg.{self.entity_type}'
        return f"""
            SELECT work_regime AS rt, COUNT(*) AS count
            FROM {table}
            WHERE work_regime IS NOT NULL
            GROUP BY rt
        """


class InstitutionFrequencyQuery(BaseQuery):
    def __init__(self, session, terms: str, institution: str, type_: str):
        super().__init__(session)
        self.terms = terms
        self.institution = institution
        self.type_ = type_.upper()

    def build_sql(self) -> str:
        filter_terms = ''
        filter_inst = ''

        if self.institution and self.institution != 'None':
            self.params['institution'] = self.institution.split(';')
            filter_inst = ' AND i.name = ANY(:institution) '

        if self.type_ == 'SPEAKER':
            if self.terms:
                sql_terms, params = tools.websearch_filter(
                    'b.event_name', self.terms
                )
                filter_terms = self._format_websearch(sql_terms)
                self.params.update(params)

            return f"""
                SELECT COUNT(DISTINCT r.id) AS qtd, i.id, i.name AS institution, i.image
                FROM researcher r
                INNER JOIN institution i ON r.institution_id = i.id
                INNER JOIN participation_events b ON r.id = b.researcher_id
                WHERE i.acronym IS NOT NULL
                AND b.type_participation IN ('Apresentação Oral', 'Conferencista', 'Moderador', 'Simposista')
                {filter_inst}
                {filter_terms}
                GROUP BY i.id, i.name, i.image
                ORDER BY qtd DESC
            """

        elif self.type_ == 'ABSTRACT':
            if self.terms:
                sql_terms, params = tools.websearch_filter(
                    'r.abstract', self.terms
                )
                filter_terms = self._format_websearch(sql_terms)
                self.params.update(params)

            return f"""
                SELECT COUNT(DISTINCT r.id) AS qtd, i.id, i.name AS institution, i.image
                FROM researcher r
                INNER JOIN institution i ON r.institution_id = i.id
                WHERE i.acronym IS NOT NULL
                {filter_inst}
                {filter_terms}
                GROUP BY i.id, i.name, i.image
                ORDER BY qtd DESC
            """

        elif self.type_ in ['ARTICLE', 'BOOK']:
            if self.terms:
                sql_terms, params = tools.websearch_filter(
                    'b.title', self.terms
                )
                filter_terms = self._format_websearch(sql_terms)
                self.params.update(params)

            filter_type = f" AND b.type = '{self.type_}' "
            if self.type_ == 'BOOK':
                filter_type = (
                    " AND (b.type = 'BOOK' OR b.type = 'BOOK_CHAPTER') "
                )

            return f"""
                SELECT COUNT(r.id) AS qtd, i.id, i.name AS institution, i.image
                FROM researcher r
                INNER JOIN institution i ON r.institution_id = i.id
                INNER JOIN bibliographic_production b ON r.id = b.researcher_id
                WHERE i.acronym IS NOT NULL
                {filter_type}
                {filter_inst}
                {filter_terms}
                GROUP BY i.id, i.name, i.image
                ORDER BY qtd DESC
            """

        elif self.type_ == 'PATENT':
            if self.terms:
                sql_terms, params = tools.websearch_filter(
                    'b.title', self.terms
                )
                filter_terms = self._format_websearch(sql_terms)
                self.params.update(params)

            return f"""
                SELECT COUNT(DISTINCT b.title) AS qtd, i.id, i.name AS institution, i.image
                FROM researcher r
                INNER JOIN institution i ON r.institution_id = i.id
                INNER JOIN patent b ON r.id = b.researcher_id
                WHERE i.acronym IS NOT NULL
                {filter_inst}
                {filter_terms}
                GROUP BY i.id, i.name, i.image
                ORDER BY qtd DESC
            """

        elif self.type_ == 'AREA':
            if self.terms:
                sql_terms, params = tools.websearch_filter(
                    'rp.area_specialty', self.terms
                )
                filter_terms = self._format_websearch(sql_terms)
                self.params.update(params)

            return f"""
                SELECT COUNT(rp.researcher_id) AS qtd, i.id, i.name AS institution, i.image
                FROM researcher_production rp
                INNER JOIN researcher r ON r.id = rp.researcher_id
                INNER JOIN institution i ON i.id = r.institution_id
                WHERE 1 = 1
                {filter_inst}
                {filter_terms}
                GROUP BY i.id, i.name, i.image
                ORDER BY qtd DESC
            """

        return ''
