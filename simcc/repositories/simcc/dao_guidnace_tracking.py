import uuid
from datetime import datetime

import pandas as pd
from pandas import NaT
from pydantic import UUID4

from ..dao import Connection

adm_database = Connection()


def get_all_guidance_trackings(data):
    filters = str()

    if data.get("supervisor_researcher_id"):
        filters += (
            "AND gt.supervisor_researcher_id = %(supervisor_researcher_id)s"
        )
    if data.get("graduate_program_id"):
        filters += "AND gt.graduate_program_id = %(graduate_program_id)s"
    
    if data.get("student_researcher_id"):
        filters += " AND gt.student_researcher_id = %(student_researcher_id)s"

    SCRIPT_SQL = f"""
        SELECT
            gt.id,
            gt.student_researcher_id,
            gt.supervisor_researcher_id,
            gt.graduate_program_id,
            gt.start_date,
            gt.planned_date_project,
            gt.done_date_project,
            gt.planned_date_qualification,
            gt.done_date_qualification,
            gt.planned_date_conclusion,
            gt.done_date_conclusion,
            COALESCE(ARRAY_AGG(DISTINCT gcs.co_supervisor_researcher_id)
                     FILTER (WHERE gcs.co_supervisor_researcher_id IS NOT NULL), '{{}}') as co_supervisor_ids,
            COALESCE(
                JSON_AGG(
                    DISTINCT JSONB_BUILD_OBJECT(
                        'id', t.id,
                        'name', t.name,
                        'color_code', t.color_code,
                        'created_at', t.created_at
                    )
                ) FILTER (WHERE t.id IS NOT NULL),
                '[]'
            ) as tags
        FROM
            guidance_tracking gt
        LEFT JOIN
            guidance_co_supervisors gcs ON gt.id = gcs.guidance_tracking_id
        LEFT JOIN
            guidance_tags gtg ON gt.id = gtg.guidance_tracking_id
        LEFT JOIN
            tags t ON gtg.tag_id = t.id
        WHERE
            gt.deleted_at IS NULL
            {filters}
        GROUP BY
            gt.id
        ORDER BY
            gt.created_at DESC;
    """
    records = adm_database.select(SCRIPT_SQL, data)

    columns = [
        "id",
        "student_researcher_id",
        "supervisor_researcher_id",
        "graduate_program_id",
        "start_date",
        "planned_date_project",
        "done_date_project",
        "planned_date_qualification",
        "done_date_qualification",
        "planned_date_conclusion",
        "done_date_conclusion",
        "co_supervisor_ids",
        "tags",
    ]
    df = pd.DataFrame(records, columns=columns)
    if df.empty:
        return []

    today = datetime.now().date()

    """

    def peding_days(student):

        is_mestrado = student["planned_date_project"] == student["start_date"]

        if not student["done_date_project"] and not is_mestrado:
            planned = student["planned_date_project"]
            return (planned - today).days if planned else None
        if not student["done_date_qualification"]:
            planned = student["planned_date_qualification"]
            return (planned - today).days if planned else None
        if not student["done_date_conclusion"]:
            planned = student["planned_date_conclusion"]
            return (planned - today).days if planned else None
        done_conclusion = student["done_date_conclusion"]
        if done_conclusion:
            return (today - done_conclusion).days

    def peding_days_(row):

        is_mestrado = row["planned_date_project"] == row["start_date"]

        delays = []
        if (
            row["done_date_conclusion"] is None
            and row["planned_date_conclusion"] < today
        ):
            delays.append((today - row["planned_date_conclusion"]).days)
        if (
            row["done_date_qualification"] is None
            and row["planned_date_qualification"] < today
        ):
            delays.append((today - row["planned_date_qualification"]).days)
        if (
            row["done_date_project"] is None
            and row["planned_date_project"] < today
            and not is_mestrado
        ):
            delays.append((today - row["planned_date_project"]).days)
        return max(delays) if delays else 0
 
    def pending(row):
        return "EM ATRASO" if peding_days_(row) > 0 else "EM DIA"

    def type_(row):

        is_mestrado = row["planned_date_project"] == row["start_date"]

        if row["done_date_project"] is None and not is_mestrado:
            return "PROJETO"
        if row["done_date_qualification"] is None:
            return "QUALIFICAÇÃO"
        if row["done_date_conclusion"] is None:
            return "CONCLUSÃO"
        return "FINALIZADO"

    """


    """
    def peding_days(student):
        # Se não houver data planejada para o projeto, pula direto para as próximas fases
        if not student["planned_date_project"]:
            if not student["done_date_qualification"]:
                planned = student["planned_date_qualification"]
                return (planned - today).days if planned else None
            if not student["done_date_conclusion"]:
                planned = student["planned_date_conclusion"]
                return (planned - today).days if planned else None
            done_conclusion = student["done_date_conclusion"]
            return (today - done_conclusion).days if done_conclusion else 0

        # Lógica padrão para quem tem projeto (Doutorado)
        if not student["done_date_project"]:
            planned = student["planned_date_project"]
            return (planned - today).days if planned else None
        if not student["done_date_qualification"]:
            planned = student["planned_date_qualification"]
            return (planned - today).days if planned else None
        if not student["done_date_conclusion"]:
            planned = student["planned_date_conclusion"]
            return (planned - today).days if planned else None
        done_conclusion = student["done_date_conclusion"]
        if done_conclusion:
            return (today - done_conclusion).days

    """
    def peding_days_(row):
        delays = []
        
        is_mestrado = (row["planned_date_project"] == row["start_date"]) or (row["planned_date_project"] == row["planned_date_qualification"]) or (row["planned_date_project"] is None)

        if row["done_date_conclusion"] is None and row["planned_date_conclusion"] < today:
            delays.append((today - row["planned_date_conclusion"]).days)
            
        if row["done_date_qualification"] is None and row["planned_date_qualification"] < today:
            delays.append((today - row["planned_date_qualification"]).days)
            
        # Só calcula atraso de projeto se NÃO for mestrado
        if not is_mestrado:
            if row["done_date_project"] is None and row["planned_date_project"] < today:
                delays.append((today - row["planned_date_project"]).days)
            
        return max(delays) if delays else 0

    def pending(row):
        return "EM ATRASO" if peding_days_(row) > 0 else "EM DIA"

    def type_(row):
        is_mestrado = (row["planned_date_project"] == row["start_date"]) or (row["planned_date_project"] == row["planned_date_qualification"]) or (row["planned_date_project"] is None)

        if is_mestrado:
            if row["done_date_qualification"] is None:
                return "QUALIFICACAO"  # Sem acento
            if row["done_date_conclusion"] is None:
                return "CONCLUSAO"    # Sem acento
            return "FINALIZADO"

        if row["done_date_project"] is None:
            return "PROJETO"
        if row["done_date_qualification"] is None:
            return "QUALIFICACAO"     # Sem acento
        if row["done_date_conclusion"] is None:
            return "CONCLUSAO"       # Sem acento
        return "FINALIZADO"
    
    print("DEBUG DATAS DO BANCO:\n", df[['planned_date_project', 'done_date_project']].to_string())

    df["peding_days"] = df.apply(peding_days_, axis=1)
    df["peding"] = df.apply(pending, axis=1)
    df["type"] = df.apply(type_, axis=1)
    df = df.replace(NaT, None)

    return df.to_dict(orient="records")


def get_guidance_tracking_by_id(guidance_id: UUID4):
    SCRIPT_SQL = """
        SELECT
            gt.id,
            gt.student_researcher_id,
            gt.supervisor_researcher_id,
            gt.graduate_program_id,
            gt.start_date,
            gt.planned_date_project,
            gt.done_date_project,
            gt.planned_date_qualification,
            gt.done_date_qualification,
            gt.planned_date_conclusion,
            gt.done_date_conclusion,
            gt.created_at,
            gt.updated_at,
            gt.deleted_at,
            COALESCE(ARRAY_AGG(DISTINCT gcs.co_supervisor_researcher_id)
                     FILTER (WHERE gcs.co_supervisor_researcher_id IS NOT NULL), '{{}}') as co_supervisor_ids,
            COALESCE(
                JSON_AGG(
                    DISTINCT JSONB_BUILD_OBJECT(
                        'id', t.id,
                        'name', t.name,
                        'color_code', t.color_code,
                        'created_at', t.created_at
                    )
                ) FILTER (WHERE t.id IS NOT NULL),
                '[]'
            ) as tags
        FROM
            guidance_tracking gt
        LEFT JOIN
            guidance_co_supervisors gcs ON gt.id = gcs.guidance_tracking_id
        LEFT JOIN
            guidance_tags gtg ON gt.id = gtg.guidance_tracking_id
        LEFT JOIN
            tags t ON gtg.tag_id = t.id
        WHERE gt.id = %(guidance_id)s AND gt.deleted_at IS NULL
        GROUP BY gt.id;
    """
    params = {"guidance_id": guidance_id}
    result = adm_database.select(SCRIPT_SQL, params)
    if not result:
        return {"message": "Registro não encontrado."}, 404

    columns = [
        "id",
        "student_researcher_id",
        "supervisor_researcher_id",
        "graduate_program_id",
        "start_date",
        "planned_date_project",
        "done_date_project",
        "planned_date_qualification",
        "done_date_qualification",
        "planned_date_conclusion",
        "done_date_conclusion",
        "created_at",
        "updated_at",
        "deleted_at",
        "co_supervisor_ids",
        "tags",
    ]
    df = pd.DataFrame(result, columns=columns)
    return df.to_dict(orient="records")[0], 200


def create_guidance_tracking(data: dict):
    co_supervisor_ids = data.pop("co_supervisor_ids", [])
    tag_ids = data.pop("tag_ids", [])
    new_guidance_id = str(uuid.uuid4())
    data["id"] = new_guidance_id

    # ----------------------------
    if data.get("tipo_programa") == "MESTRADO":
        # Forçamos a data de conclusão do projeto a ser igual à data de início
        # Assim o fluxo pula direto para "QUALIFICAÇÃO"
        data["done_date_project"] = data.get("start_date")
    # ----------------------------


    if len(tag_ids) != len(set(tag_ids)):
        raise ValueError("A lista de tags contém IDs repetidos.")

    try:
        SQL_INSERT_GUIDANCE = """
            INSERT INTO guidance_tracking (
                id, student_researcher_id, supervisor_researcher_id, graduate_program_id,
                start_date, planned_date_project, done_date_project,
                planned_date_qualification, done_date_qualification,
                planned_date_conclusion, done_date_conclusion
            ) VALUES (
                %(id)s, %(student_researcher_id)s, %(supervisor_researcher_id)s, %(graduate_program_id)s,
                %(start_date)s, %(planned_date_project)s, %(done_date_project)s,
                %(planned_date_qualification)s, %(done_date_qualification)s,
                %(planned_date_conclusion)s, %(done_date_conclusion)s
            );
        """
        adm_database.exec(SQL_INSERT_GUIDANCE, data)

        if co_supervisor_ids:
            SQL_INSERT_CO_SUPERVISOR = """
                INSERT INTO guidance_co_supervisors (guidance_tracking_id, co_supervisor_researcher_id)
                VALUES (%(guidance_id)s, %(co_supervisor_id)s);
            """
            for co_supervisor_id in co_supervisor_ids:
                params = {
                    "guidance_id": new_guidance_id,
                    "co_supervisor_id": co_supervisor_id,
                }
                adm_database.exec(SQL_INSERT_CO_SUPERVISOR, params)

        if tag_ids:
            SQL_INSERT_TAGS = """
                INSERT INTO guidance_tags (guidance_tracking_id, tag_id)
                VALUES (%(guidance_id)s, %(tag_id)s);
            """
            for tag_id in tag_ids:
                params = {"guidance_id": new_guidance_id, "tag_id": tag_id}
                adm_database.exec(SQL_INSERT_TAGS, params)

        return {
            "message": "Registro criado com sucesso.",
            "id": new_guidance_id,
        }, 201
    except ValueError as e:
        return {"message": f"Erro de Conflito: {e}"}, 409  # 409 Conflict
    except Exception as e:
        return {"message": f"Erro ao criar registro: {e}"}, 500


def update_guidance_tracking(guidance_id: UUID4, data: dict):
    co_supervisor_ids = data.pop("co_supervisor_ids", [])
    tag_ids = data.pop("tag_ids", [])
    params = data.copy()
    params["guidance_id"] = guidance_id

    if len(tag_ids) != len(set(tag_ids)):
        raise ValueError("A lista de tags contém IDs repetidos.")

    try:
        SQL_UPDATE_GUIDANCE = """
            UPDATE guidance_tracking SET
                student_researcher_id = %(student_researcher_id)s,
                supervisor_researcher_id = %(supervisor_researcher_id)s,
                graduate_program_id = %(graduate_program_id)s,
                start_date = %(start_date)s,
                planned_date_project = %(planned_date_project)s,
                done_date_project = %(done_date_project)s,
                planned_date_qualification = %(planned_date_qualification)s,
                done_date_qualification = %(done_date_qualification)s,
                planned_date_conclusion = %(planned_date_conclusion)s,
                done_date_conclusion = %(done_date_conclusion)s,
                updated_at = NOW()
            WHERE id = %(guidance_id)s AND deleted_at IS NULL;
        """
        adm_database.exec(SQL_UPDATE_GUIDANCE, params)

        adm_database.exec(
            "DELETE FROM guidance_co_supervisors WHERE guidance_tracking_id = %(guidance_id)s;",
            {"guidance_id": guidance_id},
        )
        adm_database.exec(
            "DELETE FROM guidance_tags WHERE guidance_tracking_id = %(guidance_id)s;",
            {"guidance_id": guidance_id},
        )

        if co_supervisor_ids:
            SQL_INSERT_CO_SUPERVISOR = """
                INSERT INTO guidance_co_supervisors (guidance_tracking_id, co_supervisor_researcher_id)
                VALUES (%(guidance_id)s, %(co_supervisor_id)s);
            """
            for co_supervisor_id in co_supervisor_ids:
                adm_database.exec(
                    SQL_INSERT_CO_SUPERVISOR,
                    {
                        "guidance_id": guidance_id,
                        "co_supervisor_id": co_supervisor_id,
                    },
                )

        if tag_ids:
            SQL_INSERT_TAGS = """
                INSERT INTO guidance_tags (guidance_tracking_id, tag_id)
                VALUES (%(guidance_id)s, %(tag_id)s);
            """
            for tag_id in tag_ids:
                adm_database.exec(
                    SQL_INSERT_TAGS,
                    {"guidance_id": guidance_id, "tag_id": tag_id},
                )

        return {"message": "Registro atualizado com sucesso."}, 200
    except ValueError as e:
        return {"message": f"Erro de Conflito: {e}"}, 409
    except Exception as e:
        return {"message": f"Erro ao atualizar registro: {e}"}, 500


def create_guidance_config(data: dict):
    SCRIPT_SQL = """
        INSERT INTO guidance_config (
            duration_project_months,
            duration_qualification_months,
            duration_conclusion_months,
            config_name
        ) VALUES (
            %(duration_project_months)s,
            %(duration_qualification_months)s,
            %(duration_conclusion_months)s,
            %(config_name)s);
    """
    adm_database.exec(SCRIPT_SQL, data)
    return {"message": "Configuração criada com sucesso."}, 201


def get_all_guidance_configs(data: dict):
    filters = str()

    if data.get("config_name"):
        filters += " AND config_name = %(config_name)s"

    SCRIPT_SQL = f"""
        SELECT
            id,
            duration_project_months,
            duration_qualification_months,
            duration_conclusion_months,
            config_name,
            created_at,
            updated_at
        FROM guidance_config
        WHERE 1=1
            {filters}
        ORDER BY created_at DESC;
    """
    records = adm_database.select(SCRIPT_SQL, data)
    columns = [
        "id",
        "duration_project_months",
        "duration_qualification_months",
        "duration_conclusion_months",
        "config_name",
        "created_at",
        "updated_at",
    ]
    df = pd.DataFrame(records, columns=columns)
    # Força conversão para string ou None
    df["created_at"] = (
        df["created_at"]
        .dt.strftime("%Y-%m-%d %H:%M:%S")
        .where(df["created_at"].notnull(), None)
    )
    df["updated_at"] = (
        df["updated_at"]
        .dt.strftime("%Y-%m-%d %H:%M:%S")
        .where(df["updated_at"].notnull(), None)
    )

    return df.to_dict(orient="records")


def update_guidance_config(data: dict):
    SCRIPT_SQL = """
        UPDATE guidance_config SET
            duration_project_months = %(duration_project_months)s,
            duration_qualification_months = %(duration_qualification_months)s,
            duration_conclusion_months = %(duration_conclusion_months)s,
            config_name = %(config_name)s,
            updated_at = NOW()
        WHERE id = %(config_id)s;
    """
    params = data.copy()
    params["config_id"] = data.get("id")
    adm_database.exec(SCRIPT_SQL, params)
    return {"message": "Configuração atualizada com sucesso."}, 200


def delete_guidance_config(config_id: UUID4):
    SCRIPT_SQL = """
        DELETE FROM guidance_config
        WHERE id = %(config_id)s;
    """
    params = {"config_id": config_id}
    adm_database.exec(SCRIPT_SQL, params)
    return {"message": "Configuração excluída com sucesso."}, 200


def delete_guidance_tracking(guidance_id: UUID4):
    SCRIPT_SQL = """ UPDATE guidance_tracking SET deleted_at = NOW() WHERE id = %(guidance_id)s AND deleted_at IS NULL; """
    params = {"guidance_id": guidance_id}
    adm_database.exec(SCRIPT_SQL, params)
    return {"message": "Registro excluído com sucesso."}, 200
