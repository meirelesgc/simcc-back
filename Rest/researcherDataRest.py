from http import HTTPStatus

import pandas as pd
from flask import Blueprint, jsonify, request, send_file

import Dao.generalSQL as generalSQL
import Dao.researcherSQL as researcherSQL
from Dao import sgbdSQL
from Model.City import City
from rest_imageResearcher import download_image

researcherDataRest = Blueprint("researcherDataRest", __name__)


@researcherDataRest.route("/foment", methods=["GET"])
def researcher_query_grant():
    institution_id = request.args.get("institution_id")
    researchers_list = researcherSQL.researcher_query_grant(institution_id)
    return jsonify(researchers_list), HTTPStatus.OK


@researcherDataRest.route("/ResearcherData/Image", methods=["GET"])
def image():
    researcher_id = request.args.get("researcher_id")
    if not researcher_id:
        name = request.args.get("name")
        script_sql = f"""
            SELECT
                id
            FROM
                researcher
            WHERE
                name = '{name}'
            LIMIT 1;"""
        researcher_id = sgbdSQL.consultar_db(script_sql)
        if researcher_id:
            researcher_id = researcher_id[0][0]
        else:
            return jsonify("Pesquisador não encontrado"), HTTPStatus.NOT_FOUND
    try:
        path_image = f"Files/image_researcher/{researcher_id}.jpg"
        return send_file(path_or_file=path_image)
    except Exception:
        print(f"download da foto - {researcher_id}")
        download_image(researcher_id)
        path_image = f"Files/image_researcher/{researcher_id}.jpg"
        return send_file(path_or_file=path_image)


@researcherDataRest.route("/ResearcherData/ByCity", methods=["GET"])
def byCity():
    city_name = request.args.get("city")
    city_id = researcherSQL.city_search(city_name)
    researchers = researcherSQL.researcher_search_city(city_id)

    researcher_list = list()
    if city_id is None:
        for Index, researcher in researchers.iterrows():
            area = str(";").join(
                [
                    great_area.strip().replace(
                        "_",
                        " ",
                    )
                    for great_area in researcher["area"].split(";")
                ]
            )
            dict_researcher = {
                "id": researcher["id"],
                "researcher_name": researcher["researcher_name"],
                "institution": researcher["institution"],
                "image": researcher["image"],
                "city": researcher["city"],
                "area": area,
            }
            researcher_list.append(dict_researcher)
        return jsonify(researcher_list), HTTPStatus.OK
    return jsonify(researchers), HTTPStatus.OK


# Trocar essa chamada de lugar assim que possivel


@researcherDataRest.route("/ResearcherData/TaxonomyCSV", methods=["GET"])
def getCSV():
    csv_taxonomy = pd.read_csv("/home/ejorge/simcc/back_end_simcc/article_tax.csv")

    JsonTax = list()
    for Index, Taxonomy in csv_taxonomy.iterrows():
        Tax = {
            "index": Taxonomy["index"],
            "researcher_id": Taxonomy["researcher_id"],
            "title": Taxonomy["title"],
            "institution_id": Taxonomy["institution_id"],
            "city_id": Taxonomy["city_id"],
            "year": Taxonomy["year"],
            "qualis": Taxonomy["qualis"],
            "jcr": Taxonomy["jcr"],
            "magazine_name": Taxonomy["magazine_name"],
            "tax_id": Taxonomy["tax_id"],
        }
        JsonTax.append(Tax)
    return jsonify(JsonTax)


@researcherDataRest.route("/ResearcherData/City", methods=["GET"])
def city():
    city_data_frame = generalSQL.queryCity()

    JsonCity = list()
    for Index, city_data in city_data_frame.iterrows():
        city = City(
            id=city_data["id"],
            name=city_data["name"],
            country_id=city_data["country_id"],
            state_id=city_data["state_id"],
        )

        JsonCity.append(city.getJson())
    return jsonify(JsonCity)


@researcherDataRest.route("/ResearcherData/DadosGerais", methods=["GET"])
def DadosGerais():
    year = request.args.get("year")
    graduate_program_id = request.args.get("graduate_program_id")
    dep_id = request.args.get("dep_id")
    lista = researcherSQL.generic_data(year, graduate_program_id, dep_id)
    return jsonify(lista)


@researcherDataRest.route("/researcher/DadosGerais", methods=["GET"])
def generic_researcher_data():
    year = request.args.get("year")
    graduate_program_id = request.args.get("researcher_id")
    lista = researcherSQL.generic_researcher_data_data(
        year,
        graduate_program_id,
    )
    return jsonify(lista)
