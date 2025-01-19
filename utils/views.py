from dto.readTable import transform_table_into_view

def create_segments_view(spark):
    spark.sql(
    """
        CREATE OR REPLACE TEMPORARY VIEW segments AS
        SELECT 
            aOrig.Cidade AS cidade_origem,
            aOrig.Pais AS pais_origem,
            aDest.Cidade AS cidade_destino,
            aDest.Pais AS pais_destino,
            SUM(fVoo.qtdVoos) AS qtdVoosTotal
        FROM fVoo
        INNER JOIN
            dAeroporto AS aDest ON fVoo.idAeroDest = aDest.id
        INNER JOIN
            dAeroporto AS aOrig ON fVoo.idAeroOrig = aOrig.id
        GROUP BY 
            aOrig.Cidade, aOrig.Pais, aDest.Cidade, aDest.Pais
        ORDER BY
            qtdVoosTotal DESC
    """)

def create_ordered_segments_view(spark):
    transform_table_into_view(spark, r'"fVoo"', "fVoo")

    transform_table_into_view(spark, r'"dAeroporto"', "dAeroporto")

    create_segments_view(spark)

    view_name = "ordered_segments"

    spark.sql(
    f"""
        CREATE OR REPLACE TEMPORARY VIEW {view_name} AS 
        SELECT 
            *, 
            ROUND(s.qtdVoosTotal/CAST((SELECT SUM(v.qtdVoos) FROM fVoo AS v) AS DECIMAL) * 100, 4) AS perc
        FROM segments AS s
    """
    )

    return view_name

def create_delayed_per_airport(spark):

    transform_table_into_view(spark, r'"fVoo"', "fVoo")

    transform_table_into_view(spark, r'"dAeroporto"', "dAeroporto")

    view_name = "delayed_per_airport"

    spark.sql(
    f"""
        CREATE OR REPLACE TEMPORARY VIEW {view_name} AS
        SELECT 
            a.nome AS aeroporto,
            ROUND(SUM(v.qtdAtrasados) * 100.0 / SUM(v.qtdVoos), 2) AS percAtraso
        FROM
            fVoo AS v
        INNER JOIN
            dAeroporto AS a
        ON
            v.idAeroOrig = a.id
        GROUP BY
            a.nome
        ;
    """
    )

    return view_name