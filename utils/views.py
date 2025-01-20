from dto.readTable import transform_tables_into_view

def create_segments_view(spark):

    transform_tables_into_view(spark, ["dAeroporto", "fVoo"])

    view_name = "segments"

    spark.sql(
    f"""
        CREATE OR REPLACE TEMPORARY VIEW {view_name} AS
        SELECT
            aOrig.Cidade AS cidade_origem,
            aOrig.Pais AS pais_origem,
            aDest.Cidade AS cidade_destino,
            aDest.Pais AS pais_destino,
            SUM(fvoo.atrasoMinTotal) AS atrasoMinTotais,
            SUM(fVoo.qtdCancelados) AS qtdCanceladosTotal,
            SUM(fVoo.qtdAtrasados) AS qtdAtrasadosTotal,
            SUM(fVoo.qtdVoos) AS qtdVoosTotal
        FROM 
            fVoo
        INNER JOIN
            dAeroporto AS aDest ON fVoo.idAeroDest = aDest.id
        INNER JOIN
            dAeroporto AS aOrig ON fVoo.idAeroOrig = aOrig.id
        GROUP BY 
            aOrig.Cidade, aOrig.Pais, aDest.Cidade, aDest.Pais
    """)

    return view_name



def create_ordered_segments_view(spark):

    transform_tables_into_view(spark, ["dAeroporto", "fVoo"])

    create_segments_view(spark)

    view_name = "ordered_segments"

    spark.sql(
    f"""
        CREATE OR REPLACE TEMPORARY VIEW {view_name} AS 
        SELECT 
            *,
            ROUND(s.qtdVoosTotal/CAST((SELECT SUM(v.qtdVoos) FROM fVoo AS v) AS DECIMAL) * 100, 4) AS perc
        FROM segments AS s
        ORDER BY
            perc 
        DESC
    """
    )

    return view_name



def create_delayed_per_airport(spark):

    transform_tables_into_view(spark, ["dAeroporto", "fVoo"])

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

def create_aeroportos_cancelamento_view (spark):

    transform_tables_into_view(spark, ["dAeroporto", "fVoo"])

    view_name = "aeroportos_cancelamento"

    spark.sql (
        f"""
            CREATE OR REPLACE TEMPORARY VIEW {view_name} AS
            SELECT 
                a.nome AS aeroporto,
                ROUND(SUM(v.qtdCancelados) * 100.0 / SUM(v.qtdVoos), 2) AS percCancelamento
            FROM
                fVoo AS v
            INNER JOIN
                dAeroporto AS a ON v.idAeroOrig = a.id
            GROUP BY
                a.nome
            ORDER BY
                percCancelamento DESC
            LIMIT 10;
        """
    )
    return view_name

def create_aeroportos_cancelamento_minimo_view (spark):

    transform_tables_into_view(spark, ["dAeroporto", "fVoo"])

    view_name = "aeroportos_cancelamento_minimo"

    spark.sql (
        f"""
            CREATE OR REPLACE TEMPORARY VIEW {view_name} AS
            SELECT 
                a.nome AS aeroporto,
                ROUND(SUM(v.qtdCancelados) * 100.0 / SUM(v.qtdVoos), 2) AS percCancelamento
            FROM
                fVoo AS v
            INNER JOIN
                dAeroporto AS a ON v.idAeroOrig = a.id
            GROUP BY
                a.nome
            ORDER BY
                percCancelamento ASC
            LIMIT 10;
        """
    )
    return view_name

def create_principais_justificativas_view (spark):

    transform_tables_into_view(spark, ["fVooJustificativa", "dJustificativa"])

    view_name = "principais_justificativas"

    spark.sql (
        f"""
            CREATE OR REPLACE TEMPORARY VIEW {view_name} AS
            SELECT 
                j.cod AS justificativa,
                COUNT(vj.idJustificativa) AS qtdCancelamentos
            FROM 
                fVooJustificativa AS vj
            INNER JOIN 
                dJustificativa AS j ON vj.idJustificativa = j.id
            GROUP BY 
                j.cod
            ORDER BY 
                qtdCancelamentos DESC
            LIMIT 10;
        """
    )
    return view_name

def create_principais_justificativas_maiores_taxas_view (spark):

    transform_tables_into_view(spark, ["dAeroporto", "fVoo", "fVooJustificativa", "dJustificativa"])

    view_name = "principais_justificativas_maiores_taxas"

    spark.sql (
        f"""
            CREATE OR REPLACE TEMPORARY VIEW aeroportos_id_cancelamento AS
            SELECT 
                a.id AS aeroporto_id,
                a.nome AS aeroporto,
                ROUND(SUM(v.qtdCancelados) * 100.0 / SUM(v.qtdVoos), 2) AS percCancelamento
            FROM
                fVoo AS v
            INNER JOIN
                dAeroporto AS a ON v.idAeroOrig = a.id
            GROUP BY
                a.id, a.nome
            ORDER BY
                percCancelamento DESC
            LIMIT 10;
        """
    )

    spark.sql (
        f"""
            CREATE OR REPLACE TEMPORARY VIEW {view_name} AS
            SELECT 
                j.cod AS justificativa,
                COUNT(vj.idJustificativa) AS qtdCancelamentos
            FROM 
                fVooJustificativa AS vj
            INNER JOIN 
                dJustificativa AS j ON vj.idJustificativa = j.id
            INNER JOIN 
                aeroportos_id_cancelamento AS ac ON vj.idAero = ac.aeroporto_id
            GROUP BY 
                j.cod
            ORDER BY 
                qtdCancelamentos DESC
            LIMIT 10;
        """
    )

    return view_name