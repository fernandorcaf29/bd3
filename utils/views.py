def create_segments_view(spark):
    spark.sql(
    """
        CREATE TEMPORARY VIEW trechos_mais_frequentes AS
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