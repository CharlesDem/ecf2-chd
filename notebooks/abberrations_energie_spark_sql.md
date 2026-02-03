df_abberants.createOrReplaceTempView("aberrants")

spark.sql("""
                        SELECT
                            batiment_id,
                            commune,
                            conso_energy
                        FROM aberrants
                        ORDER BY conso_energy DESC
                        LIMIT 10
""").show()

+-----------+-------------+-----------------+
|batiment_id|      commune|     conso_energy|
+-----------+-------------+-----------------+
|    BAT0121|     Le Havre|741.5154604016675|
|    BAT0043|     Bordeaux|739.9206099166299|
|    BAT0043|     Bordeaux|738.7014699429573|
|    BAT0112|        Reims|736.2293990306945|
|    BAT0112|        Reims|734.7115347334407|
|    BAT0048|        Lille|732.7303436334579|
|    BAT0048|        Lille| 732.243100692594|
|    BAT0122|     Le Havre|731.0032362254591|
|    BAT0005|        Paris|729.0305724508047|
|    BAT0134|Saint-Etienne|726.7926669802442|
+-----------+-------------+-----------------+