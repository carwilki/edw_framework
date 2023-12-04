# Databricks notebook source
# Code converted on 2023-10-24 09:48:28
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, date
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")


if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
enterprise = getEnvPrefix(env) + 'enterprise'


# COMMAND ----------

# DECISION STEP
if (date.today().weekday() != 0):
    dbutils.notebook.exit('Decision condition not satisfied, exiting the notebook process')

# COMMAND ----------

# Processing node ASQ_Shortcut_To_WEEKS, type SOURCE 
# COLUMN COUNT: 2

ASQ_Shortcut_To_WEEKS = spark.sql(f"""SELECT DATE_TYPE_ID,

       WEEK_DT

  FROM (SELECT DISTINCT

               CASE WHEN DAY_OF_WK_NBR = 7

                    THEN ( SELECT (CURRENT_DATE - (DATE_PART ('dow', CURRENT_DATE) - 1)) - 7 )

                    WHEN DAY_OF_WK_NBR <> 7

                    THEN ( SELECT (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE )-1)) )

               END AS WEEK_DT,

               3 AS DATE_TYPE_ID

          FROM {enterprise}.DAYS

         WHERE DAY_DT = CURRENT_DATE

        UNION ALL

        SELECT WK.WEEK_DT,

               4 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS WK,

               ( SELECT DAY_DT,

                        FISCAL_WK,

                        FISCAL_MO,

                        CASE WHEN LENGTH(LAST_PERIOD) = 6

                             THEN LAST_PERIOD

                             ELSE SUBSTR(LAST_PERIOD, 1, 4) || '0' || SUBSTR(LAST_PERIOD, 5, 1)

                        END AS LAST_PERIOD,

                        FISCAL_QTR,

                        CASE WHEN LENGTH(LAST_FISCAL_QTR) = 6

                             THEN LAST_FISCAL_QTR

                             ELSE SUBSTR(LAST_FISCAL_QTR, 1, 4) || '0' || SUBSTR(LAST_FISCAL_QTR, 5, 1)

                        END AS LAST_FISCAL_QTR,

                        FISCAL_YR

                  FROM ( SELECT CAST(FISCAL_YR || FISCAL_DAY_OF_YR_NBR AS BIGINT) FISCAL_DAY,

                                DAY_DT,

                                FISCAL_MO,

                                FISCAL_QTR,

                                FISCAL_YR,

                                CASE WHEN SUBSTR(FISCAL_QTR, 5, 2) = '01'

                                     THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '4' AS VARCHAR (4000))

                                     ELSE CAST(FISCAL_YR || CAST(SUBSTR(FISCAL_QTR, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000))

                                END AS LAST_FISCAL_QTR,

                                CASE WHEN SUBSTR(FISCAL_MO, 5, 2) = '01'

                                     THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '12' AS VARCHAR (4000))

                                     ELSE CAST(FISCAL_YR || CAST(SUBSTR(FISCAL_MO, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000))

                                END AS LAST_PERIOD,       FISCAL_WK

                          FROM {enterprise}.DAYS

                         WHERE DAY_DT = ( SELECT CASE WHEN DAY_OF_WK_NBR = 7

                                                      THEN ( SELECT (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE )-1)) - 7 )

                                                      ELSE ( SELECT (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE )-1)) )

                                                 END AS DAY_DT

                                            FROM {enterprise}.DAYS

                                           WHERE DAY_DT = CURRENT_DATE )



                         ) ALIAS1

                ) DATES

         WHERE WK.FISCAL_MO = DATES.FISCAL_MO

           AND WK.WEEK_DT <= DATES.DAY_DT

        UNION ALL

        SELECT WK.WEEK_DT,

               5 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS WK,

               ( SELECT DAY_DT,

                        FISCAL_WK,

                        FISCAL_MO,

                        CASE WHEN LENGTH(LAST_PERIOD) = 6

                             THEN LAST_PERIOD

                             ELSE SUBSTR(LAST_PERIOD, 1, 4) || '0' || SUBSTR(LAST_PERIOD, 5, 1)

                        END AS LAST_PERIOD,

                        FISCAL_QTR,

                        CASE WHEN LENGTH(LAST_FISCAL_QTR) = 6

                             THEN LAST_FISCAL_QTR

                             ELSE SUBSTR(LAST_FISCAL_QTR, 1, 4) || '0' || SUBSTR(LAST_FISCAL_QTR, 5, 1)

                        END AS LAST_FISCAL_QTR,      FISCAL_YR

                   FROM ( SELECT CAST(FISCAL_YR || FISCAL_DAY_OF_YR_NBR AS BIGINT) FISCAL_DAY,

                                 DAY_DT,

                                 FISCAL_MO,

                                 FISCAL_QTR,

                                 FISCAL_YR,

                                 CASE WHEN SUBSTR(FISCAL_QTR, 5, 2) = '01'

                                      THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '4' AS VARCHAR (4000))

                                      ELSE CAST(FISCAL_YR || CAST(SUBSTR(FISCAL_QTR, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000))

                                 END AS LAST_FISCAL_QTR,

                                 CASE WHEN SUBSTR(FISCAL_MO, 5, 2) = '01'

                                      THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '12' AS VARCHAR (4000))

                                      ELSE CAST(CAST( FISCAL_YR || SUBSTR(FISCAL_MO, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000))

                                 END AS LAST_PERIOD,

                                 FISCAL_WK

                            FROM {enterprise}.DAYS

                           WHERE DAY_DT = ( SELECT CASE WHEN DAY_OF_WK_NBR = 7

                                                        THEN ( SELECT (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE )-1))- 7 )

                                                        ELSE ( SELECT (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE )-1)) )

                                                   END AS DAY_DT

                                              FROM {enterprise}.DAYS

                                             WHERE DAY_DT = CURRENT_DATE

                                           )

                        ) ALIAS1

                ) DATES

         WHERE WK.FISCAL_MO = DATES.LAST_PERIOD

           AND WK.WEEK_DT <= DATES.DAY_DT

        UNION ALL

        SELECT WK.WEEK_DT,

               6 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS WK,

               ( SELECT DAY_DT,

                        FISCAL_WK,

                        FISCAL_MO,

                        CASE WHEN LENGTH(LAST_PERIOD) = 6

                             THEN LAST_PERIOD

                             ELSE SUBSTR(LAST_PERIOD, 1, 4) || '0' || SUBSTR(LAST_PERIOD, 5, 1)

                        END AS LAST_PERIOD,

                        FISCAL_QTR,

                        CASE WHEN LENGTH(LAST_FISCAL_QTR) = 6

                             THEN LAST_FISCAL_QTR

                             ELSE SUBSTR(LAST_FISCAL_QTR, 1, 4) || '0' || SUBSTR(LAST_FISCAL_QTR, 5, 1)

                        END AS LAST_FISCAL_QTR,      FISCAL_YR

                   FROM ( SELECT CAST(FISCAL_YR || FISCAL_DAY_OF_YR_NBR AS BIGINT) FISCAL_DAY,

                                 DAY_DT,

                                 FISCAL_MO,

                                 FISCAL_QTR,

                                 FISCAL_YR,

                                 CASE WHEN SUBSTR(FISCAL_QTR, 5, 2) = '01'

                                      THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '4' AS VARCHAR (4000))

                                      ELSE CAST(FISCAL_YR || CAST(SUBSTR(FISCAL_QTR, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000))

                                 END AS LAST_FISCAL_QTR,

                                 CASE WHEN SUBSTR(FISCAL_MO, 5, 2) = '01'

                                      THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '12' AS VARCHAR (4000))

                                      ELSE CAST(FISCAL_YR || CAST(SUBSTR(FISCAL_MO, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000))

                                 END AS LAST_PERIOD,

                                 FISCAL_WK

                            FROM {enterprise}.DAYS

                           WHERE DAY_DT = ( SELECT CASE WHEN DAY_OF_WK_NBR = 7

                                                        THEN ( SELECT (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE )-1)) - 7 )

                                                        ELSE ( SELECT (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE )-1)) )

                                                   END AS DAY_DT

                                              FROM {enterprise}.DAYS

                                             WHERE DAY_DT = CURRENT_DATE

                                            )

                        ) ALIAS1

                ) DATES

         WHERE WK.FISCAL_QTR = DATES.FISCAL_QTR

           AND WK.WEEK_DT <= DATES.DAY_DT

        UNION ALL

        SELECT WK.WEEK_DT,

               7 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS WK,

               ( SELECT DAY_DT,

                        FISCAL_WK,

                        FISCAL_MO,

                        CASE WHEN LENGTH(LAST_PERIOD)  = 6

                             THEN LAST_PERIOD

                             ELSE SUBSTR(LAST_PERIOD, 1, 4) || '0' || SUBSTR(LAST_PERIOD, 5, 1)

                         END AS LAST_PERIOD,

                        FISCAL_QTR,

                        CASE WHEN LENGTH(LAST_FISCAL_QTR)  = 6

                             THEN LAST_FISCAL_QTR

                             ELSE SUBSTR(LAST_FISCAL_QTR, 1, 4) || '0' || SUBSTR(LAST_FISCAL_QTR, 5, 1)

                         END AS LAST_FISCAL_QTR,

                         FISCAL_YR

                   FROM ( SELECT CAST(FISCAL_YR || FISCAL_DAY_OF_YR_NBR  AS BIGINT) FISCAL_DAY,

                                 DAY_DT,

                                 FISCAL_MO,

                                 FISCAL_QTR,

                                 FISCAL_YR,

                                 CASE WHEN SUBSTR(FISCAL_QTR, 5, 2)  = '01'

                                      THEN CAST(CAST(FISCAL_YR  AS BIGINT) - 1 || '4' AS VARCHAR (4000))

                                      ELSE CAST(FISCAL_YR || CAST(SUBSTR(FISCAL_QTR, 5, 2)  AS BIGINT) - 1 AS VARCHAR (4000))

                                  END AS LAST_FISCAL_QTR,

                                 CASE WHEN SUBSTR(FISCAL_MO, 5, 2)  = '01'

                                      THEN CAST(CAST(FISCAL_YR  AS BIGINT) - 1 || '12' AS VARCHAR (4000))

                                      ELSE CAST(FISCAL_YR || CAST(SUBSTR(FISCAL_MO, 5, 2)  AS BIGINT) - 1 AS VARCHAR (4000))

                                  END AS LAST_PERIOD,

                                 FISCAL_WK

                            FROM {enterprise}.DAYS

                           WHERE DAY_DT = ( SELECT CASE WHEN DAY_OF_WK_NBR  = 7

                                                        THEN ( SELECT (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE )-1))- 7 )

                                                        ELSE ( SELECT (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE )-1)) )

                                                    END AS DAY_DT

                                               FROM {enterprise}.DAYS

                                              WHERE DAY_DT = CURRENT_DATE

                                           )

                        ) ALIAS1

                ) DATES

         WHERE WK.FISCAL_QTR = LAST_FISCAL_QTR

           AND WK.WEEK_DT <= DATES.DAY_DT

        UNION ALL

        SELECT WK.WEEK_DT,

               8 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS WK,

               ( SELECT DAY_DT,

                        FISCAL_WK,

                        FISCAL_MO,

                        CASE WHEN LENGTH(LAST_PERIOD) = 6

                             THEN LAST_PERIOD

                             ELSE SUBSTR(LAST_PERIOD, 1, 4) || '0' || SUBSTR(LAST_PERIOD, 5, 1)

                         END AS LAST_PERIOD,

                        FISCAL_QTR,

                        CASE WHEN LENGTH(LAST_FISCAL_QTR) = 6

                             THEN LAST_FISCAL_QTR

                             ELSE SUBSTR(LAST_FISCAL_QTR, 1, 4) || '0' || SUBSTR(LAST_FISCAL_QTR, 5, 1)

                         END AS LAST_FISCAL_QTR,      FISCAL_YR

                   FROM ( SELECT CAST(FISCAL_YR || FISCAL_DAY_OF_YR_NBR AS BIGINT) FISCAL_DAY,

                                 DAY_DT,

                                 FISCAL_MO,

                                 FISCAL_QTR,

                                 FISCAL_YR,

                                 CASE WHEN SUBSTR(FISCAL_QTR, 5, 2) = '01'

                                      THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '4' AS VARCHAR (4000))

                                      ELSE CAST(FISCAL_YR || CAST(SUBSTR(FISCAL_QTR, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000))

                                  END AS LAST_FISCAL_QTR,

                                 CASE WHEN SUBSTR(FISCAL_MO, 5, 2) = '01'

                                      THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '12' AS VARCHAR (4000))

                                      ELSE CAST(FISCAL_YR || CAST(SUBSTR(FISCAL_MO, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000))

                                  END AS LAST_PERIOD,

                                 FISCAL_WK

                            FROM {enterprise}.DAYS

                           WHERE DAY_DT = ( SELECT CASE WHEN DAY_OF_WK_NBR = 7

                                                        THEN ( SELECT (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE )-1)) - 7 )

                                                        ELSE ( SELECT (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE )-1)) )

                                                    END AS DAY_DT

                                              FROM {enterprise}.DAYS

                                             WHERE DAY_DT = CURRENT_DATE

                                           )

                        ) ALIAS1

                ) DATES

         WHERE WK.FISCAL_YR = DATES.FISCAL_YR

           AND WK.WEEK_DT <= DATES.DAY_DT

        UNION ALL

        SELECT DISTINCT

               WEEK_DT,

               9 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS

         WHERE WEEK_DT >= CURRENT_DATE - 28

           AND WEEK_DT < CURRENT_DATE

        UNION ALL

        SELECT DISTINCT

               WEEK_DT,

               10 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS

          WHERE WEEK_DT >= CURRENT_DATE - 56

            AND WEEK_DT < CURRENT_DATE

        UNION ALL

        SELECT DISTINCT

               WEEK_DT,

               11 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS

         WHERE WEEK_DT >= CURRENT_DATE - 91

           AND WEEK_DT < CURRENT_DATE

        UNION ALL

        SELECT WEEK_DT,

               12 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS

         WHERE FISCAL_YR =    (

        SELECT DISTINCT FISCAL_YR - 1

          FROM {enterprise}.DAYS

         WHERE DAY_DT = (CURRENT_DATE -1 - (DATE_PART('dow', CURRENT_DATE -1)-1)) )

           AND FISCAL_WK_NBR <= ( SELECT FISCAL_WK_NBR

                                    FROM {enterprise}.WEEKS

                                   WHERE WEEK_DT = (CURRENT_DATE -1 - (DATE_PART('dow', CURRENT_DATE -1)-1))

                                )

        UNION ALL

        SELECT WEEK_DT,

               13 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS

         WHERE FISCAL_YR = ( SELECT DISTINCT

                                    FISCAL_YR - 1

                               FROM {enterprise}.DAYS

                              WHERE DAY_DT = (CURRENT_DATE -1 - (DATE_PART('dow', CURRENT_DATE -1)-1))

                           )

        UNION ALL

        SELECT DISTINCT

               WEEK_DT,

               14 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS

         WHERE WEEK_DT >= CURRENT_DATE - 112

           AND WEEK_DT < CURRENT_DATE

        UNION ALL

        SELECT WK.WEEK_DT,

               15 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS WK,

               ( SELECT WEEK_DT,

                        FISCAL_WK,

                        FISCAL_QTR_NBR,

                        FISCAL_WK_NBR,

                        (FISCAL_YR - 1) LYR_FISCAL_YR

                   FROM {enterprise}.WEEKS W

                  WHERE WEEK_DT = (CURRENT_DATE -1 - (DATE_PART('dow', CURRENT_DATE -1)-1))

               ) CW

         WHERE WK.FISCAL_YR = CW.LYR_FISCAL_YR

           AND WK.FISCAL_QTR_NBR = CW.FISCAL_QTR_NBR

           AND WK.FISCAL_WK_NBR <= CW.FISCAL_WK_NBR

        UNION ALL

        SELECT date_add(WEEK_DT, -14) AS WEEK_DT,

               16 AS DATE_TYPE_ID

          FROM {enterprise}.DAYS

         WHERE DAY_DT = CURRENT_DATE

        UNION ALL

        SELECT DISTINCT

               WEEK_DT,

               17 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS

         WHERE WEEK_DT >= CURRENT_DATE - 182

           AND WEEK_DT < CURRENT_DATE

        UNION ALL

        SELECT DISTINCT

               WEEK_DT,

               18 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS

         WHERE WEEK_DT >= CURRENT_DATE - 364

           AND WEEK_DT < CURRENT_DATE

        UNION ALL

        SELECT LY.WEEK_DT,

               19 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS CR,

               {enterprise}.WEEKS LY

         WHERE CR.WEEK_DT = (CURRENT_DATE -1 - (DATE_PART('dow', CURRENT_DATE -1)-1))

           AND LY.FISCAL_YR = CR.FISCAL_YR - 1

           AND LY.FISCAL_MO_NBR = CR.FISCAL_MO_NBR

           AND LY.FISCAL_WK_NBR <= CR.FISCAL_WK_NBR

      UNION ALL

      SELECT LY.WEEK_DT, 

             20 AS DATE_TYPE_ID

        FROM (SELECT WEEK_DT, 

                     FISCAL_MO_NBR, 

                     FISCAL_YR

                FROM {enterprise}.WEEKS

               WHERE WEEK_DT =(CURRENT_DATE - 1 - (DATE_PART ('dow', CURRENT_DATE - 1) - 1)))CR,

             {enterprise}.WEEKS LY

       WHERE LY.FISCAL_YR     = CASE WHEN CR.FISCAL_MO_NBR = 1

                                          THEN CR.FISCAL_YR - 2

                                     ELSE CR.FISCAL_YR - 1

                                END

         AND LY.FISCAL_MO_NBR = CASE WHEN CR.FISCAL_MO_NBR = 1

                                          THEN 12

                                     ELSE CR.FISCAL_MO_NBR - 1

                                END

       UNION ALL

       SELECT DISTINCT

              WEEK_DT,

              21 AS DATE_TYPE_ID

         FROM {enterprise}.WEEKS

        WHERE WEEK_DT >= CURRENT_DATE - 14

          AND WEEK_DT < CURRENT_DATE

UNION ALL

	       SELECT WEEK_DT,

               22 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS

         WHERE substr(FISCAL_QTR,5,2) in ('03','04')

		 and substr(FISCAL_QTR,1,4) = DATE_FORMAT(CURRENT_DATE,'yyyy')

   and week_Dt < CURRENT_DATE

/* Season To Date */
UNION ALL

		SELECT w1.WEEK_DT,

	  	23 AS DATE_TYPE_ID

		FROM {enterprise}.WEEKS w1

		WHERE WEEK_DT < CURRENT_DATE

		AND FISCAL_HALF=(SELECT FISCAL_HALF FROM {enterprise}.WEEKS w2

                 		WHERE w2.WEEK_DT=(SELECT MAX(w3.WEEK_DT) FROM {enterprise}.WEEKS w3 WHERE w3.WEEK_DT < CURRENT_DATE))

/* LYLW		 */
				

UNION ALL

SELECT LY.WEEK_DT

       ,72 AS DATE_TYPE_ID

     FROM {enterprise}.WEEKS LY

JOIN (

       SELECT FISCAL_YR

              ,MAX(FISCAL_WK_NBR) AS TOTAL_FISCAL_WK_AMT

       FROM {enterprise}.WEEKS

       GROUP BY FISCAL_YR

       ) WKN ON LY.FISCAL_YR = WKN.FISCAL_YR 

JOIN {enterprise}.WEEKS CY ON LY.FISCAL_WK_NBR = (1-CY.FISCAL_WK_NBR/53)*(WKN.TOTAL_FISCAL_WK_AMT - 52) + CY.FISCAL_WK_NBR -floor(CY.FISCAL_WK_NBR/53)*52

 AND LY.FISCAL_YR = floor(CY.FISCAL_YR -1 + (CY.FISCAL_WK_NBR/53))

  WHERE CY.WEEK_DT =  (CURRENT_DATE - 1 - (DATE_PART('dow', CURRENT_DATE - 1) - 1))



/* LYSTD */


UNION ALL

	  

SELECT WK.WEEK_DT

       ,73 AS DATE_TYPE_ID

FROM {enterprise}.WEEKS WK

       ,(

              SELECT WEEK_DT

                     ,FISCAL_WK

,FISCAL_HALF /* Removing Fiscal Qtr and adding Fiscal Half */
                     ,FISCAL_WK_NBR

,FISCAL_YR /* Also adding Fiscal Yr */
                     ,(FISCAL_YR - 1) LYR_FISCAL_YR

              FROM {enterprise}.WEEKS W

              WHERE WEEK_DT = (CURRENT_DATE - 1 - (DATE_PART('dow', CURRENT_DATE - 1) - 1))

              ) CW

WHERE WK.FISCAL_YR = CW.LYR_FISCAL_YR

       AND WK.FISCAL_HALF - WK.FISCAL_YR * 100 = CW.FISCAL_HALF - CW.FISCAL_YR * 100

             AND WK.FISCAL_WK_NBR <= CW.FISCAL_WK_NBR

			 

			 union all

			 

			    SELECT WK.WEEK_DT,

               74 AS DATE_TYPE_ID

          FROM {enterprise}.WEEKS WK,

               ( SELECT DAY_DT,

                        FISCAL_WK,

                        FISCAL_MO,

                        CASE WHEN LENGTH(LAST_PERIOD) = 6

                             THEN LAST_PERIOD

                             ELSE SUBSTR(LAST_PERIOD, 1, 4) || '0' || SUBSTR(LAST_PERIOD, 5, 1)

                         END AS LAST_PERIOD,

                        FISCAL_QTR,

                        CASE WHEN LENGTH(LAST_FISCAL_QTR) = 6

                             THEN LAST_FISCAL_QTR

                             ELSE SUBSTR(LAST_FISCAL_QTR, 1, 4) || '0' || SUBSTR(LAST_FISCAL_QTR, 5, 1)

                         END AS LAST_FISCAL_QTR,      FISCAL_YR

                   FROM ( SELECT CAST(FISCAL_YR || FISCAL_DAY_OF_YR_NBR AS BIGINT) FISCAL_DAY,

                                 DAY_DT,

                                 FISCAL_MO,

                                 FISCAL_QTR,

                                 FISCAL_YR,

                                 CASE WHEN SUBSTR(FISCAL_QTR, 5, 2) = '01'

                                      THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '4' AS VARCHAR (4000))

                                      ELSE CAST(FISCAL_YR || CAST(SUBSTR(FISCAL_QTR, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000))

                                  END AS LAST_FISCAL_QTR,

                                 CASE WHEN SUBSTR(FISCAL_MO, 5, 2) = '01'

                                      THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '12' AS VARCHAR (4000))

                                      ELSE CAST(FISCAL_YR || CAST(SUBSTR(FISCAL_MO, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000))

                                  END AS LAST_PERIOD,

                                 FISCAL_WK

                            FROM {enterprise}.DAYS

                           WHERE DAY_DT = ( SELECT CASE WHEN DAY_OF_WK_NBR = 7

                                                        THEN ( SELECT (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE )-1)) - 7 )

                                                        ELSE ( SELECT (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE )-1)) )

                                                    END AS DAY_DT

                                              FROM {enterprise}.DAYS

                                             WHERE DAY_DT = CURRENT_DATE

                                           )

                        ) ALIAS1

                ) DATES

         WHERE WK.FISCAL_YR = DATES.FISCAL_YR

           AND WK.WEEK_DT <= date_add(DATES.DAY_DT,-7)

	  

         )  ALIAS1""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQ_Shortcut_To_WEEKS = ASQ_Shortcut_To_WEEKS \
	.withColumnRenamed(ASQ_Shortcut_To_WEEKS.columns[0],'DATE_TYPE_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_WEEKS.columns[1],'WEEK_DT')

# COMMAND ----------

# Processing node EXP_DATE_TYPE_ID, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
ASQ_Shortcut_To_WEEKS_temp = ASQ_Shortcut_To_WEEKS.toDF(*["ASQ_Shortcut_To_WEEKS___" + col for col in ASQ_Shortcut_To_WEEKS.columns])

EXP_DATE_TYPE_ID = ASQ_Shortcut_To_WEEKS_temp.selectExpr(
	"ASQ_Shortcut_To_WEEKS___sys_row_id as sys_row_id",
	"BIGINT(ASQ_Shortcut_To_WEEKS___DATE_TYPE_ID) as DATE_TYPE_ID",
	"ASQ_Shortcut_To_WEEKS___WEEK_DT as WEEK_DT"
)

# COMMAND ----------

# Processing node Shortcut_to_DATE_TYPE_WEEK, type TARGET 
# COLUMN COUNT: 2


Shortcut_to_DATE_TYPE_WEEK = EXP_DATE_TYPE_ID.selectExpr(
	"DATE_TYPE_ID as DATE_TYPE_ID",
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT"
)
try:
	# chk=DuplicateChecker()
	# chk.check_for_duplicate_primary_keys(spark,f'{legacy}.DATE_TYPE_WEEK951',Shortcut_to_DATE_TYPE_WEEK,["KEY1","KEY1"])
	Shortcut_to_DATE_TYPE_WEEK.write.mode("overwrite").saveAsTable(f'{legacy}.DATE_TYPE_WEEK')
except Exception as e:
	raise e

# COMMAND ----------


