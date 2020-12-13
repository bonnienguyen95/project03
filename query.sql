SELECT t1.name, t1.high, t1.hour, t2.ts AS "datetime"

FROM

(SELECT name, MAX(high) AS "high", SUBSTR("ts", 12, 2) AS "hour"

    FROM "stock-db"."00"
    GROUP BY name, SUBSTR("ts", 12, 2)
    ORDER BY name, SUBSTR("ts", 12, 2)) t1

INNER JOIN 

(Select name, high, ts, SUBSTR("ts", 12, 2) AS "hour"
FROM "stock-db"."00") t2

ON t1.name = t2.name AND t1.high = t2.high AND t1.hour = t2.hour;