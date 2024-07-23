MEDQ_QUERY = '''
            SELECT

                d."medqID" AS "MedQ_ID",
                d."purchaseDate" AS "budgetYear",
                d."purchaseDate" AS "contractFirstDate",
                o."name" AS "departmentName",
                SUBSTRING(o.obj->'raw'->>'รหัสอำเภอ' FROM 4 FOR 32) AS "districtName",
                d."name" AS "goodsName",
                la."name" AS "methodName",
                d."standardID" AS "unspscId",
                d."name" AS "productName",
                d."askPrice" AS "priceUnit",
                SUBSTRING(o.obj->'raw'->>'รหัสจังหวัด' FROM 4 FOR 32) AS "provinceName",
                lm."nameEN" AS "winnerName",
                SUBSTRING(CAST(d."purchaseDate" AS TEXT) FROM 1 FOR 4) AS "year",
                o."provinceCode" AS "PROVINCE_ID",
                o.obj->'raw'->>'เขตบริการ' AS "HEALTH_AREA",
                d."orgID",
                o."hospitalCode"

            FROM device AS d

            LEFT JOIN org AS o
                ON d."orgID" = o."hospitalCode"

            LEFT JOIN "lookup_acquiredMethod" AS la
                ON d."acquiredMethodID" = la."id"

            LEFT JOIN lookup_manufacturer AS lm
                ON CAST(d."manufacturerID" AS INT) = lm."id"

            WHERE d."name" IS NOT NULL

            ORDER BY d."medqID"
        '''