SELECT
    A.horario,
    IF(A.ontem_sessoes IS NULL, 0, A.ontem_sessoes) AS ontem_sessoes,
    IF(B.hoje_sessoes IS NULL, 0, B.hoje_sessoes) AS hoje_sessoes,
    REPLACE(CAST(IF(A.ontem_trafego IS NULL, 0, A.ontem_trafego) AS VARCHAR), '.', ',') AS ontem_trafego,
    REPLACE(CAST(IF(B.hoje_trafego IS NULL, 0, B.hoje_trafego) AS VARCHAR), '.', ',') AS hoje_trafego,
    IF(C.ontem_sessoes_mac1 IS NULL, 0, C.ontem_sessoes_mac1) AS ontem_sessoes_mac1,
    IF(D.hoje_sessoes_mac1 IS NULL, 0 , D.hoje_sessoes_mac1) AS hoje_sessoes_mac1,
    REPLACE(CAST(IF(C.ontem_trafego_mac1 IS NULL, 0, C.ontem_trafego_mac1) AS VARCHAR), '.', ',') AS ontem_trafego_mac1,
    REPLACE(CAST(IF(D.hoje_trafego_mac1 IS NULL, 0, D.hoje_trafego_mac1) AS VARCHAR), '.', ',') AS hoje_trafego_mac1,
    IF(E.ontem_sessoes_mac0 IS NULL, 0, E.ontem_sessoes_mac0) AS ontem_sessoes_mac0,
    IF(F.hoje_sessoes_mac0 IS NULL, 0, F.hoje_sessoes_mac0) AS hoje_sessoes_mac0,
    REPLACE(CAST(IF(E.ontem_trafego_mac0 IS NULL, 0, E.ontem_trafego_mac0) AS VARCHAR), '.', ',') AS ontem_trafego_mac0,
    REPLACE(CAST(IF(F.hoje_trafego_mac0 IS NULL, 0, F.hoje_trafego_mac0) AS VARCHAR), '.', ',') AS hoje_trafego_mac0
FROM (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(distinct r.acctuniqueid) AS ontem_sessoes,
        ROUND((SUM(CAST(IF(acctinputoctets IS NULL, 0, acctinputoctets) AS DOUBLE) / 1073741824) + SUM(CAST((IF(acctinputgigawords IS NULL, 0, acctinputgigawords) * 4294967296) AS DOUBLE) / 1073741824) +
        SUM(CAST(IF(acctoutputoctets IS NULL, 0, acctoutputoctets) AS DOUBLE) / 1073741824) + SUM(CAST((IF(acctoutputgigawords IS NULL, 0, acctoutputgigawords) * 4294967296) AS DOUBLE) / 1073741824)), 2) AS ontem_trafego
    FROM radius."radiusaccounting_parquet" r 
    WHERE r.dt = DATE('2017-09-13')
    AND (r.authenticatedusername <> '' OR (r.authenticatedusername = '' AND r.macauth = 1)) 
    AND r.macauth IN (0,1) 
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1
  
) A LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(distinct r.acctuniqueid) AS hoje_sessoes,
        ROUND((SUM(CAST(IF(acctinputoctets IS NULL, 0, acctinputoctets) AS DOUBLE) / 1073741824) + SUM(CAST((IF(acctinputgigawords IS NULL, 0, acctinputgigawords) * 4294967296) AS DOUBLE) / 1073741824) +
        SUM(CAST(IF(acctoutputoctets IS NULL, 0, acctoutputoctets) AS DOUBLE) / 1073741824) + SUM(CAST((IF(acctoutputgigawords IS NULL, 0, acctoutputgigawords) * 4294967296) AS DOUBLE) / 1073741824)), 2) AS hoje_trafego
    FROM radius."radiusaccounting" r 
    WHERE r.dt = DATE('2017-09-14')
    AND CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) <= 23
    AND (r.authenticatedusername <> '' OR (r.authenticatedusername = '' AND r.macauth = 1)) 
    AND r.macauth IN (0,1) 
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1
  
) B ON A.horario = B.horario LEFT JOIN (

     SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(distinct r.acctuniqueid) AS ontem_sessoes_mac1,
        ROUND((SUM(CAST(IF(acctinputoctets IS NULL, 0, acctinputoctets) AS DOUBLE) / 1073741824) + SUM(CAST((IF(acctinputgigawords IS NULL, 0, acctinputgigawords) * 4294967296) AS DOUBLE) / 1073741824) +
        SUM(CAST(IF(acctoutputoctets IS NULL, 0, acctoutputoctets) AS DOUBLE) / 1073741824) + SUM(CAST((IF(acctoutputgigawords IS NULL, 0, acctoutputgigawords) * 4294967296) AS DOUBLE) / 1073741824)), 2) AS ontem_trafego_mac1
    FROM radius."radiusaccounting_parquet" r 
    WHERE r.dt = DATE('2017-09-13')
    AND (r.authenticatedusername <> '' OR (r.authenticatedusername = '' AND r.macauth = 1)) 
    AND r.macauth IN (0,1) 
    AND r.macauth = 1
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

) C ON A.horario = C.horario LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(distinct r.acctuniqueid) AS hoje_sessoes_mac1,
        ROUND((SUM(CAST(IF(acctinputoctets IS NULL, 0, acctinputoctets) AS DOUBLE) / 1073741824) + SUM(CAST((IF(acctinputgigawords IS NULL, 0, acctinputgigawords) * 4294967296) AS DOUBLE) / 1073741824) +
        SUM(CAST(IF(acctoutputoctets IS NULL, 0, acctoutputoctets) AS DOUBLE) / 1073741824) + SUM(CAST((IF(acctoutputgigawords IS NULL, 0, acctoutputgigawords) * 4294967296) AS DOUBLE) / 1073741824)), 2) AS hoje_trafego_mac1
    FROM radius."radiusaccounting" r 
    WHERE r.dt = DATE('2017-09-14')
    AND CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) <= 23
    AND (r.authenticatedusername <> '' OR (r.authenticatedusername = '' AND r.macauth = 1)) 
    AND r.macauth IN (0,1) 
    AND r.macauth = 1
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

) D ON A.horario = D.horario LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(distinct r.acctuniqueid) AS ontem_sessoes_mac0,
        ROUND((SUM(CAST(IF(acctinputoctets IS NULL, 0, acctinputoctets) AS DOUBLE) / 1073741824) + SUM(CAST((IF(acctinputgigawords IS NULL, 0, acctinputgigawords) * 4294967296) AS DOUBLE) / 1073741824) +
        SUM(CAST(IF(acctoutputoctets IS NULL, 0, acctoutputoctets) AS DOUBLE) / 1073741824) + SUM(CAST((IF(acctoutputgigawords IS NULL, 0, acctoutputgigawords) * 4294967296) AS DOUBLE) / 1073741824)), 2) AS ontem_trafego_mac0
    FROM radius."radiusaccounting_parquet" r 
    WHERE r.dt = DATE('2017-09-13')
    AND (r.authenticatedusername <> '' OR (r.authenticatedusername = '' AND r.macauth = 1)) 
    AND r.macauth IN (0,1) 
    AND r.macauth = 0
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

) E ON A.horario = E.horario LEFT JOIN (

     SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(distinct r.acctuniqueid) AS hoje_sessoes_mac0,
        ROUND((SUM(CAST(IF(acctinputoctets IS NULL, 0, acctinputoctets) AS DOUBLE) / 1073741824) + SUM(CAST((IF(acctinputgigawords IS NULL, 0, acctinputgigawords) * 4294967296) AS DOUBLE) / 1073741824) +
        SUM(CAST(IF(acctoutputoctets IS NULL, 0, acctoutputoctets) AS DOUBLE) / 1073741824) + SUM(CAST((IF(acctoutputgigawords IS NULL, 0, acctoutputgigawords) * 4294967296) AS DOUBLE) / 1073741824)), 2) AS hoje_trafego_mac0
    FROM radius."radiusaccounting" r 
    WHERE r.dt = DATE('2017-09-14')
    AND CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) <= 12
    AND (r.authenticatedusername <> '' OR (r.authenticatedusername = '' AND r.macauth = 1)) 
    AND r.macauth IN (0,1) 
    AND r.macauth = 0
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

) F ON A.horario = F.horario ORDER BY A.horario