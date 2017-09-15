SELECT
    A.horario,
    IF(A.ontem_lostservice IS NULL, 0, A.ontem_lostservice) AS ontem_lostservice,
    IF(B.hoje_lostservice IS NULL, 0, B.hoje_lostservice) AS hoje_lostservice,
    IF(C.ontem_sessiontimeout IS NULL, 0, C.ontem_sessiontimeout) AS ontem_sessiontimeout,
    IF(D.hoje_sessiontimeout IS NULL, 0, D.hoje_sessiontimeout) AS hoje_sessiontimeout,
    IF(E.ontem_idletimeout IS NULL, 0, E.ontem_idletimeout) AS ontem_idletimeout,
    IF(F.hoje_idletimeout IS NULL, 0, F.hoje_idletimeout) AS hoje_idletimeout,
    IF(G.ontem_nasrequest IS NULL, 0, G.ontem_nasrequest) AS ontem_nasrequest,
    IF(H.hoje_nasrequest IS NULL, 0, H.hoje_nasrequest) AS hoje_nasrequest,
    IF(I.ontem_userrequest IS NULL, 0, I.ontem_userrequest) AS ontem_userrequest,
    IF(J.hoje_userrequest IS NULL, 0, J.hoje_userrequest) AS hoje_userrequest,
    IF(K.ontem_lostcarrier IS NULL, 0, K.ontem_lostcarrier) AS ontem_lostcarrier,
    IF(L.hoje_lostcarrier IS NULL, 0, L.hoje_lostcarrier) AS hoje_lostcarrier,
    IF(M.ontem_hostrequest IS NULL, 0, M.ontem_hostrequest) AS ontem_hostrequest,
    IF(N.hoje_hostrequest IS NULL, 0, N.hoje_hostrequest) AS hoje_hostrequest
FROM (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(acctterminatecause) AS ontem_lostservice
    FROM radius.radiusaccounting_parquet r 
    WHERE r.dt = DATE('2017-09-13')
    AND acctterminatecause = 'Lost-Service'
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1
  
) A LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(acctterminatecause) AS hoje_lostservice
    FROM radius.radiusaccounting_parquet r 
    WHERE r.dt = DATE('2017-09-14')
    AND acctterminatecause = 'Lost-Service'
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1
  
) B ON A.horario = B.horario LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(acctterminatecause) AS ontem_sessiontimeout
    FROM radius.radiusaccounting_parquet r 
    WHERE r.dt = DATE('2017-09-13')
    AND acctterminatecause = 'Session-Timeout'
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

) C ON A.horario = C.horario LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(acctterminatecause) AS hoje_sessiontimeout
    FROM radius.radiusaccounting_parquet r 
    WHERE r.dt = DATE('2017-09-14')
    AND acctterminatecause = 'Session-Timeout'
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

)  D ON A.horario = D.horario LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(acctterminatecause) AS ontem_idletimeout
    FROM radius.radiusaccounting_parquet r 
    WHERE r.dt = DATE('2017-09-13')
    AND acctterminatecause = 'Idle-Timeout'
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

) E ON A.horario = E.horario LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(acctterminatecause) AS hoje_idletimeout
    FROM radius.radiusaccounting_parquet r 
    WHERE r.dt = DATE('2017-09-14')
    AND acctterminatecause = 'Idle-Timeout'
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

) F ON A.horario = F.horario LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(acctterminatecause) AS ontem_nasrequest
    FROM radius.radiusaccounting_parquet r 
    WHERE r.dt = DATE('2017-09-13')
    AND acctterminatecause = 'NAS-Request'
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

) G ON A.horario = G.horario LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(acctterminatecause) AS hoje_nasrequest
    FROM radius.radiusaccounting_parquet r 
    WHERE r.dt = DATE('2017-09-14')
    AND acctterminatecause = 'NAS-Request'
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1


) H ON A.horario = H.horario LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(acctterminatecause) AS ontem_userrequest
    FROM radius.radiusaccounting_parquet r 
    WHERE r.dt = DATE('2017-09-13')
    AND acctterminatecause = 'User-Request'
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

) I ON A.horario = I.horario LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(acctterminatecause) AS hoje_userrequest
    FROM radius.radiusaccounting_parquet r 
    WHERE r.dt = DATE('2017-09-14')
    AND acctterminatecause = 'User-Request'
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

) J ON A.horario = J.horario LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(acctterminatecause) AS ontem_lostcarrier
    FROM radius.radiusaccounting_parquet r 
    WHERE r.dt = DATE('2017-09-13')
    AND acctterminatecause = 'Lost-Carrier'
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

) K ON A.horario = K.horario LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(acctterminatecause) AS hoje_lostcarrier
    FROM radius.radiusaccounting_parquet r 
    WHERE r.dt = DATE('2017-09-14')
    AND acctterminatecause = 'Lost-Carrier'
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

) L ON A.horario = L.horario LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(acctterminatecause) AS ontem_hostrequest
    FROM radius.radiusaccounting_parquet r 
    WHERE r.dt = DATE('2017-09-13')
    AND acctterminatecause = 'Host-Request'
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

) M ON A.horario = M.horario LEFT JOIN (

    SELECT 
        CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp, '%a %b %d %H:%i:%s %Y'), '%H') AS INTEGER) AS horario,
        COUNT(acctterminatecause) AS hoje_hostrequest
    FROM radius.radiusaccounting_parquet r 
    WHERE r.dt = DATE('2017-09-14')
    AND acctterminatecause = 'Host-Request'
    AND r.nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
    GROUP BY 1

) N ON A.horario = N.horario ORDER BY A.horario