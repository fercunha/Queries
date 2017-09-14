SELECT DISTINCT
         CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp,'%a %b %d %H:%i:%s %Y'),'%H') AS INTEGER) AS horario,
         COUNT(distinct grelocalipaddress) AS "13/09/2017"
FROM radius.radiusaccounting_parquet
WHERE dt = DATE('2017-09-13')
AND nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
group by 1
order by 1;