SELECT DISTINCT
         -- CAST(DATE_FORMAT(DATE_PARSE(eventtimestamp,'%a %b %d %H:%i:%s %Y'),'%d') AS INTEGER) AS dia,
         CAST(date_format(date_parse(REPLACE(eventtimestamp, '  ', ' 0'), '%a %b %d %T %Y'), '%d') AS INTEGER) AS dia,
         COUNT(acctterminatecause) AS "01/09/2017"
FROM radius.radiusaccounting_parquet
WHERE dt = DATE('2017-09-01')
AND acctterminatecause = 'Lost-Service'    
group by 1
order by 1;

    