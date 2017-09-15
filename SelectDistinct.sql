SELECT DISTINCT(acctterminatecause) AS acctterminatecause
FROM radius.radiusaccounting_parquet
WHERE dt = DATE('2017-09-11')
AND nasname = 'SPO_UOL_NETCOMMUNITY_COM_BR'
ORDER BY acctterminatecause DESC