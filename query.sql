-- Edit query to suit your needs or change the QUERY_FILENAME in config.py to point to your own query.
SELECT
	*
FROM
	landed_cost.LandedCostLog
WHERE
	storeId = 4215
	AND MONTH(dateCreated) >= 2
	AND MONTH(dateCreated) <= 3