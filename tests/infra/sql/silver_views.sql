-- The silver views of one entity; ${entity} and ${key} come from SPARK_SQL_VARS.

SELECT 'history', ${key}, _commit_sequence, _change_op, _is_deleted, _valid_to IS NULL, _is_current
FROM lake.silver.${entity}_history
ORDER BY ${key}, _commit_sequence;

SELECT 'current', ${key}, _commit_sequence, _change_op
FROM lake.silver.${entity}_current
ORDER BY ${key};
