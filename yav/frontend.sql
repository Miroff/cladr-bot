/* $MD_INIT$

DROP TABLE IF EXISTS street_browser;

CREATE TABLE street_browser (
  src varchar(10),
  id bigint,
  settlement_src varchar(10),
  settlement_id bigint,
  sort_name text,
  names text[],
  status varchar(10),
  kladr_id bigint,
  related varchar(50)[],
  CONSTRAINT street_browser_pk PRIMARY KEY (src, id)
);

$MD_INIT$ */

--@: log street browser
--@: level +
BEGIN TRANSACTION;

TRUNCATE street_browser;

--@: log match + mismatch
INSERT INTO street_browser (src, id, settlement_src, settlement_id, sort_name, names, status, kladr_id)
SELECT 'osm', s.id, 'osm', s.settlement_id, s.sort_name, s.names, 
  CASE WHEN sk.street_id IS NOT NULL THEN 'match' ELSE 'mismatch' END,
  sk.kladr_id
FROM street s
  LEFT JOIN street_kladr_match sk ON sk.street_id = s.id;

--@: log none
INSERT INTO street_browser 
SELECT 'kladr', cc.code, 'osm', s.id, cc.name || ', ' || lower(cc.type), NULL,
  'none',
  cc.code
FROM settlement s INNER JOIN cladr cp ON cp.code = s.kladr
  INNER JOIN cladr cc ON cc.code_prefix = cp.code_prefix || cp.code_suffix
  LEFT JOIN street_kladr_match sk ON sk.kladr_id = cc.code
WHERE sk.kladr_id IS NULL;

--@ log related objects
UPDATE street_browser SET related = tmp.related
FROM (
  SELECT street_id, array_agg(cls || '/' || osm_id) AS related 
  FROM street_obj GROUP BY street_id
) tmp
WHERE tmp.street_id = id;

COMMIT;
--@: level -