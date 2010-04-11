ALTER TABLE osm_polygon ADD COLUMN way_valid boolean;
UPDATE osm_polygon SET way_valid = ST_IsValid(way);
