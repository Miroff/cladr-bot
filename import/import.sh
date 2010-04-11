#!/bin/bash

wget -c -t0 http://gis-lab.info/data/osm/novosib/novosib.osm.bz2
osm2pgsql -l -c -d osm -G -S default.style --prefix=osm -x novosib.osm.bz2
psql osm <post-import.sql
rm novosib.osm.bz2
