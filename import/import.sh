#!/bin/bash

wget -c -t0 http://gis-lab.info/data/osm/novosib/novosib.osm.bz2
osm2pgsql -l -c -d osm -G -S default.style novosib.osm.bz2
