from yav import ProcessorModule
from yav.dependency import *
from cladrbot.logger_db import Base as LoggerBase

from logging import getLogger

class process_street(ProcessorModule):
    cls_id = 'cladrbot.process_street'
    logger = getLogger('cladrbot.process_street')

    dependencies = {'process': [(Process, 'yav_settlement.settlement_kladr'),
                                (Process, 'yav.osm2pgsql')] }

    def do_install(self):
        LoggerBase.metadata.create_all(self.manager.engine)

    def do_uninstall(self):
        LoggerBase.metadata.drop_all(self.manager.engine)


    def do_process(self):
        from cladrbot.osm_db import OsmDB
        from cladrbot.cladr_db import CladrDB
        from cladrbot.logger_db import DatabaseLogger
        from cladrbot.process_streets import process as process_polygon

        import traceback
        
        osm_db = OsmDB(engine=self.manager.engine, session=self.manager.session)
        cladr_db = CladrDB(engine=self.manager.engine, session=self.manager.session)
        result_listeners = [DatabaseLogger(engine=self.manager.engine, session=self.manager.session)]
        
        for settlement in osm_db.query_settlements():
            self.logger.info("Processing city #%s (%s)" % (str(settlement.polygon_osm_id), str(settlement.kladr)))
        
            process_polygon(settlement.id, str(settlement.polygon_osm_id), '%013s' % (settlement.kladr), osm_db, cladr_db, result_listeners)


