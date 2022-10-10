from gtfspy.import_loaders.table_loader import TableLoader, decode_six


class RoutePatternLoader(TableLoader):
    fname = None
    table = 'route_patterns'
    tabledef = '(route_pattern_I INTEGER PRIMARY KEY, ' \
               'route_pattern_id TEXT UNIQUE NOT NULL, ' \
               'route_I INT, ' \
               'direction_id TEXT, ' \
               'name TEXT, ' \
               'sort_order INT, ' \
               'n_trips INT, ' \
               'shape_id TEXT ' \
               ')'

    def post_import(self, cur):
        stmt = (
            "INSERT INTO route_patterns "
            "(route_pattern_id, route_I, direction_id, name, sort_order, n_trips, shape_id)"
            "SELECT (route_id || '_' || direction_id || '_' || sort_order) as route_pattern_id, "
                "route_I, direction_id, name, sort_order, n_trips, shape_id "
            "FROM ( "
            "SELECT routes.route_I, routes.name, routes.route_id, trips.direction_id, trips.headsign, trips.shape_id, "
                "COUNT(trips.trip_I) as n_trips, "
                "ROW_NUMBER() OVER(PARTITION BY routes.route_I, trips.direction_id ORDER BY COUNT(trips.trip_I) DESC) as sort_order "
            "FROM routes "
            "JOIN trips ON routes.route_I == trips.route_I "
            "GROUP BY trips.shape_id "
            ") "
        )
        cur.execute(stmt)
    
    def index(self, cur):
        cur.execute('CREATE INDEX IF NOT EXISTS idx_route_pattern_id ON route_patterns (route_pattern_id)')