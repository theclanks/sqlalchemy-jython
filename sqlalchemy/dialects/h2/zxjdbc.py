"""Support for the H2 database via Jython's zxjdbc JDBC connector.

JDBC Driver
-----------

The official H2 database and JDBC driver is at
http://http://www.h2database.com/.

Character Sets
--------------

SQLAlchemy zxjdbc dialects pass unicode straight through to the
zxjdbc/JDBC layer.

"""
import re

from sqlalchemy import types as sqltypes, util
from sqlalchemy.connectors.zxJDBC import ZxJDBCConnector
from sqlalchemy.dialects.h2.base import H2Dialect

import org.h2.Driver

class H2_zxjdbc(ZxJDBCConnector, H2Dialect):
	jdbc_db_name = 'h2'
	jdbc_driver_name = 'org.h2.Driver'

	def _create_jdbc_url(self, url):
		"""Create a JDBC url from a :class:`~sqlalchemy.engine.url.URL`"""

		opts = url.translate_connect_args(database='db', username='user',password='passwd')
		opts.update(url.query)

		util.coerce_kw_type(opts, 'mode', str)

		print opts['mode']
		
		print url.database

		jdbc_url = 'jdbc:%s:%s://localhost/%s;' % (self.jdbc_db_name, opts['mode'], url.database)

		jdbc_url = ''

		print jdbc_url

		#MVCC=TRUE, can't change when db opened
		return jdbc_url
		#return 'jdbc:%s:%s;TRACE_LEVEL_SYSTEM_OUT=2' % (self.jdbc_db_name, url.database)

	def _driver_kwargs(self):
		"""return kw arg dict to be sent to connect()."""
		return {}
		#return dict(characterEncoding='UTF-8', yearIsDateType='false')

	def _extract_error_code(self, exception):
		# e.g.: DBAPIError: (Error) Table 'test.u2' doesn't exist
		# [SQLCode: 1146], [SQLState: 42S02] 'DESCRIBE `u2`' ()
		m = re.compile(r"\[SQLCode\: (\d+)\]").search(str(exception.orig.args))
		c = m.group(1)
		if c:
			return int(c)

dialect = H2_zxjdbc
