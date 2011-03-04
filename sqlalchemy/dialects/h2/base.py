# h2.py
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""Support for the H2 database.

For information on connecting using a specific driver, see the documentation
section regarding that driver.

"""

import datetime, re, time

from sqlalchemy import schema as sa_schema
from sqlalchemy import sql, exc, pool, DefaultClause
from sqlalchemy.engine import default
from sqlalchemy.engine import reflection
from sqlalchemy import types as sqltypes
from sqlalchemy import util
from sqlalchemy.sql import compiler, functions as sql_functions
from sqlalchemy.util import NoneType

from sqlalchemy.types import BIGINT, BINARY, BLOB, BOOLEAN, CHAR, CLOB, DATE, DATETIME, DECIMAL,\
							FLOAT, INTEGER, NUMERIC, SMALLINT, TEXT, TIME,\
							TIMESTAMP, VARCHAR


#class H2TimeStamp(TIMESTAMP):
#	def get_col_spec(self):
#		return 'TIMESTAMP'

colspecs = {
#	sqltypes.TIMESTAMP: H2TimeStamp,
}

ischema_names = {
	'BIGINT': BIGINT,
	'BINARY': BINARY,
	'BLOB': BLOB,
	'BOOLEAN': BOOLEAN,
	'CHAR': CHAR,
	'CLOB': CLOB,
	'DATE': DATE,
	'DECIMAL': DECIMAL,
	'DOUBLE': NUMERIC,
	'INT': INTEGER,
	'INTEGER': INTEGER,
	#'REAL': REAL,
	'SMALLINT': SMALLINT,
	'TIME': TIME,
	'TIMESTAMP': TIMESTAMP,
	#'TINYINT': TINYINT,
	'VARCHAR': VARCHAR,
	'VARCHAR_IGNORECASE': VARCHAR,
}


class H2Compiler(compiler.SQLCompiler):
	extract_map = compiler.SQLCompiler.extract_map.copy()

	def visit_now_func(self, fn, **kw):
		return "CURRENT_TIMESTAMP"

	def for_update_clause(self, select):
		return ''


class H2DDLCompiler(compiler.DDLCompiler):

	def get_column_specification(self, column, **kwargs):
		colspec = self.preparer.format_column(column) + " " + self.dialect.type_compiler.process(column.type)
		default = self.get_column_default_string(column)
		if default is not None:
			colspec += " DEFAULT " + default

		if not column.nullable:
			colspec += " NOT NULL"

		if column.primary_key and \
			 len(column.table.primary_key.columns) == 1 and \
			 isinstance(column.type, sqltypes.Integer) and \
			 not column.foreign_keys:
			 colspec += " PRIMARY KEY AUTO_INCREMENT"

		return colspec

class H2TypeCompiler(compiler.GenericTypeCompiler):
	pass

class H2IdentifierPreparer(compiler.IdentifierPreparer):
	reserved_words = set([
		'add', 'after', 'all', 'alter', 'analyze', 'and', 'as', 'asc',
		'attach', 'autoincrement', 'before', 'begin', 'between', 'by',
		'cascade', 'case', 'cast', 'check', 'collate', 'column', 'commit',
		'conflict', 'constraint', 'create', 'cross', 'current_date',
		'current_time', 'current_timestamp', 'database', 'default',
		'deferrable', 'deferred', 'delete', 'desc', 'detach', 'distinct',
		'drop', 'each', 'else', 'end', 'escape', 'except', 'exclusive',
		'explain', 'false', 'fail', 'for', 'foreign', 'from', 'full', 'glob',
		'group', 'having', 'if', 'ignore', 'immediate', 'in', 'index',
		'indexed', 'initially', 'inner', 'insert', 'instead', 'intersect', 'into', 'is',
		'isnull', 'join', 'key', 'left', 'like', 'limit', 'match', 'natural',
		'not', 'notnull', 'null', 'of', 'offset', 'on', 'or', 'order', 'outer',
		'plan', 'pragma', 'primary', 'query', 'raise', 'references',
		'reindex', 'rename', 'replace', 'restrict', 'right', 'rollback',
		'row', 'select', 'set', 'table', 'temp', 'temporary', 'then', 'to',
		'transaction', 'trigger', 'true', 'union', 'unique', 'update', 'using',
		'vacuum', 'values', 'view', 'virtual', 'when', 'where',
		])

	def __init__(self, dialect, initial_quote="",
					final_quote=None, escape_quote="", omit_schema=False):
		super(H2IdentifierPreparer, self).__init__(
									dialect=dialect,
									initial_quote=initial_quote,
									final_quote=final_quote,
									escape_quote=escape_quote,
									
									omit_schema=omit_schema,
									)

class H2Dialect(default.DefaultDialect):
	name = 'h2'
	supports_alter = True
	supports_unicode_statements = True
	supports_unicode_binds = True
	returns_unicode_strings = True
	supports_default_values = True
	supports_empty_insert = False
	supports_cast = True
	supports_native_boolean = True

	default_paramstyle = 'qmark'
	statement_compiler = H2Compiler
	ddl_compiler = H2DDLCompiler
	type_compiler = H2TypeCompiler
	preparer = H2IdentifierPreparer
	ischema_names = ischema_names
	colspecs = colspecs

	def do_begin(self, connect):
		cu = connect.cursor()
		cu.execute('SET AUTOCOMMIT ON')

	#def do_commit(self, connect):
	#	pass

	#def do_rollback(self, connect):
	#	pass

	def table_names(self, connection, schema):
		quote = self.identifier_preparer.quote_identifier
		if schema is not None:
			s = ("SELECT table_name FROM information_schema.tables "
				 "WHERE table_type='TABLE' AND table_schema=%s ORDER BY table_name") % (quote(schema.upper()),)
		else:
			s = ("SELECT table_name FROM information_schema.tables "
				"WHERE table_type='TABLE' AND table_schema='PUBLIC' ORDER BY table_name")

		rs = connection.execute(s)
		return [row[0] for row in rs]

	@reflection.cache
	def get_schema_names(self, connection, **kw):
			s = """
			SELECT
				SCHEMA_NAME
			FROM
				INFORMATION_SCHEMA.SCHEMATA;
			"""
			rp = connection.execute(s)
			# what about system tables?
			# Py3K
			#schema_names = [row[0] for row in rp \
			#				if not row[0].startswith('pg_')]
			# Py2K
			schema_names = [row[0].decode(self.encoding) for row in rp]
			# end Py2K
			return schema_names
	
	def _get_default_schema_name(self, connection):
		return 'PUBLIC'
		#return connection.scalar("select current_schema()")

	def has_table(self, connection, table_name, schema=None):
		quote = self.identifier_preparer.quote_identifier
		if schema is not None:
			s = ("SELECT table_name FROM information_schema.tables "
					  "WHERE table_type='TABLE' AND table_schema=%s AND table_name=%s") % (quote(schema.upper()), quote(table_name.upper()))
		else:
			s = ("SELECT table_name FROM information_schema.tables "
					  "WHERE table_type='TABLE' AND table_schema='PUBLIC' AND table_name=%s") % (quote(table_name.upper()),)

		rs = connection.execute(s)
		row = rs.fetchone()

		return (row is not None)

	@reflection.cache
	def get_table_names(self, connection, schema=None, **kw):
		return self.table_names(connection, schema)

	@reflection.cache
	def get_columns(self, connection, table_name, schema=None, **kw):

		if schema==None:
			schema = self._get_default_schema_name(connection)
		SQL_COLS = """
				 SELECT C.COLUMN_NAME, C.TYPE_NAME,
					C.COLUMN_DEFAULT,
					C.IS_NULLABLE,
					(Select T.AUTO_INCREMENT
					from INFORMATION_SCHEMA.TYPE_INFO T
					where T.DATA_TYPE = C.DATA_TYPE and T.TYPE_NAME=C.TYPE_NAME ) AS AUTO_INCREMENT,
					C.CHARACTER_MAXIMUM_LENGTH
					FROM INFORMATION_SCHEMA.COLUMNS C
					   WHERE TABLE_NAME = '%s'
					   AND TABLE_SCHEMA = '%s'
		"""
		#PUBLIC <--- select current_schema()
		

		SQL_COLS = SQL_COLS % (table_name,schema)

		s = sql.text(SQL_COLS,
					 
					 typemap={'COLUMN_NAME':sqltypes.Unicode,
							  'TYPE_NAME':sqltypes.Unicode,
							  'COLUMN_DEFAULT':sqltypes.Unicode,
							  'IS_NULLABLE':sqltypes.BOOLEAN,
							  'AUTO_INCREMENT':sqltypes.BOOLEAN,
							  'CHARACTER_MAXIMUM_LENGTH':sqltypes.INTEGER}
					 )

		c = connection.execute(s)
		rows = c.fetchall()
#		domains = self._load_domains(connection)
#		enums = self._load_enums(connection)

		# format columns
		columns = []
		for column_name, type_name, default, nullable, autoincrement, charlen in rows:
			
			
			#is_array = format_type.endswith('[]')
			#charlen = re.search('\(([\d,]+)\)', format_type)
			#if charlen:
			#		charlen = charlen.group(1)
			kwargs = {}

			#if attype == 'numeric':
			#		if charlen:
			#				prec, scale = charlen.split(',')
			#				args = (int(prec), int(scale))
			#		else:
			#				args = ()
			#elif attype == 'double precision':
			#		args = (53, )
			#elif attype == 'integer':
			#		args = ()
			#elif attype in ('timestamp with time zone',
			#								'time with time zone'):
			#		kwargs['timezone'] = True
			#		if charlen:
			#				kwargs['precision'] = int(charlen)
			#		args = ()
			#elif attype in ('timestamp without time zone',
			#								'time without time zone', 'time'):
			#		kwargs['timezone'] = False
			#		if charlen:
			#				kwargs['precision'] = int(charlen)
			#		args = ()
			#elif attype in ('interval','interval year to month',
			#										'interval day to second'):
			#		if charlen:
			#				kwargs['precision'] = int(charlen)
			#		args = ()
			#elif charlen:
			#		args = (int(charlen),)
			#else:
			#		args = ()

			#while True:
			#		if attype in self.ischema_names:
			#				coltype = self.ischema_names[attype]
			#				break
			#		elif attype in enums:
			#				enum = enums[attype]
			#				coltype = ENUM
			#				if "." in attype:
			#						kwargs['schema'], kwargs['name'] = attype.split('.')
			#				else:
			#						kwargs['name'] = attype
			#				args = tuple(enum['labels'])
			#				break
			#		elif attype in domains:
			#				domain = domains[attype]
			#				attype = domain['attype']
			#				# A table can't override whether the domain is nullable.
			#				nullable = domain['nullable']
			#				if domain['default'] and not default:
			#						# It can, however, override the default
			#						# value, but can't set it to null.
			#						default = domain['default']
			#				continue
			#		else:
			#				coltype = None
			#				break

			#if coltype:
			#		coltype = coltype(*args, **kwargs)
			#		if is_array:
			#				coltype = ARRAY(coltype)
			#else:
			#		util.warn("Did not recognize type '%s' of column '%s'" %
			#						  (attype, name))
			#		coltype = sqltypes.NULLTYPE
			# adjust the default value
			
			if default is not None:
					match = re.search(r"""(NEXT VALUE FOR )([^\.]+)('.*$)""", default)
					if match is not None:
							autoincrement = True
							# the default is related to a Sequence
							#sch = schema
							#if '.' not in match.group(2) and sch is not None:
							#		# unconditionally quote the schema name.  this could
							#		# later be enhanced to obey quoting rules /
							#		# "quote schema"
							#		default = match.group(1) + \
							#								('"%s"' % sch) + '.' + \
							#								match.group(2) + match.group(3)

			column_info = dict(name=column_name, type=ischema_names[type_name], nullable=nullable,
											   default=default, autoincrement=autoincrement)
			columns.append(column_info)
		return columns

	@reflection.cache
	def get_primary_keys(self, connection, table_name, schema=None, **kw):
		if schema==None:
			schema = self._get_default_schema_name(connection)
		PK_SQL = """
		  select COLUMN_NAME from
			INFORMATION_SCHEMA.INDEXES
			where PRIMARY_KEY = 'TRUE'
			AND TABLE_NAME = '%s'
			AND TABLE_SCHEMA = '%s'
		"""
		PK_SQL = PK_SQL % (table_name, schema)
		t = sql.text(PK_SQL, typemap={'COLUMN_NAME':sqltypes.Unicode})
		c = connection.execute(t)
		primary_keys = [r[0] for r in c.fetchall()]

		return primary_keys

	@reflection.cache
	def get_pk_constraint(self, connection, table_name, schema=None, **kw):

		if schema==None:
			schema = self._get_default_schema_name(connection)

		cols = self.get_primary_keys(connection, table_name,
											schema=schema, **kw)

		PK_CONS_SQL = """
		SELECT CONSTRAINT_NAME
			FROM INFORMATION_SCHEMA.CONSTRAINTS
			WHERE  TABLE_NAME= '%s'
			and TABLE_SCHEMA = '%s'
			and CONSTRAINT_TYPE = 'PRIMARY_KEY'

		"""

		PK_CONS_SQL = PK_CONS_SQL % (table_name, schema)

		t = sql.text(PK_CONS_SQL, typemap={'CONSTRAINT_NAME':sqltypes.Unicode})
		c = connection.execute(t)
		name = c.scalar()
		return {
			'constrained_columns':cols,
			'name':name
		}

	@reflection.cache
	def get_foreign_keys(self, connection, table_name, schema=None, **kw):

		if schema==None:
			schema = self._get_default_schema_name(connection)

		preparer = self.identifier_preparer

		FK_CONS_SQL = """
		SELECT CONSTRAINT_NAME, SQL as condef
			FROM INFORMATION_SCHEMA.CONSTRAINTS
			WHERE  TABLE_NAME= '%s'
			and TABLE_SCHEMA = '%s'
			and CONSTRAINT_TYPE = 'REFERENTIAL'
		"""

		FK_CONS_SQL = FK_CONS_SQL % (table_name, schema)

		t = sql.text(FK_CONS_SQL, typemap={
								'CONSTRAINT_NAME':sqltypes.Unicode,
								'condef':sqltypes.Unicode})
		c = connection.execute(t)
		fkeys = []
		for conname, condef in c.fetchall():

			m = re.search('FOREIGN KEY\((.*?)\).*?REFERENCES (?:(.*?)\.)?(.*?)\((.*?)\)', condef).groups()

			constrained_columns, referred_schema, \
					referred_table, referred_columns = m
			constrained_columns = [preparer._unescape_identifier(x)
						for x in re.split(r'\s*,\s*', constrained_columns)]
			if referred_schema:
				referred_schema =\
								preparer._unescape_identifier(referred_schema)
			elif schema is not None and schema == self.default_schema_name:
				# no schema (i.e. its the default schema), and the table we're
				# reflecting has the default schema explicit, then use that.
				# i.e. try to use the user's conventions
				referred_schema = schema
			referred_table = preparer._unescape_identifier(referred_table)
			referred_columns = [preparer._unescape_identifier(x)
						for x in re.split(r'\s*,\s', referred_columns)]
			fkey_d = {
				'name' : conname,
				'constrained_columns' : constrained_columns,
				'referred_schema' : referred_schema,
				'referred_table' : referred_table,
				'referred_columns' : referred_columns
			}
			fkeys.append(fkey_d)
		return fkeys

	@reflection.cache
	def get_indexes(self, connection, table_name, schema, **kw):

		if schema==None:
			schema = self._get_default_schema_name(connection)
			
		IDX_SQL = """
		  SELECT
			INDEX_NAME,
			NON_UNIQUE,
			COLUMN_NAME
		FROM
			INFORMATION_SCHEMA.INDEXES
		WHERE
			TABLE_NAME= '%s'
			AND TABLE_SCHEMA = '%s'
		"""

		IDX_SQL = IDX_SQL % (table_name, schema)

		t = sql.text(IDX_SQL, typemap={'INDEX_NAME':sqltypes.Unicode,
						'NON_UNIQUE':sqltypes.BOOLEAN,
						'COLUMN_NAME':sqltypes.Unicode,
						}
					)
		c = connection.execute(t)
		index_names = {}
		indexes = []
#		sv_idx_name = None
		for row in c.fetchall():
			idx_name, unique, col = row
#			if expr:
#				if idx_name != sv_idx_name:
#					util.warn(
#					  "Skipped unsupported reflection of "
#					  "expression-based index %s"
#					  % idx_name)
#				sv_idx_name = idx_name
#				continue

#			if prd and not idx_name == sv_idx_name:
#				util.warn(
#				   "Predicate of partial index %s ignored during reflection"
#				   % idx_name)
#				sv_idx_name = idx_name

			if idx_name in index_names:
				index_d = index_names[idx_name]
			else:
				index_d = {'column_names':[]}
				indexes.append(index_d)
				index_names[idx_name] = index_d
				
			index_d['name'] = idx_name
			index_d['column_names'].append(col)
			index_d['unique'] = unique
		return indexes
