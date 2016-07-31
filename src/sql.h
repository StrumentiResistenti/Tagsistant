/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor Boston, MA 02110-1301,  USA
 */

/*****************\
 * SQL FUNCTIONS *
\*****************/

#include <dbi/dbi.h>

#if  LIBDBI_LIB_CURRENT > 1
#define TAGSISTANT_REENTRANT_DBI 1
#else
#define TAGSISTANT_REENTRANT_DBI 0
#endif

#define TAGSISTANT_NULL_BACKEND			0
#define TAGSISTANT_DBI_MYSQL_BACKEND	1
#define TAGSISTANT_DBI_SQLITE_BACKEND	2

#define TAGSISTANT_COMMIT_TRANSACTION	1
#define TAGSISTANT_ROLLBACK_TRANSACTION	0

#ifndef TAGSISTANT_SQL_BACKEND
#	define TAGSISTANT_SQL_BACKEND TAGSISTANT_DBI_SQLITE_BACKEND
#endif

#define TAGSISTANT_START_TRANSACTION		1
#define TAGSISTANT_DONT_START_TRANSACTION	0

extern void tagsistant_db_init();
extern dbi_conn *tagsistant_db_connection(int start_transaction);
extern void tagsistant_create_schema();
extern void tagsistant_wal_sync();

#define _safe_string(string) string ? string : ""

/* number of active connections */
extern int connections;

/**
 * Tagsistant query callback function type definition
 */
typedef int (*tagsistant_query_callback)(void *, dbi_result);

/**
 * Prepare SQL queries and perform them.
 *
 * @param dbi a dbi_conn connection
 * @param format printf-like string with the SQL query
 * @param callback pointer to function to be called on results of SQL query
 * @param file the file where the function is called from (see tagsistant_query() macro)
 * @param file the file line where the function is called from (see tagsistant_query() macro)
 * @param firstarg pointer to buffer for callback returned data
 * @return the number of selected rows
 */
extern int tagsistant_real_query(
	dbi_conn conn,
	const char *format,
	int (*callback)(void *, dbi_result),
	char *file,
	int line,
	void *firstarg,
	...);

/**
 * execute SQL statements auto formatting the
 * SQL string and adding file:line coords
 */
#define tagsistant_query(format, conn, callback, firstarg, ...) \
	tagsistant_real_query(conn, format, callback, __FILE__, __LINE__, firstarg, ## __VA_ARGS__)

/** callback to return a string */
extern int tagsistant_return_string(void *return_string, dbi_result result);

/** callback to return an integer */
extern int tagsistant_return_integer(void *return_integer, dbi_result result);

extern void tagsistant_db_connection_release(dbi_conn dbi, gboolean is_writer_locked);

/**
 * if true, use backend calls to fetch last insert row id,
 * otherwise use libdbi dbi_conn_sequence_last()
 */
#define TAGSISTANT_USE_INTERNAL_SEQUENCES TRUE

/**
 * if true, start transactions using backend calls, otherwise use
 * libdbi dbi_conn_transaction_begin()
 */
#define TAGSISTANT_USE_INTERNAL_TRANSACTIONS TRUE

#if TAGSISTANT_USE_INTERNAL_TRANSACTIONS
#	define tagsistant_commit_transaction(dbi_conn) tagsistant_query("commit", dbi_conn, NULL, NULL)
#	define tagsistant_rollback_transaction(dbi_conn) tagsistant_query("rollback", dbi_conn, NULL, NULL)
#else
#	define tagsistant_commit_transaction(dbi_conn) dbi_conn_transaction_commit(dbi_conn)
#	define tagsistant_rollback_transaction(dbi_conn) dbi_conn_transaction_rollback(dbi_conn)
#endif /* TAGSISTANT_USE_INTERNAL_TRANSACTIONS */

/***************\
 * SQL QUERIES *
\***************/

extern void				tagsistant_sql_create_tag(dbi_conn conn, const gchar *tagname, const gchar *key, const gchar *value);
extern tagsistant_inode	tagsistant_sql_get_tag_id(dbi_conn conn, const gchar *tagname, const gchar *key, const gchar *value);
extern void				tagsistant_sql_delete_tag(dbi_conn conn, const gchar *tagname, const gchar *key, const gchar *value);
extern void				tagsistant_sql_tag_object(dbi_conn conn, const gchar *tagname, const gchar *key, const gchar *value, tagsistant_inode inode);
extern void				tagsistant_sql_untag_object(dbi_conn conn, const gchar *tagname, const gchar *key, const gchar *value, tagsistant_inode inode);
extern void				tagsistant_sql_rename_tag(dbi_conn conn, const gchar *tagname, const gchar *oldtagname);
extern tagsistant_inode	tagsistant_last_insert_id(dbi_conn conn);
extern int				tagsistant_object_is_tagged(dbi_conn conn, tagsistant_inode inode);
extern int				tagsistant_object_is_tagged_as(dbi_conn conn, tagsistant_inode inode, tagsistant_inode tag_id);
extern void				tagsistant_full_untag_object(dbi_conn conn, tagsistant_inode inode);
extern void				tagsistant_remove_tag_from_cache(const gchar *tagname, const gchar *key, const gchar *value);
extern int				tagsistant_sql_alias_exists(dbi_conn conn, const gchar *alias);
extern void				tagsistant_sql_alias_create(dbi_conn conn, const gchar *alias);
extern void				tagsistant_sql_alias_delete(dbi_conn conn, const gchar *alias);
extern void				tagsistant_sql_alias_set(dbi_conn conn, const gchar *alias, const gchar *query);
extern gchar *			tagsistant_sql_alias_get(dbi_conn conn, const gchar *alias);
extern size_t			tagsistant_sql_alias_get_length(dbi_conn conn, const gchar *alias);

/**
 * Prepare a key for saving a tag_id inside the cache
 *
 * @param tagname the name of the tag or the namespace of a triple tag
 * @param key the key of a triple tag
 * @param value the value of a triple tag
 */
#define tagsistant_make_tag_key(tagname, key, value) g_strdup_printf("%s<separator>%s<separator>%s", tagname, key, value)
