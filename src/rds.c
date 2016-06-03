/*
   Tagsistant (tagfs) -- rds.c
   Copyright (C) 2006-2013 Tx0 <tx0@strumentiresistenti.org>

   Manage Reusable Data Sets including query results.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
*/

#include "tagsistant.h"

/************************************************************************************/
/***                                                                              ***/
/*** RDS are reusable data sets with query results. An RDS can be served on       ***/
/*** answering to a readdir() call or to a getattr() call. It's basically a       ***/
/*** cache and has to be invalidated when data change.                            ***/
/***                                                                              ***/
/************************************************************************************/

#define TAGSISTANT_RDS_HARD_CLEAN TRUE

GRWLock tagsistant_rds_cache_rwlock;
GHashTable *tagsistant_rds_cache = NULL;

/**
 * add a file to the file tree (callback function)
 *
 * @param hash_table_pointer a GHashTable to hold results
 * @param result a DBI result
 */
static int
tagsistant_rds_materialize_entry(tagsistant_rds *rds, dbi_result result)
{
	/*
	 * fetch query results
	 */
	tagsistant_inode inode = dbi_result_get_uint_idx(result, 1);
	gchar *name = dbi_result_get_string_copy_idx(result, 2);

	dbg('f', LOG_INFO, "adding (%d,%s) to RDS %s", inode, name, rds->checksum);

	/*
	 * lookup the GList object. Since a filename can feature more
	 * than once with different inodes, the value of hash_table keys
	 * is a GList that holds different inodes
	 */
	GList *list = (GList *) (rds->entries ? g_hash_table_lookup(rds->entries, name) : NULL);
	list = g_list_prepend(list, GUINT_TO_POINTER(inode));
	dbg('f', LOG_INFO, "Adding inode %d, list holds %d elements", inode, g_list_length(list));

	/*
	 * save the new start of the GList inside the hash table
	 */
	g_hash_table_insert(rds->entries, g_strdup(name), list);

	return (0);
}

/**
 * To be called once inside a g_hash_table_foreach()
 * after RDS materializing, removes duplicated entries
 * for each object name
 *
 * @param object_name the object name
 * @param inode_list the GList with the inodes bound to the object name
 * @param rds the tagsistant_rds object just materialized
 */
void
tagsistant_rds_uniq_entries(gchar *object_name, GList *inode_list, tagsistant_rds *rds)
{
	(void) object_name;
	(void) rds;

	/*
	 * a GHashTable used as a set: each inode will be
	 * recorded just once as a key of the hash table
	 */
	GHashTable *set = g_hash_table_new(NULL, NULL);

	/*
	 * iterate the GList elements
	 */
	GList *ptr = inode_list;
	while (ptr) {
		if (g_hash_table_contains(set, ptr->data)) {
			inode_list = g_list_delete_link(inode_list, ptr);
			// dbg('f', LOG_INFO, "Removing duplicate inode %d", GPOINTER_TO_UINT(ptr->data));
		} else {
			g_hash_table_add(set, ptr->data);
			// dbg('f', LOG_INFO, "Confirming inode %d", GPOINTER_TO_UINT(ptr->data));
		}

		ptr = ptr->next;
	}

	/*
	 * drop the hash table
	 */
	g_hash_table_unref (set);
}


/**
 * Add a filter criterion to a WHERE clause based on a qtree_and_node object
 *
 * @param statement a GString object holding the building query statement
 * @param and_set the qtree_and_node object describing the tag to be added as a criterion
 */
void tagsistant_query_add_and_set(GString *statement, qtree_and_node *and_set)
{
	if (!and_set) {
		dbg('f', LOG_ERR, "tagsistant_query_add_and_set() called with NULL and_set");
		return;
	}

	if (!statement) {
		dbg('f', LOG_ERR, "tagsistant_query_add_and_set() called with NULL statement");
		return;
	}

	if (and_set->value && strlen(and_set->value)) {
		switch (and_set->operator) {
			case TAGSISTANT_EQUAL_TO:
				g_string_append_printf(statement,
					"tagname = \"%s\" and `key` = \"%s\" and value = \"%s\" ",
					and_set->namespace,
					and_set->key,
					and_set->value);
				break;
			case TAGSISTANT_CONTAINS:
				g_string_append_printf(statement,
					"tagname = \"%s\" and `key` = \"%s\" and value like '%%%s%%' ",
					and_set->namespace,
					and_set->key,
					and_set->value);
				break;
			case TAGSISTANT_GREATER_THAN:
				g_string_append_printf(statement,
					"tagname = \"%s\" and `key` = \"%s\" and value > \"%s\" ",
					and_set->namespace,
					and_set->key,
					and_set->value);
				break;
			case TAGSISTANT_SMALLER_THAN:
				g_string_append_printf(statement,
					"tagname = \"%s\" and `key` = \"%s\" and value < \"%s\" ",
					and_set->namespace,
					and_set->key,
					and_set->value);
				break;
		}
	} else if (and_set->tag) {
		g_string_append_printf(statement, "tagname = \"%s\" ", and_set->tag);
	} else if (and_set->tag_id) {
		g_string_append_printf(statement, "tagging.tag_id = %d ", and_set->tag_id);
	}
}

/*
 * Return the query portion up to the delimiter token
 *
 * @param qtree the tagsistant_querytree object
 * @return a string containing the query up to the delimiter token
 */
gchar *tagsistant_rds_path(tagsistant_querytree *qtree)
{
	gchar *path = g_strdup(qtree->expanded_full_path);
	gchar *delimiter = strstr(path, TAGSISTANT_QUERY_DELIMITER);
	if (delimiter) { *delimiter = '\0'; }
	return (path);
}

/*
 * Compute the checksum identifying a RDS
 *
 * @param qtree the tagsistant_querytree object
 * @return a string containing the checksum
 */
gchar *tagsistant_get_rds_checksum(tagsistant_querytree *qtree)
{
	/*
	 * Extract the path to be looked up
	 */
	gchar *path = tagsistant_rds_path(qtree);

	/*
	 * compute the checksum
	 */
	gchar *checksum = g_compute_checksum_for_string(G_CHECKSUM_MD5, path, -1);

	g_free(path);
	return (checksum);
}

#if 0
/*
 * Append a tag to a sources GString
 *
 * @param sources a GString object containing the source string
 * @param and a qtree_and_node to be added to the source string
 */
void tagsistant_register_rds_source(GString *sources, qtree_and_node *and)
{
	gchar *tag = (and->namespace && strlen(and->namespace)) ? and->namespace : and->tag;
	g_string_append(sources, tag);
	g_string_append(sources, "|");
}

/*
 * Create a new RDS for the query and return its id.
 *
 * @param query the query that generates the RDS
 * @return the RDS id
 */
gchar *tagsistant_compute_rds_sources(tagsistant_querytree *qtree)
{
	GString *sources = g_string_sized_new(64000);
	g_string_append(sources, "|");

	/*
	 * For every subquery, save the source tags
	 */
	qtree_or_node *query = qtree->tree;
	while (query) {
		/*
		 * Register every tag...
		 */
		qtree_and_node *and = query->and_set;

		while (and) {
			tagsistant_register_rds_source(sources, and);

			/*
			 * ... and every related tag ...
			 */
			qtree_and_node *related = and->related;
			while (related) {
				tagsistant_register_rds_source(sources, related);
				related = related->related;
			}

			/*
			 * ... and even negated tags.
			 */
			qtree_and_node *negated = and->negated;
			while (negated) {
				tagsistant_register_rds_source(sources, negated);
				negated = negated->negated;
			}

			and = and->next;
		}
		query = query->next;
	}

	/*
	 * Free the GString object and return the sources string
	 */
	gchar *source_string = sources->str;
	g_string_free(sources, FALSE);

	return (source_string);
}
#endif

/**
 * Materialize the RDS of a query
 *
 * @param qtree the querytree object
 * @return the RDS checksum id
 */
gboolean
tagsistant_rds_materialize(tagsistant_rds *rds, tagsistant_querytree *qtree)
{
	/*
	 * Declare the entries hash table
	 */
#if 0
	rds->entries = g_hash_table_new_full(
		g_str_hash, g_str_equal,
		(GDestroyNotify) g_free,
		(GDestroyNotify) g_list_free
	);
#else
	rds->entries = g_hash_table_new(g_str_hash, g_str_equal);
#endif

	if (!rds->entries) {
		dbg('f', LOG_ERR, "Error allocating RDS entries");
		return (FALSE);
	}
	g_hash_table_ref(rds->entries);

	/*
	 * If the RDS is an ALL path, just load all the objects
	 */
	if (rds->is_all_path) {
		tagsistant_query(
			"select inode, objectname from objects",
			qtree->dbi,
			(tagsistant_query_callback) tagsistant_rds_materialize_entry,
			rds);

		/*
		 * remove entry duplicates
		 */
		g_hash_table_foreach(rds->entries, (GHFunc) tagsistant_rds_uniq_entries, rds);
		return (TRUE);
	}

	/*
	 * PHASE 1.
	 * Build a set of temporary tables containing all the matched objects
	 *
	 * Step 1.1. for each qtree_or_node build a temporary table
	 */
	qtree_or_node *query = qtree->tree;

	while (query) {
		GString *create_base_table = g_string_sized_new(51200);
		g_string_append_printf(create_base_table,
			"create temporary table tv%.16" PRIxPTR " as "
			"select objects.inode, objects.objectname from objects "
				"join tagging on tagging.inode = objects.inode "
				"join tags on tags.tag_id = tagging.tag_id "
				"where ",
			(uintptr_t) query);

		/*
		 * add each qtree_and_node (main and ->related) to the query
		 */
		tagsistant_query_add_and_set(create_base_table, query->and_set);

		qtree_and_node *related = query->and_set ? query->and_set->related : NULL;
		while (related) {
			g_string_append(create_base_table, " or ");
			tagsistant_query_add_and_set(create_base_table, related);
			related = related->related;
		}

		/*
		 * create the table and dispose the statement GString
		 */
		tagsistant_query(create_base_table->str, qtree->dbi, NULL, NULL);
		g_string_free(create_base_table, TRUE);

		/*
		 * Step 1.2.
		 * for each ->next linked node, subtract from the base table
		 * the objects not matching this node
		 */
		qtree_and_node *next = query->and_set ? query->and_set->next : NULL;
		while (next) {
			GString *cross_tag = g_string_sized_new(51200);
			g_string_append_printf(cross_tag,
				"delete from tv%.16" PRIxPTR " where inode not in ("
				"select objects.inode from objects "
					"join tagging on tagging.inode = objects.inode "
					"join tags on tags.tag_id = tagging.tag_id "
					"where ",
				(uintptr_t) query);

			/*
			 * add each qtree_and_node (main and ->related) to the query
			 */
			tagsistant_query_add_and_set(cross_tag, next);

			qtree_and_node *related = next->related;
			while (related) {
				g_string_append(cross_tag, " or ");
				tagsistant_query_add_and_set(cross_tag, related);
				related = related->related;
			}

			/*
			 * close the subquery
			 */
			g_string_append(cross_tag, ")");

			/*
			 * apply the query and dispose the statement GString
			 */
			tagsistant_query(cross_tag->str, qtree->dbi, NULL, NULL);
			g_string_free(cross_tag, TRUE);

			next = next->next;
		}

		/*
		 * Step 1.3.
		 * for each ->negated linked node, subtract from the base table
		 * the objects that do match this node
		 */
		next = query->and_set;
		while (next) {
			qtree_and_node *negated = next->negated;
			while (negated) {
				GString *cross_tag = g_string_sized_new(51200);
				g_string_append_printf(cross_tag,
					"delete from tv%.16" PRIxPTR " where inode in ("
					"select objects.inode from objects "
						"join tagging on tagging.inode = objects.inode "
						"join tags on tags.tag_id = tagging.tag_id "
						"where ",
					(uintptr_t) query);

				/*
				 * add each qtree_and_node (main and ->related) to the query
				 */
				tagsistant_query_add_and_set(cross_tag, negated);

				qtree_and_node *related = negated->related;
				while (related) {
					g_string_append(cross_tag, " or ");
					tagsistant_query_add_and_set(cross_tag, related);
					related = related->related;
				}

				/*
				 * close the subquery
				 */
				g_string_append(cross_tag, ")");

				/*
				 * apply the query and dispose the statement GString
				 */
				tagsistant_query(cross_tag->str, qtree->dbi, NULL, NULL);
				g_string_free(cross_tag, TRUE);

				negated = negated->negated;
			}

			next = next->next;
		}

		/*
		 * move to the next qtree_or_node in the linked list
		 */
		query = query->next;
	}

	/*
	 * format the main statement which reads from the temporary
	 * tables using UNION and ordering the files
	 */
	GString *view_statement = g_string_sized_new(10240);

	query = qtree->tree;

	while (query) {
		switch (tagsistant.sql_database_driver) {
			case TAGSISTANT_DBI_SQLITE_BACKEND:
				g_string_append_printf(view_statement,
					"select inode, objectname from tv%.16" PRIxPTR, (uintptr_t) query);
				break;
			case TAGSISTANT_DBI_MYSQL_BACKEND:
				g_string_append_printf(view_statement,
					"select inode, objectname from tv%.16" PRIxPTR, (uintptr_t) query);
				break;
			default:
				g_string_append_printf(view_statement,
					"select inode, objectname from tv%.16" PRIxPTR, (uintptr_t) query);
				break;
		}

		if (query->next) g_string_append(view_statement, " union ");
		query = query->next;
	}

	/*
	 * Load all the files in the RDS
	 */
	tagsistant_query(view_statement->str, qtree->dbi,
		(tagsistant_query_callback) tagsistant_rds_materialize_entry, rds);

	g_string_free(view_statement, TRUE);

	/*
	 * remove entry duplicates
	 */
	g_hash_table_foreach(rds->entries, (GHFunc) tagsistant_rds_uniq_entries, rds);

	/*
	 * PHASE 3.
	 *
	 * drop the temporary tables
	 */
	query = qtree->tree;
	while (query) {
		tagsistant_query("drop table tv%.16" PRIxPTR, qtree->dbi, NULL, NULL, (uintptr_t) query);
		query = query->next;
	}

	return (TRUE);
}

#if 0
/**
 * Deletes the oldest RDS from the rds table
 *
 * @param qtree the querytree object used for DBI access
 */
void
tagsistant_rds_delete_oldest(tagsistant_querytree *qtree)
{
	int rds_id;
	tagsistant_query("select min(id) from rds", qtree->dbi, tagsistant_return_integer, &rds_id);
	dbg('f', LOG_INFO, "Garbage collector: deleting rds %d", rds_id);
	tagsistant_query("delete from rds where id = %d", qtree->dbi, NULL, NULL, rds_id);
}
#endif

/**
 * RDS garbage collector
 *
 * @param qtree the querytree object used for DBI access
 */
void
tagsistant_rds_garbage_collector(tagsistant_querytree *qtree)
{
	(void) qtree;
#if 0
	int declared_rds = 0;
	tagsistant_query("select count(distinct id) from rds", qtree->dbi, tagsistant_return_integer, &declared_rds);

	while (declared_rds > TAGSISTANT_GC_RDS) {
		tagsistant_rds_delete_oldest(qtree);
		declared_rds--;
	}

	int stored_tuples = 0;
	tagsistant_query("select count(*) from rds", qtree->dbi, tagsistant_return_integer, &stored_tuples);

	if (stored_tuples > TAGSISTANT_GC_TUPLES) {
		tagsistant_rds_delete_oldest(qtree);
	}
#endif
}

/**
 * Returns TRUE if the ->name field of the tagsistant_rds_entry
 * passed as value equals the check name passed as user_data
 *
 * @param key unused
 * @param value a gpointer to a tagsistant_rds_entry struct
 * @param user_data a gpointer to a string with the name to match
 */
gboolean
tagsistant_rds_contains_object(gpointer key, gpointer value, gpointer user_data)
{
	(void) key;

	tagsistant_rds_entry *e = (tagsistant_rds_entry *) value;
	gchar *match_name = (gchar *) user_data;

	if (e is NULL || e->name is NULL) return (FALSE);
	return (strcmp(e->name, match_name) is 0 ? TRUE : FALSE);
}

#if 0
/*
 * Lookup the id of the RDS of a query.
 *
 * @param query the query that generates the RDS
 * @param materialized set to one if the RDS exists
 * @return the RDS id
 */
gchar *tagsistant_get_rds_id(tagsistant_querytree *qtree, int *materialized)
{
	/*
	 * compute the checksum
	 */
	gchar *checksum = tagsistant_get_rds_checksum(qtree);

	/*
	 * check if the RDS has been materialized
	 */
	tagsistant_query(
		"select 1 from rds where id = \"%s\" and reasoned = %d limit 1",
		qtree->dbi, tagsistant_return_integer, materialized, checksum, qtree->do_reasoning);

	return (checksum);
}
#endif

/**
 * Add an RDS to the cache
 *
 * @param rds the RDS to be cached
 */
void tagsistant_rds_add_to_cache(tagsistant_rds *rds)
{
	g_rw_lock_writer_lock(&tagsistant_rds_cache_rwlock);
	g_hash_table_insert(tagsistant_rds_cache, g_strdup(rds->checksum), rds);
	g_rw_lock_writer_unlock(&tagsistant_rds_cache_rwlock);
}

/**
 * Lookup and RDS into the cache
 *
 * @param checksum the RDS checksum
 */
tagsistant_rds *tagsistant_rds_lookup_in_cache(gchar *checksum)
{
	g_rw_lock_reader_lock(&tagsistant_rds_cache_rwlock);
	tagsistant_rds *rds = g_hash_table_lookup(tagsistant_rds_cache, checksum);
	g_rw_lock_reader_unlock(&tagsistant_rds_cache_rwlock);

	return (rds);
}

void
tagsistant_rds_entries_clean(gchar *key, GList *inodes, gpointer unused)
{
	(void) unused;

	if (key) g_free(key);
	if (inodes) g_list_free(inodes);
}

/**
 * Destroy an RDS
 *
 * @param rds the RDS to be destroyed
 */
void tagsistant_rds_destroy(tagsistant_rds *rds)
{
	if (!rds) return;

	tagsistant_rds_write_lock(rds);
	dbg('f', LOG_INFO, "Destroying RDS %s", rds->checksum);

	g_free(rds->checksum);
	g_free(rds->path);

	if (rds->entries) {
		g_hash_table_foreach(rds->entries, (GHFunc) tagsistant_rds_entries_clean, NULL);
		g_hash_table_destroy(rds->entries);
	}

#if TAGSISTANT_RDS_NEEDS_TREE
	if (rds->tree) (void) 0; // TODO destroy the ->tree
#endif

	tagsistant_rds_write_unlock(rds);
	g_free(rds);
}

/**
 * Callback to tagsistant_rds_destroy() used when
 * declaring RDS cache
 *
 * @param _rds a gpointer to be casted as tagsistant_rds*
 */
void tagsistant_rds_destroy_func(gpointer _rds)
{
	tagsistant_rds_destroy((tagsistant_rds *) _rds);
}

/**
 * Initialize the RDS module
 */
void tagsistant_rds_init()
{
	tagsistant_rds_cache = g_hash_table_new_full(
		g_str_hash, /* hash keys as strings */
		g_str_equal, /* compare keys as strings */
		(GDestroyNotify) g_free, /* how to free keys */
		(GDestroyNotify) tagsistant_rds_destroy_func /* how to free values */
	);
}

/**
 * build a linked list of filenames that satisfy the querytree
 * object. This is translated in a two phase flow:
 *
 * 1. each qtree_and_node list is translated into one
 * (temporary) table
 *
 * 2. the content of all tables are read in with a UNION
 * chain inside a super-select to apply the ORDER BY clause.
 *
 * 3. all the (temporary) tables are removed
 *
 * tagsistant_readdir_on_store
 *   tagsistant_rds_new
 *      garbage collection
 *      if (all) materialize the rds with tagsistant_query
 *      get the RDS ID
 *      lookup the RDS as GHashTable from cache, using the RDS ID
 *          if found, return it
 *      if (!materialized) materialize the rds with tagsistant_query
 *          create a GHashTable with RDS contents
 *          cache it
 *          return it
 *
 * @param query the qtree_or_node query structure to be resolved
 * @param conn a libDBI dbi_conn handle
 * @return a pointer to a GHashTable of tagsistant_file_handle objects
 */
tagsistant_rds *
tagsistant_rds_new(tagsistant_querytree *qtree)
{
	/*
	 * Calls the garbage collector
	 */
	tagsistant_rds_garbage_collector(qtree);

	/*
	 * a NULL query can't be processed
	 */
	if (!qtree) {
		dbg('f', LOG_ERR, "NULL tagsistant_querytree object provided to %s", __func__);
		return (NULL);
	}

#if TAGSISTANT_RDS_NEEDS_TREE
	if (!qtree->tree) {
		dbg('f', LOG_ERR, "NULL qtree_or_node object provided to %s", __func__);
		return (NULL);
	}
#endif

	int is_all_path = is_all_path(qtree->full_path);

	/*
	 * Allocate the RDS structure
	 */
	tagsistant_rds *rds = g_new0(tagsistant_rds, 1);
	if (!rds) {
		dbg('f', LOG_ERR, "Error allocating memory for RDS: %s", strerror(errno));
		return (NULL);
	}

	rds->checksum = tagsistant_get_rds_checksum(qtree);
	rds->path = g_strdup(qtree->expanded_full_path);
#if TAGSISTANT_RDS_NEEDS_TREE
	rds->tree = tagsistant_duplicate_tree(qtree->tree);
#endif
	rds->is_all_path = is_all_path;
	rds->entries = NULL;

	/*
	 * cache the new RDS
	 */
	tagsistant_rds_add_to_cache(rds);

	return (rds);
}

/**
 * Read lock an RDS. If the RDS has not been yet materialized,
 * materialize it too.
 *
 * @param rds The RDS to be locked
 * @param dbi a dbi_conn connection
 */
gboolean tagsistant_rds_read_lock(tagsistant_rds *rds, tagsistant_querytree *qtree)
{
	if (!rds) return (FALSE);
	g_rw_lock_reader_lock(&rds->rwlock);

	g_mutex_lock(&rds->materializer_mutex);
	if (!rds->entries) tagsistant_rds_materialize(rds, qtree);
	g_mutex_unlock(&rds->materializer_mutex);

	return (TRUE);
}

void tagsistant_rds_read_unlock(tagsistant_rds *rds)
{
	if (!rds) return;
	g_rw_lock_reader_unlock(&rds->rwlock);
}

gboolean tagsistant_rds_write_lock(tagsistant_rds *rds)
{
	if (!rds) return (FALSE);
	g_rw_lock_writer_lock(&rds->rwlock);
	return (TRUE);
}

void tagsistant_rds_write_unlock(tagsistant_rds *rds)
{
	if (!rds) return;
	g_rw_lock_writer_unlock(&rds->rwlock);
}

/**
 * Lookup an RDS in the RDS cache
 *
 * @param checksum the RDS checksum to lookup
 * @return the RDS, if found, NULL otherwise
 */
tagsistant_rds *
tagsistant_rds_lookup(const gchar *checksum)
{
	if (checksum is NULL) return (NULL);
	return (g_hash_table_lookup(tagsistant_rds_cache, checksum));
}

/**
 * Lookup or create an RDS based on a tagsistant_querytree object
 *
 * @param qtree the tagsistant_querytree source object
 * @param writeable if the RDS must be modified, writable must be
 *        TRUE, so the RDS will be returned write locked. Otherwise
 *        set it to FALSE: the RDS will be read locked.
 */
tagsistant_rds *
tagsistant_rds_new_or_lookup(tagsistant_querytree *qtree)
{
	tagsistant_rds *rds = NULL;

	gchar *checksum = tagsistant_get_rds_checksum(qtree);
	if (checksum) {
		rds = tagsistant_rds_lookup(checksum);
		g_free(checksum);
	}

	if (rds is NULL) rds = tagsistant_rds_new(qtree);

	return (rds);
}

#if !TAGSISTANT_RDS_HARD_CLEAN
void tagsistant_delete_rds_by_source(qtree_and_node *node, dbi_conn dbi)
{
	gchar *tag = (node->tag && strlen(node->tag)) ? node->tag : node->namespace;

	tagsistant_query(
		"select rds_id from rds where tagset like \"%%|%s|%%\"",
		dbi, tagsistant_rds_uncache, NULL, tag);

	tagsistant_query(
		"delete from rds where tagset like \"%%|%s|%%\"",
		dbi, NULL, NULL, tag);
}
#endif

/**
 * Destroy an RDS entry hash table
 *
 * @param key unused
 * @param rds the RDS to dematerialize
 * @param unused an unused field
 */
void
tagsistant_rds_dematerialize(gpointer key, tagsistant_rds *rds, gpointer unused)
{
	(void) key;
	(void) unused;
	if (!rds) return;

	/*
	 * destroy the hash table and set its field to NULL
	 * to signal that the RDS has been de-materialized
	 */
	tagsistant_rds_write_lock(rds);
	if (rds->entries) {
		g_hash_table_foreach(rds->entries, (GHFunc) tagsistant_rds_entries_clean, NULL);
		g_hash_table_destroy(rds->entries);
		rds->entries = NULL;
	}
	tagsistant_rds_write_unlock(rds);
}

/**
 * Deletes every RDS involved with one query
 *
 * TODO: current implementation just deletes EVERY RDS.
 * Code a less coarse alternative, then set
 * TAGSISTANT_RDS_HARD_CLEAN to FALSE
 *
 * @param query the query driving the deletion
 */
void tagsistant_delete_rds_involved(tagsistant_querytree *qtree)
{
#if TAGSISTANT_RDS_HARD_CLEAN

	(void) qtree;
	g_rw_lock_writer_lock(&tagsistant_rds_cache_rwlock);
	// g_hash_table_remove_all(tagsistant_rds_cache);
	g_hash_table_foreach(tagsistant_rds_cache, (GHFunc) tagsistant_rds_dematerialize, NULL);
	g_rw_lock_writer_unlock(&tagsistant_rds_cache_rwlock);

#else

	/*
	 * For every subquery, save the source tags
	 */
	qtree_or_node *query = qtree->tree;
	while (query) {
		/*
		 * Delete every tag...
		 */
		qtree_and_node *and = query->and_set;

		while (and) {
			tagsistant_delete_rds_by_source(and, qtree->dbi);

			/*
			 * ... and every related tag ...
			 */
			qtree_and_node *related = and->related;
			while (related) {
				tagsistant_delete_rds_by_source(related, qtree->dbi);
				related = related->related;
			}

			/*
			 * ... and even negated tags.
			 */
			qtree_and_node *negated = and->negated;
			while (negated) {
				tagsistant_delete_rds_by_source(negated, qtree->dbi);
				negated = negated->negated;
			}

			and = and->next;
		}
		query = query->next;
	}
#endif
}
