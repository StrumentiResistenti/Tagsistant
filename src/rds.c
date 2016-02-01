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
/*** RDS can be stored in memory or in a temporary table in SQL. This behaviour   ***/
/*** is determined by setting the TAGSISTANT_RDS_IN_MEMORY macro.                 ***/
/***                                                                              ***/
/************************************************************************************/

#ifndef TAGSISTANT_RDS_IN_MEMORY
#define TAGSISTANT_RDS_IN_MEMORY 1
#endif

/**
 * add a file to the file tree (callback function)
 *
 * @param hash_table_pointer a GHashTable to hold results
 * @param result a DBI result
 */
static int tagsistant_add_to_fileset(void *hash_table_pointer, dbi_result result)
{
	/* Cast the hash table */
	GHashTable *hash_table = (GHashTable *) hash_table_pointer;

	/* fetch query results */
	gchar *name = dbi_result_get_string_copy_idx(result, 1);
	if (!name) return (0);

	tagsistant_inode inode = dbi_result_get_uint_idx(result, 2);

	/* lookup the GList object */
	GList *list = g_hash_table_lookup(hash_table, name);

	/* look for duplicates due to reasoning results */
	GList *list_tmp = list;
	while (list_tmp) {
		tagsistant_file_handle *fh_tmp = (tagsistant_file_handle *) list_tmp->data;

		if (fh_tmp && (fh_tmp->inode is inode)) {
			g_free_null(name);
			return (0);
		}

		list_tmp = list_tmp->next;
	}

	/* fetch query results into tagsistant_file_handle struct */
	tagsistant_file_handle *fh = g_new0(tagsistant_file_handle, 1);
	if (!fh) {
		g_free_null(name);
		return (0);
	}

	g_strlcpy(fh->name, name, 1024);
	fh->inode = inode;
	g_free_null(name);

	/* add the new element */
	// TODO valgrind says: check for leaks
	g_hash_table_insert(hash_table, g_strdup(fh->name), g_list_prepend(list, fh));

//	dbg('f', LOG_INFO, "adding (%d,%s) to filetree", fh->inode, fh->name);

	return (0);
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

/*
 * Lookup the id of the RDS of a query.
 *
 * @param query the query that generates the RDS
 * @param materialized set to one if the RDS exists
 * @return the RDS id
 */
#if !TAGSISTANT_RDS_IN_MEMORY
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

/*
 * Materialize the RDS of a query
 *
 * @param qtree the querytree object
 * @return the RDS checksum id
 */
gchar *tagsistant_materialize_rds(tagsistant_querytree *qtree)
{
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
	 * PHASE 2.
	 *
	 * Create a new RDS
	 */
	gchar *checksum = tagsistant_get_rds_checksum(qtree);
	gchar *sources = tagsistant_compute_rds_sources(qtree);

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
					"select \"%s\", %d, inode, objectname, \"%s\", datetime(\"now\") from tv%.16" PRIxPTR,
					checksum, qtree->do_reasoning, sources, (uintptr_t) query);
				break;
			case TAGSISTANT_DBI_MYSQL_BACKEND:
				g_string_append_printf(view_statement,
					"select \"%s\", %d, inode, objectname, \"%s\", now() from tv%.16" PRIxPTR,
					checksum, qtree->do_reasoning, sources, (uintptr_t) query);
				break;
			default:
				g_string_append_printf(view_statement,
					"select \"%s\", %d, inode, objectname, \"%s\", \"\" from tv%.16" PRIxPTR,
					checksum, qtree->do_reasoning, sources, (uintptr_t) query);
				break;
		}

		if (query->next) g_string_append(view_statement, " union ");
		query = query->next;
	}

	/*
	 * Load all the files in the RDS
	 */
	tagsistant_query(
		"insert into rds %s",
		qtree->dbi, NULL, NULL, view_statement->str);

	/*
	 * free the SQL statement and any other intermediate string
	 */
	g_string_free(view_statement, TRUE);
	g_free(sources);

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

	return (checksum);
}

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

/**
 * RDS garbage collector
 *
 * @param qtree the querytree object used for DBI access
 */
void
tagsistant_rds_garbage_collector(tagsistant_querytree *qtree)
{
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
}

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

GHashTable *tagsistant_rds_cache = NULL;

void tagsistant_rds_add_to_cache(gchar *rds_id, GHashTable *file_hash)
{
	g_hash_table_insert(tagsistant_rds_cache, g_strdup(rds_id), file_hash);
}

GHashTable *tagsistant_rds_lookup_in_cache(gchar *rds_id)
{
	return (g_hash_table_lookup(tagsistant_rds_cache, rds_id));
}

void tagsistant_rds_destroy_func(gpointer *list)
{
	if (list) g_list_free_full((GList *) list, (GDestroyNotify) g_free);
}

void tagsistant_rds_init()
{
	tagsistant_rds_cache = g_hash_table_new_full(g_str_hash, g_str_equal,
		(GDestroyNotify) g_free, (GDestroyNotify) tagsistant_rds_destroy_func);
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
 * @param is_all_path is true when the path includes the ALL/ tag
 * @return a pointer to a GHashTable of tagsistant_file_handle objects
 */
GHashTable *tagsistant_rds_new(tagsistant_querytree *qtree, int is_all_path)
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
		return(NULL);
	}

	qtree_or_node *query = qtree->tree;

	if (!query) {
		dbg('f', LOG_ERR, "NULL qtree_or_node object provided to %s", __func__);
		return(NULL);
	}

	/*
	 * If the query contains the ALL meta-tag, just select all the available
	 * objects and return them
	 */
	if (is_all_path) {
		GHashTable *file_hash = g_hash_table_new(g_str_hash, g_str_equal);

		tagsistant_query(
			"select objectname, inode from objects",
			qtree->dbi, tagsistant_add_to_fileset, file_hash);

		return(file_hash);
	}

	/*
	 * Get the RDS id
	 */
	int materialized = 0;
	gchar *rds_id = tagsistant_get_rds_id(qtree, &materialized);

	GHashTable *file_hash = NULL;

#if TAGSISTANT_RDS_IN_MEMORY
	/*
	 * Check if the RDS is cached in memory
	 */
	file_hash = tagsistant_rds_lookup_in_cache(rds_id);
#endif

	/*
	 * If the RDS has not been materialized yet, materialize it
	 */
	if (!materialized) {
		rds_id = tagsistant_materialize_rds(qtree);
	}

	/*
	 * Then load the RDS in a GHashTable
	 */
	file_hash = g_hash_table_new(g_str_hash, g_str_equal);
	tagsistant_query(
		"select objectname, inode from rds where id = \"%s\" and reasoned = %d",
		qtree->dbi, tagsistant_add_to_fileset, file_hash, rds_id, qtree->do_reasoning);

#if TAGSISTANT_RDS_IN_MEMORY
	/*
	 * Cache the RDS in memory
	 */
	tagsistant_rds_add_to_cache(rds_id, file_hash);
#endif

	return (file_hash);
}

/**
 * Destroy a filetree element GList list of tagsistant_file_handle.
 * This will free the GList data structure by first calling
 * tagsistant_filetree_destroy_value() on each linked node.
 *
 * @param key the entry of the GHashTable element to be cleared
 * @param list the value of the GHashTable element to be cleared which is a GList object
 * @param data unused
 */
void tagsistant_rds_destroy_value_list(gchar *key, GList *list, gpointer data)
{
	(void) data;

	g_free_null(key);

	if (list) g_list_free_full(list, (GDestroyNotify) g_free /* was tagsistant_filetree_destroy_value */);
}

int tagsistant_rds_uncache(gpointer p, dbi_result result)
{
	(void) p;

	const gchar *rds_id = dbi_result_get_string_idx(result, 1);
	dbg('s', LOG_INFO, "Uncaching RDS %s", rds_id);

	g_hash_table_remove(tagsistant_rds_cache, rds_id);

	return (0);
}

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

/**
 * Deletes every RDS involved with one query
 *
 * @param query the query driving the deletion
 */
void tagsistant_delete_rds_involved(tagsistant_querytree *qtree)
{
#if 0
	tagsistant_query("delete from rds", qtree->dbi, NULL, NULL);
	tagsistant_query("delete from rds_index", qtree->dbi, NULL, NULL);
	tagsistant_query("delete from rds_tags", qtree->dbi, NULL, NULL);

	return;
#endif

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
}
