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
/*** FileTree translation                                                         ***/
/***                                                                              ***/
/************************************************************************************/

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

		if (fh_tmp && (fh_tmp->inode == inode)) {
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
gboolean tagsistant_materialize_rds(tagsistant_rds *rds, dbi_conn dbi)
{
	/*
	 * If the RDS is an ALL path, just load all the objects
	 */
	if (rds->is_all_path) {
		tagsistant_query(
			"select objectname, inode from objects",
			dbi, tagsistant_add_to_fileset, rds->entries);

		return (TRUE);
	}

	/*
	 * PHASE 1.
	 * Build a set of temporary tables containing all the matched objects
	 *
	 * Step 1.1. for each qtree_or_node build a temporary table
	 */
	qtree_or_node *query = rds->tree;

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
		tagsistant_query(create_base_table->str, dbi, NULL, NULL);
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
			tagsistant_query(cross_tag->str, dbi, NULL, NULL);
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
				tagsistant_query(cross_tag->str, dbi, NULL, NULL);
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

	query = rds->tree;

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
	tagsistant_query(view_statement->str, dbi, tagsistant_add_to_fileset, rds->entries);
	g_string_free(view_statement, TRUE);

	/*
	 * PHASE 3.
	 *
	 * drop the temporary tables
	 */
	query = rds->tree;
	while (query) {
		tagsistant_query("drop table tv%.16" PRIxPTR, dbi, NULL, NULL, (uintptr_t) query);
		query = query->next;
	}

	return (TRUE);
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

	return (strcmp(e->name, match_name) is 0 ? TRUE : FALSE);
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

	if (!qtree->tree) {
		dbg('f', LOG_ERR, "NULL qtree_or_node object provided to %s", __func__);
		return(NULL);
	}

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
	rds->tree = tagsistant_duplicate_tree(qtree->tree);
	rds->is_all_path = is_all_path;

	/*
	 * Materialize the RDS
	 */
	if (!tagsistant_materialize_rds(rds, qtree->dbi)) {
		dbg('f', LOG_ERR, "Error materializing RDS");
		// TODO: destroy the RDS...
		return (NULL);
	}

	/*
	 * TODO: cache the new RDS
	 */

	return (rds);
}

/**
 * Lookup an RDS in the RDS registry
 *
 * @param checksum the RDS checksum to lookup
 * @return the RDS, if found, NULL otherwise
 */
tagsistant_rds *
tagsistant_rds_lookup(const gchar *checksum)
{
	if (checksum is NULL) return (NULL);
	return (g_hash_table_lookup(tagsistant_rds_registry, checksum));
}

tagsistant_rds *
tagsistant_rds_new_or_lookup(tagsistant_querytree *qtree)
{
	tagsistant_rds *rds = tagsistant_rds_lookup(tagsistant_get_rds_checksum(qtree));
	if (rds is NULL) rds = tagsistant_rds_new(qtree);

	return (rds);
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

void tagsistant_delete_rds_by_source(qtree_and_node *node, dbi_conn dbi)
{
	gchar *tag = (node->tag && strlen(node->tag)) ? node->tag : node->namespace;

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
