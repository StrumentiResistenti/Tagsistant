/*
   Tagsistant (tagfs) -- path_resolution.c
   Copyright (C) 2006-2013 Tx0 <tx0@strumentiresistenti.org>

   Transform paths into queries and apply queries to file sets to
   grep files matching with queries.

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

gchar *tagsistant_querytree_types[QTYPE_TOTAL];

#if TAGSISTANT_ENABLE_AND_SET_CACHE
/**
 * Cache inode resolution from DB
 */
GRWLock tagsistant_and_set_cache_lock;
GHashTable *tagsistant_and_set_cache = NULL;
#endif

GRegex *tagsistant_inode_extract_from_path_regex_1 = NULL;
GRegex *tagsistant_inode_extract_from_path_regex_2 = NULL;

/**
 * Just a wrapper around tagsistant_querytree_destroy() called when
 * an element is removed from tagsistant_querytree_cache.
 *
 * @param qtree the querytree object to be removed
 */
void tagsistant_querytree_cache_destroy_element(tagsistant_querytree *qtree)
{
	tagsistant_querytree_destroy(qtree, 0);
}

/**
 * Given a linked list of qtree_and_node objects (called an and-set)
 * return a string with a comma separated list of all the tags.
 *
 * @param and_set the linked and-set list
 * @return a string like "tag1, tag2, tag3"
 */
gchar *tagsistant_compile_and_set(gchar *objectname, qtree_and_node *and_set)
{
	// TODO valgrind says: check for leaks
	GString *str = g_string_sized_new(10240);
	g_string_append_printf(str, "%s>>>", objectname);

	/* compile the string */
	qtree_and_node *and_pointer = and_set;
	while (and_pointer) {
		if (and_pointer->tag) {
			g_string_append(str, and_pointer->tag);

			/* look for related tags too */
			qtree_and_node *related = and_pointer->related;
			while (related) {
				g_string_append_printf(str, ",%s", related->tag);
				related = related->related;
			}
		} else if (and_pointer->namespace && and_pointer->key && and_pointer->value) {
			g_string_append_printf(str, "%s%s=%s", and_pointer->namespace, and_pointer->key, and_pointer->value);
		}
		and_pointer = and_pointer->next;
	}

	/* destroy the GString but not its content, which is returned */
	return (g_string_free(str, FALSE));
}

#if TAGSISTANT_ENABLE_AND_SET_CACHE
/**
 * Invalidate a tagsistant_and_set_cache entry
 *
 * @param objectname the key of the object to be invalidated
 */
void tagsistant_invalidate_and_set_cache_entries(tagsistant_querytree *qtree)
{
	(void) qtree;


	qtree_or_node *ptr = qtree->tree;
	while (ptr) {
		// compute the key
		gchar *search_key = tagsistant_compile_and_set(qtree->object_path, ptr->and_set);

		// if lookup succeed, removes the entry
		g_rw_lock_writer_lock(&tagsistant_and_set_cache_lock);
		if (g_hash_table_remove(tagsistant_and_set_cache, search_key)) {
			dbg('F', LOG_INFO, "Cache entry %s invalidated", search_key);
		} else {
			dbg('F', LOG_INFO, "Cache entry %s not found!", search_key);
		}
		g_rw_lock_writer_unlock(&tagsistant_and_set_cache_lock);

		// free memory
		g_free(search_key);

		// next or node
		ptr = ptr->next;
	}
}
#endif

/**
 * check if a tagging is valid
 *
 * @param and the qtree_and_node holding the tag
 * @param dbi a dbi_conn connection
 * @param objectname the name of the object to be checked for tagging by the qtree_and_node
 * @return the inode of the object if the tagging is valid, 0 otherwise
 */
int tagsistant_check_single_tagging(qtree_and_node *and, dbi_conn dbi, gchar *objectname)
{
	tagsistant_inode inode = 0;

	/*
	 * if the tag is a triple tag and its operator is not equality,
	 * this function should return 1
	 */
	if (and->namespace && strlen(and->namespace) && and->operator isNot TAGSISTANT_EQUAL_TO) return (1);

	/*
	 * otherwise the tagging has to be checked
	 */
	tagsistant_query(
		"select objects.inode from objects "
			"join tagging on objects.inode = tagging.inode "
			"where objects.objectname = \"%s\" and tagging.tag_id = %d",
		dbi,
		tagsistant_return_integer,
		&inode,
		objectname,
		and->tag_id);

	return (inode);
}

/**
 * return the tagsistant inode contained into a path
 *
 * @param qtree the querytree object supposed to contain an inode
 * @return the inode, if found
 */
tagsistant_inode tagsistant_inode_extract_from_path(const gchar *path)
{
	if (!path && strlen(path) is 0) return (0);

	tagsistant_inode inode = 0;

	GMatchInfo *match_info;
	if (g_regex_match(tagsistant_inode_extract_from_path_regex_1, path, 0, &match_info)) {
		/* extract the inode */
		gchar *inode_text = g_match_info_fetch(match_info, 1);
		inode = strtoul(inode_text, NULL, 10);
		g_free_null(inode_text);
	} else if (g_regex_match(tagsistant_inode_extract_from_path_regex_2, path, 0, &match_info)) {
		/* extract the inode */
		gchar *inode_text = g_match_info_fetch(match_info, 1);
		inode = strtoul(inode_text, NULL, 10);
		g_free_null(inode_text);
	}

	g_match_info_free(match_info);

	if (inode) {
		dbg('l', LOG_INFO, "%s has inode %lu", path, (long unsigned int) inode);
	} else {
		dbg('l', LOG_INFO, "%s does not contain an inode", path);
	}

	return (inode);
}

/**
 * Try to guess the inode of an object by comparing DB contents
 * with and and-set of tags
 *
 * @param and_set a pointer to a qtree_and_node and-set data structure
 * @param dbi a libDBI dbi_conn reference
 * @param objectname the name of the object we are looking up the inode
 * @return the inode of the object if found, zero otherwise
 */
tagsistant_inode
tagsistant_guess_inode_from_and_set(qtree_and_node *and_set, dbi_conn dbi, gchar *objectname)
{
	tagsistant_inode inode = 0, guessed_inode = 0;

	/*
	 * if called without an and_set (which can happen on syntactically
	 * wrong queries) just return
	 */
	if (!and_set) return (0);

#if TAGSISTANT_ENABLE_AND_SET_CACHE
	/* check if the query has been already answered and cached */
	gchar *search_key = tagsistant_compile_and_set(objectname, and_set);

	// if lookup succeed, returns the inode
	g_rw_lock_reader_lock(&tagsistant_and_set_cache_lock);
	tagsistant_inode *value = (tagsistant_inode *) g_hash_table_lookup(tagsistant_and_set_cache, search_key);
	g_rw_lock_reader_unlock(&tagsistant_and_set_cache_lock);

	if (value) {
		g_free_null(search_key);
		return (GPOINTER_TO_UINT(value));
	}
#endif

	/*
	 * the first step is to check every tag in the and_set linked by
	 * the ->next field. if the object is tagged by each tag, the first
	 * step is fulfilled
	 */
	qtree_and_node *and_set_ptr = and_set;

	while (and_set_ptr) {
		/*
		 * handle the ALL/ special case by guessing the inode of the first
		 * object named objectname
		 */
		if (g_strcmp0(and_set_ptr->tag, "ALL") is 0) {
			// get the inode from the object path
			inode = tagsistant_inode_extract_from_path(objectname);

			// load the inode from the object table
			if (!inode) {
				tagsistant_query(
					"select inode from objects where objectname = \"%s\"",
					dbi, tagsistant_return_integer, &inode, objectname);
			}

			goto BREAK_LOOKUP;
		}

		/*
		 * the query is not an ALL/ query, so we process each tag.
		 * if no match is directly found, we check the ->related tags too.
		 */
		tagsistant_inode single_and_inode = tagsistant_check_single_tagging(and_set_ptr, dbi, objectname);

		/*
		 * if no match has been found, try with related tags
		 */
		if (!single_and_inode) {
			qtree_and_node *related = and_set_ptr->related;

			while (related) {
				single_and_inode = tagsistant_check_single_tagging(related, dbi, objectname);

				/* if a related tag match, break the while loop */
				if (single_and_inode) break;

				related = related->related;
			}

			/* it this tag (nor any of its related) matches, break the lookup */
			if (!single_and_inode && !and_set_ptr->negate) goto BREAK_LOOKUP;
		}

		/*
		 * if this is the first iteration and guessed_inode is still
		 * empty, save the inode for the further checking
		 */
		if (!guessed_inode) guessed_inode = single_and_inode;

		/*
		 * if the inode extracted from this iteration is different from the previous one
		 * this may suggest a case of file homonymy, so we must return a match failure
		 */
		if (guessed_inode isNot single_and_inode) goto BREAK_LOOKUP;

		/*
		 * otherwise we continue with the next tag
		 */
		and_set_ptr = and_set_ptr->next;
	}

	/*
	 * save the inode for returning it in the end of the function
	 */
	inode = guessed_inode;

	/*
	 * the second step involves negated tags
	 */
	and_set_ptr = and_set->negated;
	while (and_set_ptr) {
		tagsistant_inode single_and_inode = tagsistant_check_single_tagging(and_set_ptr, dbi, objectname);

		/*
		 * if no match has been found, try with related tags
		 */
		if (!single_and_inode) {
			qtree_and_node *related = and_set_ptr->related;

			while (related) {
				single_and_inode = tagsistant_check_single_tagging(related, dbi, objectname);

				/* if a related tag match, break the while loop */
				if (single_and_inode) break;

				related = related->related;
			}
		}

		/*
		 * if we have a match, the file must be discarder (returning 0)
		 * because this is the ->negated branch of the tree
		 */
		if (single_and_inode) {
			inode = 0;
			goto BREAK_LOOKUP;
		}

		/*
		 * otherwise we continue with the next tag
		 */
		and_set_ptr = and_set_ptr->next;
	}

BREAK_LOOKUP:

#if TAGSISTANT_ENABLE_AND_SET_CACHE
	/* cache a result if one has been found */
	if (inode) {
		g_rw_lock_writer_lock(&tagsistant_and_set_cache_lock);
		g_hash_table_insert(tagsistant_and_set_cache, search_key, GUINT_TO_POINTER(inode));
		g_rw_lock_writer_unlock(&tagsistant_and_set_cache_lock);
	} else {
		g_free_null(search_key);
	}
#endif

	return (inode);
}

/**
 * Check if the object_path of the qtree is contained in at least one
 * of the and_sets referenced by the query
 *
 * @param qtree the tagsistant_querytree object to check
 * @return true if contained, false otherwise
 */
int tagsistant_querytree_check_tagging_consistency(tagsistant_querytree *qtree)
{
	qtree->exists = 0;

	/*
	 * 0. no object path means no need to check for inner consistency
	 */
	if (!qtree->object_path)
		return (0);

	if (strlen(qtree->object_path) is 0) {
		qtree->exists = 1;
		return (1);
	}

	/*
	 * 1. get the object path first element
	 */
	gchar *object_path_first_token = g_strdup(qtree->object_path);

	gchar *first_slash = g_strstr_len(object_path_first_token,
		strlen(object_path_first_token), G_DIR_SEPARATOR_S);

	if (first_slash) {
		*first_slash = '\0';
	} else {
		qtree->is_taggable = 1;
	}

	/*
	 * 2. use the object first token to guess if its tagged in the RDS
	 */
	tagsistant_rds *rds = tagsistant_rds_new_or_lookup(qtree);

	if (rds) {
		tagsistant_rds_read_lock(rds, qtree);

		/*
		 * lookup all the inodes that match the object name
		 */
		GList *inodes = g_hash_table_lookup(rds->entries, object_path_first_token);

		/*
		 * if a GList is returned, set the querytree object as existing
		 * and try to assign a proper inode
		 */
		if (inodes) {
			qtree->exists = 1;

			GList *ptr = inodes;
			GList *match = NULL;
			while (ptr) {
				if (ptr->data is qtree->inode) {
					match = ptr;
					break;
				} else {
					dbg('f', LOG_INFO, "%d is not %d", ptr->data, qtree->inode);
				}
				ptr = ptr->next;
			}

			if (match is NULL) {
				if (qtree->inode is 0) {
					/*
					 * set the inode to the first available one
					 */
					tagsistant_querytree_set_inode(qtree, GPOINTER_TO_UINT(inodes->data));
				} else {
					/*
					 * no match for an object with a provided inode
					 * means that the object doesn't exist
					 */
					qtree->exists = 0;
				}
			}
		}

		tagsistant_rds_read_unlock(rds);
	}

	g_free_null(object_path_first_token);

	return(qtree->exists);
}

/*
 * parsing macros to be used in specialized parse functions
 */

/** get the value of the current token */
#define __TOKEN **token_ptr

/** get the value of the next token (could be NULL) */
#define __NEXT_TOKEN *(*token_ptr + 1)

/** move the pointer to the next token */
#define __SLIDE_TOKEN (*token_ptr)++;

/** add next token to a tag group by creating the and_node_t structure */
#define TAGSISTANT_TAG_GROUP_ADD_NEW_NODE 2

/** add next token to a tag group by appending it to the last and_node_t structure */
#define TAGSISTANT_TAG_GROUP_ADD_TO_NODE 1

/** next token is outside any tag group */
#define TAGSISTANT_TAG_GROUP_DONT_ADD 0

/** abort parsing a store query with an error message */
#define TAGSISTANT_ABORT_STORE_PARSING(message) { qtree->error_message = g_strdup(message); return (0); }

#if 0
typedef enum {
	BEGIN,
	AND_NODE,
	TAG_GROUP
} tagsistant_query_context;

/**
 * parse the query portion between store/ and @/
 *
 * @param qtree the querytree object
 * @param path the parsed path
 * @param token_ptr a pointer to the tokenized path (three stars because we need to move it across the array even in calling function)
 * @param do_reasoning boolean flag which activate the reasoner if true
 * @return 1 on success, 0 on failure or errors
 */
int tagsistant_querytree_parse_store_recursive (
	tagsistant_querytree *qtree,
	const char *path,
	gchar ***token_ptr,
	qtree_or_node *current_or,
	qtree_and_node *current_and,
	tagsistant_query_context context)
{
	return (1);
}
#endif

/**
 * parse the query portion between store/ and @/
 *
 * @param qtree the querytree object
 * @param path the parsed path
 * @param token_ptr a pointer to the tokenized path (three stars because we need to move it across the array even in calling function)
 * @param do_reasoning boolean flag which activate the reasoner if true
 * @return 1 on success, 0 on failure or errors
 */
int tagsistant_querytree_parse_store (
	tagsistant_querytree *qtree,
	const char *path,
	gchar ***token_ptr,
	int disable_reasoner)
{
	unsigned int orcount = 0, andcount = 0;

	/*
	 * This variable holds the state of a tag group and starts as TAGSISTANT_TAG_GROUP_DONT_ADD.
	 *
	 * When a tag group is started by {, this variable is set to TAGSISTANT_TAG_GROUP_ADD_NEW_NODE.
	 *
	 * The first token will be saved in a new qtree_and_node structure and the tag_group variable
	 * will be set to TAGSISTANT_TAG_GROUP_ADD_TO_NODE.
	 *
	 * When the tag group is closed by }, the tag_group variable is set back to TAGSISTANT_TAG_GROUP_DONT_ADD.
	 *
	 * Next token will be saved on its own qtree_and_node structure.
	 */
	unsigned int tag_group = TAGSISTANT_TAG_GROUP_DONT_ADD;

	/*
	 * guess if the query is complete or not
	 */
	size_t path_length = strlen(path);
	if (g_strstr_len(path, path_length, "/" TAGSISTANT_QUERY_DELIMITER)) {
		qtree->complete = 1;
		qtree->do_reasoning = g_strstr_len(path, path_length, "/" TAGSISTANT_QUERY_DELIMITER_NO_REASONING) ? 0 : 1;
		qtree->do_reasoning = disable_reasoner ? 0 : qtree->do_reasoning;
	}
	dbg('q', LOG_INFO, "Path %s is %scomplete", path, qtree->complete ? "" : "not ");

	/*
	 * by default a query is valid until something wrong happens while parsing it
	 */
	qtree->valid = 1;

	/*
	 * initialize iterator variables on query tree nodes
	 */
	qtree_and_node *last_and = NULL;
	qtree_or_node  *last_or  = qtree->tree = g_new0(qtree_or_node, 1);
	if (qtree->tree is NULL) {
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		dbg('q', LOG_ERR, "Error allocating memory");
		return (0);
	}

	/*
	 * begin parsing
	 */
	while (__TOKEN && (*__TOKEN isNot TAGSISTANT_QUERY_DELIMITER_CHAR)) {
		if (strlen(__TOKEN) is 0) {
			/* ignore zero length tokens */

		} else if (strcmp(__TOKEN, TAGSISTANT_NEGATE_NEXT_TAG) is 0) {
			/* double negations make no sense */
			if (qtree->negate_next_tag)
				TAGSISTANT_ABORT_STORE_PARSING(TAGSISTANT_ERROR_DOUBLE_NEGATION);

			/* negation can't be used inside a tag group */
			if (tag_group isNot TAGSISTANT_TAG_GROUP_DONT_ADD)
				TAGSISTANT_ABORT_STORE_PARSING(TAGSISTANT_ERROR_NEGATION_INSIDE_TAG_GROUP);

			qtree->negate_next_tag = 1;

		} else if (strcmp(__TOKEN, TAGSISTANT_ANDSET_DELIMITER) is 0) {
			/* open new entry in OR level */
			orcount++;
			andcount = 0;

			qtree_or_node *new_or = g_new0(qtree_or_node, 1);
			if (new_or is NULL) {
				dbg('q', LOG_ERR, "Error allocating memory");
				return (0);
			}

			last_or->next = new_or;
			last_or = new_or;
			last_and = NULL;

		} else if (strcmp(__TOKEN, TAGSISTANT_TAG_GROUP_BEGIN) is 0) {
			/* Can't nest tag groups */
			if (tag_group isNot TAGSISTANT_TAG_GROUP_DONT_ADD)
				TAGSISTANT_ABORT_STORE_PARSING(TAGSISTANT_ERROR_NESTED_TAG_GROUP);

			tag_group = TAGSISTANT_TAG_GROUP_ADD_NEW_NODE;

		} else if (strcmp(__TOKEN, TAGSISTANT_TAG_GROUP_END) is 0) {
			/* can't close a tag group that has not been opened */
			if (TAGSISTANT_TAG_GROUP_DONT_ADD is tag_group)
				TAGSISTANT_ABORT_STORE_PARSING(TAGSISTANT_ERROR_CLOSE_TAG_GROUP_NOT_OPENED);

			tag_group = TAGSISTANT_TAG_GROUP_DONT_ADD;

		} else {
			/* save next token in new qtree_and_node_t slot */
			qtree_and_node *and = g_new0(qtree_and_node, 1);
			if (and is NULL) {
				dbg('q', LOG_ERR, "Error allocating memory");
				TAGSISTANT_ABORT_STORE_PARSING(TAGSISTANT_ERROR_MEMORY_ALLOCATION);
				return (0);
			}

			/* clean the environment */
			g_free_null(qtree->last_tag);
			g_free_null(qtree->namespace);
			g_free_null(qtree->key);
			g_free_null(qtree->value);
			qtree->operator = 0;

			/*
			 * check if the tag token ends with a colon (:)
			 * if so, this tag is a triple tag and requires special
			 * parsing ":$"
			 */
			if (g_regex_match_simple(tagsistant.triple_tag_regex, __TOKEN, 0, 0)) {
				and->namespace = g_strdup(__TOKEN);
				qtree->namespace = g_strdup(__TOKEN);

				if (__NEXT_TOKEN) {
					__SLIDE_TOKEN;
					and->key = g_strdup(__TOKEN);
					qtree->key = g_strdup(__TOKEN);

					if (__NEXT_TOKEN) {
						__SLIDE_TOKEN;
						if (strcmp(__TOKEN, TAGSISTANT_GREATER_THAN_OPERATOR) is 0) {
							qtree->operator = and->operator = TAGSISTANT_GREATER_THAN;
							qtree->force_inode_in_filenames = 1;

						} else if (strcmp(__TOKEN, TAGSISTANT_SMALLER_THAN_OPERATOR) is 0) {
							qtree->operator = and->operator = TAGSISTANT_SMALLER_THAN;
							// qtree->force_inode_in_filenames = 1;

						} else if (strcmp(__TOKEN, TAGSISTANT_EQUALS_TO_OPERATOR) is 0) {
							qtree->operator = and->operator = TAGSISTANT_EQUAL_TO;

						} else if (strcmp(__TOKEN, TAGSISTANT_CONTAINS_OPERATOR) is 0) {
							qtree->operator = and->operator = TAGSISTANT_CONTAINS;
							// qtree->force_inode_in_filenames = 1;
						}

						if (__NEXT_TOKEN) {
							__SLIDE_TOKEN;
							and->value = g_strdup(__TOKEN);
							qtree->value = g_strdup(__TOKEN);
						}
					}
				}
				and->tag_id = tagsistant_sql_get_tag_id(qtree->dbi, and->namespace, and->key, and->value);
			} else {
				qtree->last_tag = g_strdup(__TOKEN);
				and->tag = g_strdup(__TOKEN);
				and->tag_id = tagsistant_sql_get_tag_id(qtree->dbi, and->tag, NULL, NULL);
			}

			and->next = NULL;
			and->related = NULL;
			and->negated = NULL;

			/*
			 * Append this node to the tree:
			 *
			 * - If it's a negated tag append it to the ->negated field of the last negated node.
			 * - If it's the nth node of a tag group, append it to the ->related field of the last related node.
			 * - Otherwise append it to the ->next field of the last node.
			 */
			if (qtree->negate_next_tag) {
				qtree->negate_next_tag = 0;
				and->negate = 1;

				/* append this node to the last qtree_and_node as a negated node */
				qtree_and_node *last_negated = last_and;
				if (!last_and)
					TAGSISTANT_ABORT_STORE_PARSING(TAGSISTANT_ERROR_NEGATION_ON_FIRST_POSITION);

				while (last_negated->negated) {
					last_negated = last_negated->negated;
				}
				last_negated->negated = and;
			} else if (TAGSISTANT_TAG_GROUP_ADD_TO_NODE is tag_group) {
				/* append this node to the last related node of the last qtree_and_node */
				qtree_and_node *last_related = last_and;
				while (last_related->related) {
					last_related = last_related->related;
				}
				last_related->related = and;
			} else {
				/* append this node to last qtree_and_node */
				if (last_and is NULL) {
					last_or->and_set = and;
				} else {
					last_and->next = and;
				}
				last_and = and;
			}

			dbg('q', LOG_INFO, "Query tree nodes %.2d.%.2d %s", orcount, andcount, __TOKEN);
			andcount++;

			/*
			 * Next tokens will be added to current qtree_and_node structure
			 */
			if (tag_group is TAGSISTANT_TAG_GROUP_ADD_NEW_NODE) {
				tag_group = TAGSISTANT_TAG_GROUP_ADD_TO_NODE;
			}

			/* search related tags */
			if (qtree->do_reasoning && (and->tag || (and->namespace && and->key && and->value))) {
				dbg('q', LOG_INFO, "Searching for other tags related to %s", and->tag);

				tagsistant_reasoning *reasoning = g_malloc(sizeof(tagsistant_reasoning));
				if (reasoning isNot NULL) {
					reasoning->start_node = and;
					reasoning->current_node = and;
					reasoning->added_tags = 0;
					reasoning->conn = qtree->dbi;
					int newtags = tagsistant_reasoner(reasoning);
					dbg('q', LOG_INFO, "Reasoning added %d tags", newtags);
					g_free_null(reasoning);
				}
			}
		}

		// save last tag found
		g_free_null(qtree->last_tag);
		qtree->last_tag = g_strdup(__TOKEN);

		__SLIDE_TOKEN;
	}

	/*
	 * if last token is TAGSISTANT_QUERY_DELIMITER_CHAR,
	 * move the pointer one element forward
	 */
	if (__TOKEN && (*__TOKEN is TAGSISTANT_QUERY_DELIMITER_CHAR)) {
		if (!qtree->tree || !qtree->tree->and_set) {
			qtree->error_message = g_strdup(TAGSISTANT_ERROR_NULL_QUERY);
		}
		__SLIDE_TOKEN;
	}

	/*
	 * if the query is complete, the remaining tokens form the object path
	 */
	if (qtree->complete) {
		/*
		 *  get the object path name joining the remaining part of tokens
		 */
		qtree->object_path = g_strjoinv(G_DIR_SEPARATOR_S, *token_ptr);

		/*
		 *  look for an inode in object_path
		 */
		qtree->inode = tagsistant_inode_extract_from_path(qtree->object_path);

		/*
		 * if no inode is found, try to guess it resolving the querytree
		 * we just need to check if an object with *token_ptr objectname and
		 * a matching or_node->and_set->tag named tag is listed
		 */
		if (!qtree->inode) {
			qtree_or_node *or_tmp = qtree->tree;
			while (or_tmp && !qtree->inode && strlen(qtree->object_path)) {
				qtree->inode = tagsistant_guess_inode_from_and_set(or_tmp->and_set, qtree->dbi, **token_ptr);
				or_tmp = or_tmp->next;
			}
		} else {
			/*
			 * replace the inode and the separator with a blank string,
			 * actually stripping it from the object_path
			 */
			gchar *new_object_path = g_regex_replace(
				tagsistant_inode_extract_from_path_regex_1,
				qtree->object_path,
				strlen(qtree->object_path),
				0, "", 0, NULL);

			g_free(qtree->object_path);
			qtree->object_path = new_object_path;

			/*
			 * check if the inode found in the object_path refers to an object
			 * which is really tagged by the tagset described in the tags/.../=
			 * path. a query is valid if the object is tagged in at least one
			 * and set
			 */
			int valid_query = 0;
			qtree_or_node *or_iterator = qtree->tree;

			while (or_iterator) {
				/*
				 * check if the object is tagged in this andset
				 */
				int valid_and_set = 1;
				qtree_and_node *and_tmp = or_iterator->and_set;

				while (and_tmp) {
					tagsistant_inode tmp_inode;
					tagsistant_query(
						"select tagging.inode from tagging "
							"join tags on tagging.tag_id = tags.tag_id "
							"where tagging.inode = %d and tags.tagname = \"%s\"",
						qtree->dbi,
						tagsistant_return_integer,
						&tmp_inode,
						qtree->inode,
						and_tmp->tag);

					/*
					 * if just one tag is not valid, the whole andset is not valid
					 */
					if (tmp_inode isNot qtree->inode) {
						valid_and_set = 0;
						break;
					}

					and_tmp = and_tmp->next;
				}

				/*
				 * if at least one andset is valid, the whole query is valid
				 * and we don't need further checking
				 */
				if (valid_and_set) {
					valid_query = 1;
					break;
				}

				or_iterator = or_iterator->next;
			}

			/*
			 * if the whole query is not valid
			 * we should reset the inode to 0
			 */
			if (!valid_query)
				tagsistant_querytree_set_inode(qtree, 0);
		}

		/*
		 * if an inode has been found, form the archive_path and full_archive_path
		 */
		if (qtree->inode)
			tagsistant_querytree_set_inode(qtree, qtree->inode);

		if (strlen(qtree->object_path))
			qtree->points_to_object = qtree->valid = qtree->complete = 1;

		/*
		 * checking if the query is consistent or not has been
		 * moved where the check matters:
		 *
		 *   getattr(), link(), mkdir(), mknod(), open(),
		 *   release(), rename(), rmdir(), symlink(), unlink()
		 *
		 * Despite it may seems odd to place an explicit call on all
		 * that methods, the main reason to move this call outside of
		 * here is to avoid it in highly frequent calls, namely read()
		 * and write()! The performance improvement is huge, especially
		 * when reading and writing big files.
		 */
		// tagsistant_querytree_check_tagging_consistency(qtree);
	}

	return (1);
}

/**
 * parse the query portion after tags/
 *
 * @param qtree the querytree object
 * @param token_ptr a pointer to the tokenized path (three stars because we need to move it across the array even in calling function)
 * @return 1 on success, 0 on failure or errors
 */
int tagsistant_querytree_parse_tags (
	tagsistant_querytree* qtree,
	gchar ***token_ptr)
{
	if (__TOKEN) {
		if (g_regex_match_simple(tagsistant.triple_tag_regex, __TOKEN, 0, 0)) {
			qtree->first_tag = qtree->second_tag = qtree->last_tag = NULL;

			qtree->namespace = g_strdup(__TOKEN);
			if (__NEXT_TOKEN) {
				__SLIDE_TOKEN;
				qtree->key = g_strdup(__TOKEN);
				if (__NEXT_TOKEN) {
					__SLIDE_TOKEN;
					qtree->value = g_strdup(__TOKEN);
				}
			}
		} else {
			qtree->namespace = qtree->key = qtree->value = NULL;

			qtree->first_tag = qtree->last_tag = g_strdup(__TOKEN);
			/*
			__SLIDE_TOKEN;
			if (__TOKEN) {
				qtree->second_tag = g_strdup(__TOKEN);
			}
			*/
		}
	}

	return (1);
}

/**
 * 'consumes' (remove from the token stack) enough tokens to
 * for a triple tag, if available
 *
 * @param qtree the tagsistant_querytree object to be filled
 * @param token_ptr the token stack
 * @param is_related if true, put tokens in tagsistant_querytree related_namespace, related_key and related_value fields, otherwise use namespace, key and value fields
 */
void tagsistant_querytree_parse_relations_consume_triple (
	tagsistant_querytree *qtree,
	gchar ***token_ptr,
	int is_related)
{
	gchar **namespace = is_related ? &qtree->related_namespace : &qtree->namespace;
	gchar **key = is_related ? &qtree->related_key : &qtree->key;
	gchar **value = is_related ? &qtree->related_value : &qtree->value;

	g_free_null(*namespace);
	g_free_null(*key);
	g_free_null(*value);

	/*
	 * get the namespace
	 */
	*namespace = g_strdup(__TOKEN);

	if (__NEXT_TOKEN) {
		__SLIDE_TOKEN;
		/*
		 * get the key
		 */
		*key = g_strdup(__TOKEN);

		if (__NEXT_TOKEN) {
			__SLIDE_TOKEN;
			/*
			 * get the operator
			 */
			*value = g_strdup(__TOKEN);

			if (is_related) qtree->complete = 1;
		}
	}
}

#define TAGSISTANT_IS_RELATED 1
#define TAGSISTANT_IS_NOT_RELATED 0

/**
 * parse the query portion after relations/
 *
 * @param qtree the querytree object
 * @param token_ptr a pointer to the tokenized path (three stars because we need to move it across the array even in calling function)
 * @return 1 on success, 0 on failure or errors
 */
int tagsistant_querytree_parse_relations (
	tagsistant_querytree* qtree,
	gchar ***token_ptr)
{
	/* parse a relations query */
	if (__TOKEN) {
		if (g_regex_match_simple(tagsistant.triple_tag_regex, __TOKEN, 0, 0)) {
			/*
			 *  the left tag is a triple tag
			 */
			tagsistant_querytree_parse_relations_consume_triple(qtree, token_ptr, TAGSISTANT_IS_NOT_RELATED);

			if (__NEXT_TOKEN) {
				__SLIDE_TOKEN;

				/* check if relation is allowed */
				if (!g_regex_match_simple(TAGSISTANT_RELATION_PATTERN, __TOKEN, G_REGEX_EXTENDED, 0)) return (0);

				qtree->relation = g_strdup(__TOKEN);

				if (__NEXT_TOKEN) {
					__SLIDE_TOKEN;

					if (g_regex_match_simple(tagsistant.triple_tag_regex, __TOKEN, 0, 0)) {
						/*
						 *  the right (related) tag is a triple tag
						 */
						tagsistant_querytree_parse_relations_consume_triple(qtree, token_ptr, TAGSISTANT_IS_RELATED);
					} else {
						/*
						 *  the right (related) tag is a triple tag
						 */
						qtree->second_tag = g_strdup(__TOKEN);
						qtree->complete = 1;
					}

					/*
					 * a relations/ path should be completed so far,
					 * if another token is available, the path is malformed
					 */
					if (__NEXT_TOKEN) qtree->type = QTYPE_MALFORMED;
				}
			}
		} else {
			/*
			 *  the left tag is a flat tag
			 */
			qtree->first_tag = g_strdup(__TOKEN);

			if (__NEXT_TOKEN) {
				__SLIDE_TOKEN;

				/* check if relation is allowed */
				if (!g_regex_match_simple(TAGSISTANT_RELATION_PATTERN, __TOKEN, G_REGEX_EXTENDED, 0)) return (0);

				qtree->relation = g_strdup(__TOKEN);

				if (__NEXT_TOKEN) {
					__SLIDE_TOKEN;

					if (g_regex_match_simple(tagsistant.triple_tag_regex, __TOKEN, 0, 0)) {
						/*
						 *  the right (related) tag is a triple tag
						 */
						tagsistant_querytree_parse_relations_consume_triple(qtree, token_ptr, TAGSISTANT_IS_RELATED);
					} else {
						/*
						 *  the right (related) tag is a flat tag
						 */
						qtree->second_tag = g_strdup(__TOKEN);
						qtree->complete = 1;
					}

					/*
					 * a relations/ path should be completed so far,
					 * if another token is available, the path is malformed
					 */
					if (__NEXT_TOKEN) qtree->type = QTYPE_MALFORMED;
				}
			}
		}
	}

	__SLIDE_TOKEN;

	return (1);
}

/**
 * parse the query portion after stats/
 *
 * @param qtree the querytree object
 * @param token_ptr a pointer to the tokenized path (three stars because we need to move it across the array even in calling function)
 * @return 1 on success, 0 on failure or errors
 */
int tagsistant_querytree_parse_stats (
	tagsistant_querytree* qtree,
	gchar ***token_ptr)
{
	if (__TOKEN isNot NULL) {
		qtree->stats_path = g_strdup(__TOKEN);
		qtree->complete = 1;
	}

	return (1);
}

int tagsistant_querytree_parse_alias(
	tagsistant_querytree* qtree,
	gchar ***token_ptr)
{
	if (__TOKEN) {
		qtree->alias = g_strdup(__TOKEN);

		// check if another element is present
		__SLIDE_TOKEN;
		if (__TOKEN) qtree->valid = 0;
	}
	return (1);
}

int tagsistant_querytree_parse_archive(
	tagsistant_querytree* qtree,
	gchar ***token_ptr)
{
	qtree->object_path = g_strjoinv(G_DIR_SEPARATOR_S, *token_ptr);
	qtree->inode = tagsistant_inode_extract_from_path(qtree->object_path);

	if (qtree->inode) {
		/*
		 * replace the inode and the separator with a blank string,
		 * actually stripping it from the object_path
		 */
		gchar *new_object_path = g_regex_replace(
			tagsistant_inode_extract_from_path_regex_1,
			qtree->object_path,
			strlen(qtree->object_path),
			0, "", 0, NULL);

		g_free(qtree->object_path);
		qtree->object_path = new_object_path;

		tagsistant_querytree_set_inode(qtree, qtree->inode);
	}

	if (strlen(qtree->object_path) is 0) {
		qtree->archive_path = g_strdup("");
		qtree->full_archive_path = g_strdup(tagsistant.archive);
	} else {
		qtree->archive_path = g_strdup(qtree->object_path);
		qtree->full_archive_path = g_strdup_printf("%s/%s", tagsistant.archive, qtree->archive_path);
	}

	return (1);
}

/**
 * return the directory structure derived from the reverse path of
 * an inode. If the inode was 1985 and TAGSISTANT_ARCHIVE_DEPTH was 3,
 * this function would return the string:
 *
 *   "/5/8/9"
 *
 * @param inode the inode to be reversed
 * @return the directory tree string
 */
gchar *tagsistant_get_reversed_inode_tree(tagsistant_inode inode)
{
	gchar *reversed_inode = g_strreverse(g_strdup_printf("%u", inode % TAGSISTANT_ARCHIVE_DEPTH));
	GRegex *rx = g_regex_new("(.)", 0, G_REGEX_MATCH_NOTEMPTY, NULL);
	gchar *relative_path = g_regex_replace(rx, reversed_inode, -1, 0, "/\\1", 0, NULL);
	g_regex_unref(rx);
	g_free(reversed_inode);
	return (relative_path);
}

/**
 * rebuild the paths of a tagsistant_querytree object
 * a path contains a hierarchy of directories which is derived by the
 * reverse of the object inode. For example, if an object has inode 3987,
 * its path under archive/ will be:
 *
 *   archive/7/8/9/3/3987___object.ext
 *
 * This schema is intended to mitigate the archive/ directory overcrowding
 * when more than tens of thousands of files are stored inside Tagsistant.
 *
 * @param qtree the tagsistant_querytree object
 */
void tagsistant_querytree_rebuild_paths(tagsistant_querytree *qtree)
{
	if (!qtree || !qtree->inode) return;

	/* get inode's readable string and point its last char as printable_inode_ptr */
	gchar *relative_path = tagsistant_get_reversed_inode_tree(qtree->inode);

	/* build the full directory path under archive/ */
	gchar *full_archive_hierarchy = g_strdup_printf("%s%s", tagsistant.archive, relative_path);
	g_free(relative_path);

	/* make the corresponding archive/ directory */
	if (g_mkdir_with_parents(full_archive_hierarchy, 0755) is -1) {
		dbg('q', LOG_ERR, "Error creating directory %s", full_archive_hierarchy);
	}

	/* reset the querytree archive path */
	g_free_null(qtree->archive_path);
	qtree->archive_path = g_strdup_printf("%d" TAGSISTANT_INODE_DELIMITER "%s", qtree->inode, qtree->object_path);

	/* reset the querytree full archive path */
	g_free_null(qtree->full_archive_path);
	qtree->full_archive_path = g_strdup_printf("%s/%s", full_archive_hierarchy, qtree->archive_path);

	dbg('q', LOG_ERR, "Full archive/ path is  %s", qtree->full_archive_path);

	/* free the string with the archive/ path */
	g_free(full_archive_hierarchy);
}

/**
 * set the object_path field of a tagsistant_querytree object and
 * then update all the other depending fields
 *
 * @param qtree the tagsistant_querytree object
 * @param path the new object_path which is copied by g_strdup()
 */
void tagsistant_querytree_set_object_path(tagsistant_querytree *qtree, char *new_object_path)
{
	if (!new_object_path) return;

	if (qtree->object_path) g_free_null(qtree->object_path);
	qtree->object_path = g_strdup(new_object_path);

	tagsistant_querytree_rebuild_paths(qtree);
}

/**
 * set the object inode and rebuild the paths
 *
 * @param qtree the tagsistant_querytree object
 * @param inode the new inode
 */
void tagsistant_querytree_set_inode(tagsistant_querytree *qtree, tagsistant_inode inode)
{
	if (!qtree || (inode <= 0)) return;

	dbg('q', LOG_INFO, "Setting %s inode to %d", qtree->full_path, inode);
	qtree->inode = inode;
	tagsistant_querytree_rebuild_paths(qtree);
}

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE

/**
 * tagsistant_querytree objects cache
 */
GHashTable *tagsistant_querytree_cache = NULL;
GRWLock tagsistant_querytree_cache_lock;

/**
 * Counts one single element of the querytree hashtable
 *
 * @return
 */
void tagsistant_querytree_cache_counter(gpointer key, gpointer value, gpointer user_data)
{
	(void) key;
	(void) value;

 	int *elements = (int *) user_data;
	*elements = *elements + 1;
}

/**
 * Count the elements contained in the querytree cache
 *
 * @param qtree
 */
int tagsistant_querytree_cache_total()
{
	int elements = 0;
	g_hash_table_foreach(tagsistant_querytree_cache, tagsistant_querytree_cache_counter, &elements);
	return (elements);
}

/**
 * Duplicate a qtree_and_node branch
 *
 * @param origin
 * @return
 */
qtree_and_node *tagsistant_querytree_duplicate_qtree_and_node(qtree_and_node *origin)
{
	if (!origin) return (NULL);

	qtree_and_node *copy = g_new0(qtree_and_node, 1);
	if (!copy) return (NULL);

	copy->negate 		= origin->negate;
	copy->tag 			= g_strdup(origin->tag);
	copy->tag_id 		= origin->tag_id;
	copy->namespace 	= g_strdup(origin->namespace);
	copy->key 			= g_strdup(origin->key);
	copy->operator 		= origin->operator;
	copy->value 		= g_strdup(origin->value);

	copy->related 		= tagsistant_querytree_duplicate_qtree_and_node(origin->related);
	copy->negated		= tagsistant_querytree_duplicate_qtree_and_node(origin->negated);
	copy->next			= tagsistant_querytree_duplicate_qtree_and_node(origin->next);

	return (copy);
}

/**
 * Duplicate a qtree_or_node tree from a querytree
 *
 * @param origin
 * @return
 */
qtree_or_node *tagsistant_querytree_duplicate_qtree_or_node(qtree_or_node *origin)
{
	if (!origin) return (NULL);

	qtree_or_node *copy = g_new0(qtree_or_node, 1);
	if (!copy) return (NULL);

	copy->next = tagsistant_querytree_duplicate_qtree_or_node(origin->next);
	copy->and_set = tagsistant_querytree_duplicate_qtree_and_node(origin->and_set);

	return (copy);
}

/**
 * Duplicate a querytree, assigning a new connection
 *
 * @param qtree
 * @return
 */
tagsistant_querytree *tagsistant_querytree_duplicate(tagsistant_querytree *qtree)
{
	/*
	 * create the duplicated querytree object
	 */
	tagsistant_querytree *duplicated = g_new0(tagsistant_querytree, 1);
	if (!duplicated) return (NULL);

	/*
	 * duplicate all the strings (->operator and ->related_operator
	 * fields are not strings but we keep them here for completeness
	 * since operators are part of triple tags with namespaces, keys
	 * and values)
	 */
	duplicated->full_path			= g_strdup(qtree->full_path);
	duplicated->expanded_full_path	= g_strdup(qtree->expanded_full_path);
	duplicated->object_path			= g_strdup(qtree->object_path);
	duplicated->archive_path		= g_strdup(qtree->archive_path);
	duplicated->full_archive_path	= g_strdup(qtree->full_archive_path);
	duplicated->last_tag			= g_strdup(qtree->last_tag);
	duplicated->first_tag			= g_strdup(qtree->first_tag);
	duplicated->second_tag			= g_strdup(qtree->second_tag);
	duplicated->namespace			= g_strdup(qtree->namespace);
	duplicated->key					= g_strdup(qtree->key);
	duplicated->operator			= qtree->operator;
	duplicated->value				= g_strdup(qtree->value);
	duplicated->related_namespace	= g_strdup(qtree->related_namespace);
	duplicated->related_key			= g_strdup(qtree->related_key);
	duplicated->related_operator	= qtree->related_operator;
	duplicated->related_value		= g_strdup(qtree->related_value);
	duplicated->relation			= g_strdup(qtree->relation);
	duplicated->stats_path			= g_strdup(qtree->stats_path);
	duplicated->alias				= g_strdup(qtree->alias);
	duplicated->error_message		= g_strdup(qtree->error_message);

	/*
	 * duplicate all the numeric fields
	 */
	duplicated->inode 						= qtree->inode;
	duplicated->type						= qtree->type;
	duplicated->points_to_object			= qtree->points_to_object;
	duplicated->is_taggable					= qtree->is_taggable;
	duplicated->is_external					= qtree->is_external;
	duplicated->valid						= qtree->valid;
	duplicated->complete					= qtree->complete;
	duplicated->exists						= qtree->exists;
	duplicated->transaction_started			= qtree->transaction_started;
	duplicated->force_inode_in_filenames	= qtree->force_inode_in_filenames;
	duplicated->negate_next_tag				= qtree->negate_next_tag;
	duplicated->do_reasoning				= qtree->do_reasoning;
	duplicated->schedule_for_unlink			= qtree->schedule_for_unlink;

	/*
	 * duplicate the tag tree
	 */
	duplicated->tree = tagsistant_querytree_duplicate_qtree_or_node(qtree->tree);

	/*
	 * return the querytree
	 */
	return (duplicated);
}

/**
 * Lookup a querytree from the cache
 *
 * @param path the query path
 * @return a tagsistant_querytree duplicated object
 */
tagsistant_querytree *tagsistant_querytree_lookup(const char *path)
{
	/*
	 * lookup the querytree
	 */
	tagsistant_querytree *qtree = NULL;
	g_rw_lock_reader_lock(&tagsistant_querytree_cache_lock);
	qtree = g_hash_table_lookup (tagsistant_querytree_cache, path);
	g_rw_lock_reader_unlock(&tagsistant_querytree_cache_lock);

	/*
	 * not found, return and proceed to normal creation
	 */
	if (!qtree) return (NULL);

	/*
	 * the querytree is no longer valid, so we destroy it and return NULL
	 */
	struct stat st;
	if (qtree->full_archive_path && (stat(qtree->full_archive_path, &st) isNot 0)) {
		g_rw_lock_reader_lock(&tagsistant_querytree_cache_lock);
		g_hash_table_remove(tagsistant_querytree_cache, path);
		g_rw_lock_reader_unlock(&tagsistant_querytree_cache_lock);

		return (NULL);
	}

	/*
	 * set the last_access_microsecond time
	 * will be used in future code to decide if a cached entry
	 * could be removed for memory management
	 */
	qtree->last_access_microsecond = g_get_real_time();

	/*
	 * return a duplicate of the qtree
	 */
	return (tagsistant_querytree_duplicate(qtree));
}

/**
 * Return true if entry->full_path contains qtree->first_tag or qtree->second_tag
 * or both.
 *
 * @param key unused
 * @param entry the querytree object from the cache being evaluated
 * @param qtree the querytree object invalidating the cache
 * @return TRUE if a match is found, FALSE otherwise
 */
gboolean tagsistant_invalidate_querytree_entry(gpointer key_p, gpointer entry_p, gpointer qtree_p)
{
	(void) key_p;
	(void) entry_p;
	(void) qtree_p;

	return (TRUE);

#if 0
	tagsistant_querytree *entry = (tagsistant_querytree *) entry_p;
	tagsistant_querytree *qtree = (tagsistant_querytree *) qtree_p;

	gboolean matches = FALSE;

	gchar *first_tag = g_strdup_printf("/%s/", qtree->first_tag);
	gchar *second_tag = g_strdup_printf("/%s/", qtree->second_tag);

	if (strstr(entry->full_path, first_tag) || strstr(entry->full_path, second_tag))
		matches = TRUE;

	g_free_null(first_tag);
	g_free_null(second_tag);

	return (matches);
#endif
}

/**
 * Delete the cache entries which involve one of the tags qtree->first_tag
 * and qtree->second_tag
 *
 * @param qtree the querytree object which is invalidating the cache
 */
void tagsistant_invalidate_querytree_cache(tagsistant_querytree *qtree)
{
	g_rw_lock_writer_lock(&tagsistant_querytree_cache_lock);
	g_hash_table_foreach_remove(tagsistant_querytree_cache, tagsistant_invalidate_querytree_entry, qtree);
	g_rw_lock_writer_unlock(&tagsistant_querytree_cache_lock);
}

#endif // TAGSISTANT_ENABLE_QUERYTREE_CACHE

/**
 * Alias replacement callback
 */
gboolean  tagsistant_expand_path_callback(
	const GMatchInfo *match_info,
    GString *result,
    gpointer user_data)
{
	tagsistant_querytree *qtree = (tagsistant_querytree *) user_data;

	// fetch the pattern
	gchar *pattern = g_match_info_fetch(match_info, 0);

	// the alias is the patter excluded the identifier (thus the + 1)
	gchar *alias = pattern + 1;

	// load the expansion from the DB
	gchar *alias_expansion = tagsistant_sql_alias_get(qtree->dbi, alias);

	// replace the alias
	g_string_append(result, alias_expansion);

	// free allocated memory
	g_free(alias_expansion);
	g_free(pattern);

	return FALSE;
}

/**
 * expand a path, resolving the aliases
 *
 * @param qtree the tagsistant_querytree to expand
 * @return the expanded path
 */
gchar *tagsistant_expand_path(tagsistant_querytree *qtree)
{
	// duplicate the path to work on it
	gchar *expanded_path = g_strdup(qtree->full_path);

	// declare a regular expression to detect the aliases
	GRegex *rx = g_regex_new(TAGSISTANT_ALIAS_IDENTIFIER "([^/]+)", 0, 0, NULL);

	// apply the regular expression
	GMatchInfo *match_info;
	while (g_regex_match(rx, expanded_path, 0, &match_info)) {
		// get the alias found in the path
		gchar *pattern = g_match_info_fetch(match_info, 0);

		// build the GRegex of the alias
		GRegex *replace_rx = g_regex_new(pattern, 0, 0, NULL);

		// expand the alias
		gchar *replaced = g_regex_replace_eval(
			replace_rx,
			expanded_path,
			-1, 0, 0,
			tagsistant_expand_path_callback,
			qtree,
			NULL);

		// update the expanded path
		g_free(expanded_path);
		expanded_path = replaced;

		// free allocated memory
		g_free(pattern);
		g_regex_unref(replace_rx);
	}

	// free allocated memory
	g_match_info_free(match_info);
	g_regex_unref(rx);

	// remove duplicated slashes
	GRegex *slash_compressor = g_regex_new("/+", 0, 0, NULL);
	gchar *simplified_path = g_regex_replace_literal(slash_compressor, expanded_path, -1, 0, "/", 0, NULL);
	g_free(expanded_path);
	g_regex_unref(slash_compressor);

	// return the simplified path
	return (simplified_path);
}

qtree_and_node *tagsistant_duplicate_and_set(qtree_and_node *orig)
{
	qtree_and_node *node = g_new0(qtree_and_node, 1);
	if (!node) return (NULL);

	node->key = g_strdup(orig->key);
	node->namespace = g_strdup(orig->namespace);
	node->negate = orig->negate;
	node->negated = tagsistant_duplicate_and_set(orig->negated);
	node->next = tagsistant_duplicate_and_set(orig->next);
	node->operator = orig->operator;
	node->related = tagsistant_duplicate_and_set(orig->related);
	node->tag = g_strdup(orig->tag);
	node->tag_id = orig->tag_id;
	node->value = g_strdup(orig->value);

	return (node);
}

qtree_or_node *tagsistant_duplicate_tree(qtree_or_node *orig)
{
	qtree_or_node *node = g_new0(qtree_or_node, 1);
	if (!node) return (NULL);

	node->and_set = tagsistant_duplicate_and_set(orig->and_set);
	node->next = tagsistant_duplicate_tree(orig->next);

	return (node);
}

/**
 * Build query tree from path. A querytree is composed of a linked
 * list of qtree_or_node_t objects. Each or object has a descending
 * linked list of qtree_and_node_t objects. This kind of structure
 * should be considered as a collection (the or horizontal level)
 * of queries (the and vertical lists). Each and list should be
 * resolved using SQLite INTERSECT compound operator. All results
 * should be then merged in a unique list of filenames.
 *
 * @param path                the path to be converted in a logical query
 * @param assign_inode        if the path has not an associated inode, assign one (used in mknod() and such)
 * @param start_transaction   do a "start transaction" on the DB connection
 * @param provide_connection  if true, create a DBI connection
 * @param disable_reasoner    some FUSE operations do not get affected by the reasoner and can disable it
 * @return a tagsistant_querytree object
 */
tagsistant_querytree *tagsistant_querytree_new(
	const char *path,
	int assign_inode,
	int start_transaction,
	int provide_connection,
	int disable_reasoner)
{
	(void) assign_inode;

	tagsistant_querytree *qtree = NULL;

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
	/*
	 * look up the querytree object in the cache
	 */
	qtree = tagsistant_querytree_lookup(path);
	if (qtree) {
		if (provide_connection) {
			/* assign a new connection */
			qtree->dbi = tagsistant_db_connection(start_transaction);
			qtree->transaction_started = start_transaction;
		}

		return (qtree);
	}
#endif

	/*
	 * the qtree object has not been found so lets allocate the querytree structure
	 */
	qtree = g_new0(tagsistant_querytree, 1);
	if (qtree is NULL) {
		dbg('q', LOG_ERR, "Error allocating memory");
		return(NULL);
	}

	/*
	 * set default values
	 * TODO: can this be avoided since we use g_new0() to create the struct?
	 */
	qtree->inode = 0;
	qtree->type = 0;
	qtree->points_to_object = 0;
	qtree->is_taggable = 0;
	qtree->is_external = 0;
	qtree->valid = 0;
	qtree->complete = 0;
	qtree->exists = 0;
	qtree->do_reasoning = 0;
	qtree->schedule_for_unlink = 0;
	qtree->force_inode_in_filenames = 0;
	qtree->error_message = NULL;

	/*
	 * tie this query to a DBI handle
	 */
	if (provide_connection) {
		qtree->dbi = tagsistant_db_connection(start_transaction);
		qtree->transaction_started = start_transaction;
	} else {
		qtree->dbi = NULL;
	}

	/*
	 * duplicate the path inside the struct
	 */
	qtree->full_path = g_strdup(path);

	/*
	 * expand the path, resolving aliases
	 */
	if (g_regex_match_simple("/(" TAGSISTANT_QUERY_DELIMITER "|" TAGSISTANT_QUERY_DELIMITER_NO_REASONING ")", qtree->full_path, 0, 0)) {
		qtree->expanded_full_path = tagsistant_expand_path(qtree);
	} else {
		qtree->expanded_full_path = g_strdup(qtree->full_path);
	}

	dbg('q', LOG_INFO, "Building querytree for %s", qtree->full_path);

	/*
	 * split the path
	 */
	gchar **splitted = g_strsplit(qtree->expanded_full_path, "/", 512); /* split up to 512 tokens */
	gchar __TOKEN = splitted + 1; /* first element is always "" since path begins with '/' */

	/* guess the type of the query by first token */
	if (__TOKEN is '\0')								qtree->type = QTYPE_ROOT;
	else if (g_strcmp0(*token_ptr, "store") is 0)		qtree->type = QTYPE_STORE;
	else if (g_strcmp0(*token_ptr, "relations") is 0)	qtree->type = QTYPE_RELATIONS;
	else if (g_strcmp0(*token_ptr, "tags") is 0)		qtree->type = QTYPE_TAGS;
	else if (g_strcmp0(*token_ptr, "alias") is 0)		qtree->type = QTYPE_ALIAS;
	else if (g_strcmp0(*token_ptr, "archive") is 0)		qtree->type = QTYPE_ARCHIVE;
	else if (g_strcmp0(*token_ptr, "stats") is 0)		qtree->type = QTYPE_STATS;
	else {
		qtree->type = QTYPE_MALFORMED;
		dbg('q', LOG_ERR, "Malformed or not existing path (%s)", path);
		goto RETURN;
	}

	/*
	 * then skip first token (used for guessing query type)
	 */
	token_ptr++;

	/*
	 * do selective parsing of the query
	 */
	if (QTREE_IS_STORE(qtree)) {
		if (!tagsistant_querytree_parse_store(qtree, path, &token_ptr, disable_reasoner)) goto RETURN;
	} else if (QTREE_IS_TAGS(qtree)) {
		if (!tagsistant_querytree_parse_tags(qtree, &token_ptr)) goto RETURN;
	} else if (QTREE_IS_RELATIONS(qtree)) {
		tagsistant_querytree_parse_relations(qtree, &token_ptr);
	} else if (QTREE_IS_STATS(qtree)) {
		tagsistant_querytree_parse_stats(qtree, &token_ptr);
	} else if (QTREE_IS_ALIAS(qtree)) {
		tagsistant_querytree_parse_alias(qtree, &token_ptr);
	} else if (QTREE_IS_ARCHIVE(qtree)) {
		tagsistant_querytree_parse_archive(qtree, &token_ptr);
	}

	/*
	 * do some logging...
	 */
	dbg('q', LOG_INFO, "inode = %d", qtree->inode);
	dbg('q', LOG_INFO, "object_path = \"%s\"", qtree->object_path);
	dbg('q', LOG_INFO, "archive_path = \"%s\"", qtree->archive_path);
	dbg('q', LOG_INFO, "full_archive_path = \"%s\"", qtree->full_archive_path);

	/*
	 * guess if query points to an object on disk or not
	 * that's true if object_path property is not null or zero length
	 * and both query is /archive or query is a complete /tags (including = sign)
	 */
	if (
		(QTREE_IS_ARCHIVE(qtree) || (QTREE_IS_STORE(qtree) && qtree->complete && qtree->object_path))
		&&
		(strlen(qtree->object_path) > 0)
	) {
		qtree->points_to_object = 1;

		if (!qtree->inode)
			dbg('q', LOG_INFO, "Qtree path %s points to an object but does NOT contain an inode", qtree->full_path);
	} else {
		qtree->points_to_object = 0;
	}

RETURN:
	g_strfreev(splitted);

	if (QTREE_IS_MALFORMED(qtree) && !qtree->error_message) {
		qtree->error_message = g_strdup(TAGSISTANT_ERROR_MALFORMED_QUERY);
	}

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
	/*
	 * cache the querytree object
	 */
	if ((!qtree->points_to_object) || qtree->inode) {
		/* save the querytree in the cache */
		tagsistant_querytree *duplicated = tagsistant_querytree_duplicate(qtree);

		g_rw_lock_writer_lock(&tagsistant_querytree_cache_lock);
		g_hash_table_insert(tagsistant_querytree_cache, duplicated->full_path, duplicated);
		g_rw_lock_writer_unlock(&tagsistant_querytree_cache_lock);
	}
#endif

	return(qtree);
}

/**
 * Quick macro to free a qtree_and_node object
 *
 * @param andnode a qtree_and_node to be freed
 */
#define qtree_and_node_destroy(andnode) {\
	g_free_null(andnode->tag);\
	g_free_null(andnode->namespace);\
	g_free_null(andnode->key);\
	g_free_null(andnode->value);\
	g_free_null(andnode);\
}

/**
 * destroy a tagsistant_querytree_t structure
 *
 * @param qtree the tagsistant_querytree_t to be destroyed
 */
void tagsistant_querytree_destroy(tagsistant_querytree *qtree, guint commit_transaction)
{
	if (!qtree) return;

	/* if scheduled for unlink, unlink it */
	if (qtree->schedule_for_unlink)
		unlink(qtree->full_archive_path);

	/* commit the transaction, if any, and mark the connection as available */
	if (qtree->dbi) {
		if (qtree->transaction_started) {
			if (commit_transaction)
				tagsistant_commit_transaction(qtree->dbi);
			else
				tagsistant_rollback_transaction(qtree->dbi);
		}

		tagsistant_db_connection_release(qtree->dbi, qtree->transaction_started);
	}

	/* free the paths */
	g_free_null(qtree->full_path);
	g_free_null(qtree->expanded_full_path);
	g_free_null(qtree->object_path);
	g_free_null(qtree->archive_path);
	g_free_null(qtree->full_archive_path);
	g_free_null(qtree->error_message);

	if (QTREE_IS_STORE(qtree)) {
		qtree_or_node *node = qtree->tree;
		while (node isNot NULL) {

			qtree_and_node *tag = node->and_set;
			while (tag isNot NULL) {

				// walk related tags
				while (tag->related) {
					qtree_and_node *related = tag->related;
					tag->related = tag->related->related;
					qtree_and_node_destroy(related);
				}

				// free the qtree_and_node_t node
				qtree_and_node *next = tag->next;
				qtree_and_node_destroy(tag);
				tag = next;
			}

			// free the qtree_or_node_t node
			qtree_or_node *next = node->next;
			g_free_null(node);
			node = next;
		}
		g_free_null(qtree->last_tag);
		g_free_null(qtree->namespace);
		g_free_null(qtree->key);
		g_free_null(qtree->value);
	} else if (QTREE_IS_TAGS(qtree)) {
		g_free_null(qtree->namespace);
		g_free_null(qtree->key);
		g_free_null(qtree->value);
	} else if (QTREE_IS_RELATIONS(qtree)) {
		g_free_null(qtree->first_tag);
		g_free_null(qtree->second_tag);
		g_free_null(qtree->relation);
	} else if (QTREE_IS_STATS(qtree)) {
		g_free_null(qtree->stats_path);
	}

	// free the structure
	g_free_null(qtree);
}

extern void tagsistant_querytree_traverse(
	tagsistant_querytree *qtree,
	tagsistant_querytree_traverser funcpointer,
	tagsistant_inode opt_inode)
{
    if (!qtree) { return; }

	qtree_or_node *ptx = qtree->tree;
	while (ptx isNot NULL) {
		qtree_and_node *andptx = ptx->and_set;
		while (andptx isNot NULL) {
			if (andptx->tag) {
				funcpointer(qtree->dbi, andptx->tag, NULL, NULL, opt_inode);
			} else {
				funcpointer(qtree->dbi, andptx->namespace, andptx->key, andptx->value, opt_inode);
			}
			andptx = andptx->next;
		}
		ptx = ptx->next;
	}

    return;
}

/**
 * Initialize path_resolution.c module
 */
void tagsistant_path_resolution_init()
{
	/* prepare stringified version of tagsistant_querytree types */
	tagsistant_querytree_types[QTYPE_MALFORMED]	= g_strdup("QTYPE_MALFORMED");
	tagsistant_querytree_types[QTYPE_ROOT]		= g_strdup("QTYPE_ROOT");
	tagsistant_querytree_types[QTYPE_ARCHIVE]	= g_strdup("QTYPE_ARCHIVE");
	tagsistant_querytree_types[QTYPE_TAGS]		= g_strdup("QTYPE_TAGS");
	tagsistant_querytree_types[QTYPE_RELATIONS]	= g_strdup("QTYPE_RELATIONS");
	tagsistant_querytree_types[QTYPE_STATS]		= g_strdup("QTYPE_STATS");
	tagsistant_querytree_types[QTYPE_STORE]		= g_strdup("QTYPE_STORE");
	tagsistant_querytree_types[QTYPE_ALIAS]		= g_strdup("QTYPE_ALIAS");

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
	/* initialize the tagsistant_querytree object cache */
	tagsistant_querytree_cache = g_hash_table_new_full(
		g_str_hash,
		g_str_equal,
		NULL,
		(GDestroyNotify) tagsistant_querytree_cache_destroy_element);
#endif // TAGSISTANT_ENABLE_QUERYTREE_CACHE

#if TAGSISTANT_ENABLE_AND_SET_CACHE
	tagsistant_and_set_cache = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
#endif

	/* compile regular expressions */
	tagsistant_inode_extract_from_path_regex_1 = g_regex_new("^([0-9]+)" TAGSISTANT_INODE_DELIMITER, 0, 0, NULL);
	tagsistant_inode_extract_from_path_regex_2 = g_regex_new("/([0-9]+)" TAGSISTANT_INODE_DELIMITER, 0, 0, NULL);
}
