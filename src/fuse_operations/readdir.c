/*
   Tagsistant (tagfs) -- fuse_operations/readdir.c
   Copyright (C) 2006-2014 Tx0 <tx0@strumentiresistenti.org>

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

#include "../tagsistant.h"

/**
 * used by add_entry_to_dir() SQL callback to perform readdir() operations
 */
struct tagsistant_use_filler_struct {
	fuse_fill_dir_t filler;			/**< libfuse filler hook to return dir entries */
	void *buf;						/**< libfuse buffer to hold readdir results */
	const char *path;				/**< the path that generates the query */
	tagsistant_querytree *qtree;	/**< the querytree that originated the readdir() */
	int is_alias;					/**< set to 1 if entries are aliases and must be prefixed with the alias identifier (=) */
};

/**
 * SQL callback. Add dir entries to libfuse buffer.
 *
 * @param filler_ptr struct tagsistant_use_filler_struct pointer (cast to void*)
 * @param result dbi_result pointer
 * @return(0 (always, see SQLite policy, may change in the future))
 */
static int tagsistant_add_entry_to_dir(void *filler_ptr, dbi_result result)
{
	struct tagsistant_use_filler_struct *ufs = (struct tagsistant_use_filler_struct *) filler_ptr;
	const char *dir = dbi_result_get_string_idx(result, 1);

	/* this must be the last value, just exit */
	if (dir is NULL) return(0);

	/*
	 * zero-length values can be returned while listing triple tags
	 * we must suppress them, but returning 1, to prevent the callback
	 * from exiting its cycle
	 */
	if (strlen(dir) is 0) return (1);

	/* check if this tag has been already listed inside the path */
	qtree_or_node *ptx = ufs->qtree->tree;
	if (ptx) {
		while (ptx->next isNot NULL) ptx = ptx->next; // last OR section

		qtree_and_node *and_t = ptx->and_set;
		while (and_t isNot NULL) {
			if (g_strcmp0(and_t->tag, dir) is 0) {
				return(0);
			}
			and_t = and_t->next;
		}
	}

	if (ufs->is_alias) {
		/*
		 *  prepend the alias identified otherwise
		 */
		gchar *entry = g_strdup_printf("=%s", dir);
		int filler_result = ufs->filler(ufs->buf, entry, NULL, 0);
		g_free(entry);
		return (filler_result);
	} else if (ufs->qtree->force_inode_in_filenames) {
		/*
		 * force the inode in the object name, assuming a second field
		 * provides the inode
		 */
		const char *inode = dbi_result_get_string_idx(result, 2);
		if (inode) {
			gchar *entry = g_strdup_printf("%s%s%s", inode, TAGSISTANT_INODE_DELIMITER, dir);
			int filler_result = ufs->filler(ufs->buf, entry, NULL, 0);
			g_free(entry);
			return (filler_result);
		} else {
			return(ufs->filler(ufs->buf, dir, NULL, 0));
		}
	} else {
		/*
		 * add the entry as is if it's not an alias
		 */
		return(ufs->filler(ufs->buf, dir, NULL, 0));
	}
}

/**
 * Add a tag while listing the export/ first level directory
 *
 * @param filler_ptr a pointer to a tagsistant_use_filler_struct struct
 * @param result the result of the SQL query to be accessed by DBI methods
 */
static int tagsistant_add_tag_to_export(void *filler_ptr, dbi_result result)
{
	struct tagsistant_use_filler_struct *ufs = (struct tagsistant_use_filler_struct *) filler_ptr;
	const char *tag_or_namespace = dbi_result_get_string_idx(result, 1);

	/* this must be the last value, just exit */
	if (tag_or_namespace is NULL) return(0);

	/*
	 * zero-length values can be returned while listing triple tags
	 * we must suppress them, but returning 1, to prevent the callback
	 * from exiting its cycle
	 */
	if (strlen(tag_or_namespace) is 0) return (1);

	if (g_regex_match_simple(":$", tag_or_namespace, G_REGEX_EXTENDED, 0)) {
		const char *key = dbi_result_get_string_idx(result, 2);
		const char *value = dbi_result_get_string_idx(result, 3);

		if (strlen(value) > 0) {
			gchar *entry = g_strdup_printf("%s%s=%s", tag_or_namespace, key, value);
			int result = ufs->filler(ufs->buf, entry, NULL, 0);
			g_free(entry);
			return (result);
		} else {
			/*
			 * skip this invalid entry but keep adding other entries
			 */
			return (1);
		}
	} else {
		return (ufs->filler(ufs->buf, tag_or_namespace, NULL, 0));
	}
}

/**
 * Add a file entry from a GList to the readdir() buffer
 *
 * @param name unused
 * @param fh_list the GList holding filenames
 * @param ufs a context structure
 * @return 0 always
 */
static int
tagsistant_readdir_on_store_filler(
	gchar *name,
	GList *inode_list,
	struct tagsistant_use_filler_struct *ufs)
{
	if (inode_list is NULL) return (0);

	if (inode_list->next is NULL) {
		/*
		 * single entry, just add the filename
		 */
		if (ufs->qtree->force_inode_in_filenames) {
			tagsistant_inode inode = GPOINTER_TO_UINT(inode_list->data);
			gchar *filename = g_strdup_printf("%d%s%s",	inode, TAGSISTANT_INODE_DELIMITER, name);
			ufs->filler(ufs->buf, filename, NULL, 0);
			g_free_null(filename);
		} else {
			ufs->filler(ufs->buf, name, NULL, 0);
		}

		return (0);
	}

	/*
	 * add all the inodes as separate entries
	 */
	while (inode_list) {
		tagsistant_inode inode = GPOINTER_TO_UINT(inode_list->data);
		gchar *filename = g_strdup_printf("%d%s%s", inode, TAGSISTANT_INODE_DELIMITER, name);
		ufs->filler(ufs->buf, filename, NULL, 0);
		g_free_null(filename);
		inode_list = inode_list->next;
	}

	return (0);
}

/**
 * Return true if the operators +/, @/ and @@/ should be added to
 * while listing the content of a store/ query
 *
 * @param qtree the tagsistant_querytree object
 */
int tagsistant_do_add_operators(tagsistant_querytree *qtree)
{
	gchar tagsistant_check_tags_path_regex[] = "/(\\"
		TAGSISTANT_ANDSET_DELIMITER "|"
		TAGSISTANT_QUERY_DELIMITER "|"
		TAGSISTANT_QUERY_DELIMITER_NO_REASONING "|"
		TAGSISTANT_NEGATE_NEXT_TAG ")$";

	if (!g_regex_match_simple(tagsistant_check_tags_path_regex, qtree->full_path, G_REGEX_EXTENDED, 0)) {
		if (g_strcmp0(qtree->full_path, "/tags")) {
			if (!qtree->namespace || (qtree->namespace && qtree->value)) {
				return (1);
			}
		}
	}

	return (0);
}

/**
 * Check if an _incomplete_ path has an open tag group
 */
int is_inside_tag_group(gchar *path)
{
	if (g_regex_match_simple("/{/[^{}]+$", path, G_REGEX_EXTENDED, 0)) return (1);
	if (g_regex_match_simple("/{$", path, G_REGEX_EXTENDED, 0)) return (1);
	return (0);
}

/**
 * Joins the tags from the last qtree_or_node branch into a string
 * to be used for "requires" relation checking
 *
 * @param qtree the tagsistant_querytree object holding the tree
 * @return a string with all tag IDs joined by commas
 */
gchar *tagsistant_qtree_list_tags_in_last_or_node(tagsistant_querytree *qtree)
{
	GString *tags_list = g_string_new("");

	/*
	 * reach the last qtree_or_node in the tree
	 */
	qtree_or_node *or_ptr = qtree->tree;
	while (or_ptr->next) or_ptr = or_ptr->next;

	/*
	 * loop through the qtree_and_nodes and their related
	 */
	qtree_and_node *and_ptr = or_ptr->and_set;
	while (and_ptr) {
		g_string_append_printf(tags_list, "%d, ", and_ptr->tag_id);
		qtree_and_node *related = and_ptr->related;
		while (related) {
			g_string_append_printf(tags_list, "%d, ", related->tag_id);
			related = related->next;
		}
		and_ptr = and_ptr->next;
	}

	/*
	 * free the GString object saving the compiled string
	 */
	gchar *string = tags_list->str;
	g_string_free(tags_list, FALSE);

	/*
	 * remove the last comma in the list and return
	 */
	gchar *last_comma = rindex(string, ',');
	if (last_comma)	*last_comma = '\0';

	return (string);
}

/**
 * Read the content of the store/ directory
 *
 * @param qtree the tagsistant_querytree object
 * @param path the query path
 * @param buf FUSE buffer used by FUSE filler
 * @param filler the FUSE fuse_fill_dir_t compatible function used to fill the buffer
 * @param off_t the offset of the readdir() operation
 * @param tagsistant_errno pointer to return the state of the errno macro
 * @return always 0
 */
int tagsistant_readdir_on_store(
	tagsistant_querytree *qtree,
	const char *path,
	void *buf,
	fuse_fill_dir_t filler,
	off_t offset,
	int *tagsistant_errno)
{
	(void) offset;

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	/*
	 * check if path contains the ALL/ meta-tag
	 */
	int is_all_path = is_all_path(qtree->full_path);

	/*
	 * if path does not terminate by @,
 	 * directory should be filled with tagsdir registered tags
 	 */
	struct tagsistant_use_filler_struct *ufs = g_new0(struct tagsistant_use_filler_struct, 1);
	if (!ufs) {
		dbg('F', LOG_ERR, "Error allocating memory");
		*tagsistant_errno = ENOMEM;
		return (-1);
	}

	ufs->filler = filler;
	ufs->buf = buf;
	ufs->path = path;
	ufs->qtree = qtree;
	ufs->is_alias = 0;

	if (qtree->complete) {

		if (qtree->error_message) {
			/*
			 * report a file with the error message
			 */
			filler(buf, "error", NULL, 0);
		} else {
			/*
			 * build the filetree
			 */
			tagsistant_rds *rds = tagsistant_rds_new_or_lookup(qtree);
			if (rds) {
				tagsistant_rds_read_lock(rds, qtree);
				g_hash_table_foreach(rds->entries, (GHFunc) tagsistant_readdir_on_store_filler, ufs);
				tagsistant_rds_read_unlock(rds);
			} else {
				dbg('F', LOG_ERR, "Unable to get an RDS when readdir(%s)", qtree->full_archive_path);
			}
		}
	} else {

		// add operators if path is not "/tags", to avoid "/tags/+" and "/tags/@"
		if (tagsistant_do_add_operators(qtree)) {
			if (is_inside_tag_group(qtree->full_path)) {
				filler(buf, TAGSISTANT_TAG_GROUP_END, NULL, 0);
			} else {
				filler(buf, TAGSISTANT_QUERY_DELIMITER, NULL, 0);
				filler(buf, TAGSISTANT_QUERY_DELIMITER_NO_REASONING, NULL, 0);
				if (!is_all_path) {
					filler(buf, TAGSISTANT_ANDSET_DELIMITER, NULL, 0);
					filler(buf, TAGSISTANT_NEGATE_NEXT_TAG, NULL, 0);
					filler(buf, TAGSISTANT_TAG_GROUP_BEGIN, NULL, 0);
				}
			}
		}

		if (is_all_path) {
			// OK

		} else if (qtree->value && strlen(qtree->value)) {
			filler(buf, "ALL", NULL, 0);
			// tagsistant_query("select distinct tagname from tags", qtree->dbi, tagsistant_add_entry_to_dir, ufs);

			gchar *tags_list = tagsistant_qtree_list_tags_in_last_or_node(qtree);

			if (strlen(tags_list)) {
				tagsistant_query(
					"select distinct a.tagname from tags a "
						"left outer join relations r on r.tag1_id = a.tag_id and r.relation = 'requires' "
						"left outer join tags b on b.tag_id = r.tag2_id "
						"where b.tag_id in (%s) or b.tagname is null",
					qtree->dbi,
					tagsistant_add_entry_to_dir,
					ufs,
					tags_list);
			} else {
				tagsistant_query(
					"select distinct a.tagname from tags a "
						"left outer join relations r on r.tag1_id = a.tag_id and r.relation = 'requires' "
						"left outer join tags b on b.tag_id = r.tag2_id "
						"where b.tagname is null",
					qtree->dbi,
					tagsistant_add_entry_to_dir,
					ufs);
			}

			g_free(tags_list);

			ufs->is_alias = 1;
			tagsistant_query("select alias from aliases", qtree->dbi, tagsistant_add_entry_to_dir, ufs);

		} else if (qtree->operator) {
			tagsistant_query(
				"select distinct value from tags where tagname = \"%s\" and `key` = \"%s\"",
				qtree->dbi, tagsistant_add_entry_to_dir, ufs,
				qtree->namespace, qtree->key);

		} else if (qtree->key && strlen(qtree->key)) {
			filler(buf, TAGSISTANT_EQUALS_TO_OPERATOR, NULL, 0);
			filler(buf, TAGSISTANT_CONTAINS_OPERATOR, NULL, 0);
			filler(buf, TAGSISTANT_GREATER_THAN_OPERATOR, NULL, 0);
			filler(buf, TAGSISTANT_SMALLER_THAN_OPERATOR, NULL, 0);

		} else if (qtree->namespace && strlen(qtree->namespace)) {
			tagsistant_query(
				"select distinct `key` from tags where tagname = \"%s\"",
				qtree->dbi, tagsistant_add_entry_to_dir, ufs,
				qtree->namespace);

		} else {
			filler(buf, "ALL", NULL, 0);

			gchar *tags_list = tagsistant_qtree_list_tags_in_last_or_node(qtree);

			if (strlen(tags_list)) {
				tagsistant_query(
					"select distinct a.tagname from tags a "
						"left outer join relations r on r.tag1_id = a.tag_id and r.relation = 'requires' "
						"left outer join tags b on b.tag_id = r.tag2_id "
						"where b.tag_id in (%s) or b.tagname is null",
					qtree->dbi,
					tagsistant_add_entry_to_dir,
					ufs,
					tags_list);
			} else {
				tagsistant_query(
					"select distinct a.tagname from tags a "
						"left outer join relations r on r.tag1_id = a.tag_id and r.relation = 'requires' "
						"left outer join tags b on b.tag_id = r.tag2_id "
						"where b.tagname is null",
					qtree->dbi,
					tagsistant_add_entry_to_dir,
					ufs);
			}

			g_free(tags_list);

			ufs->is_alias = 1;
			tagsistant_query("select alias from aliases", qtree->dbi, tagsistant_add_entry_to_dir, ufs);
		}
	}

	g_free_null(ufs);
	return (0);
}

/**
 * Read the content of an object from the archive/ directory or from a
 * complete query on the store/ directory
 */
int tagsistant_readdir_on_object(
	tagsistant_querytree *qtree,
	const char *path,
	void *buf,
	fuse_fill_dir_t filler,
	int *tagsistant_errno)
{
	(void) path;

	DIR *dp = opendir(qtree->full_archive_path);
	if (dp is NULL) {
		*tagsistant_errno = errno;
		dbg('F', LOG_ERR, "Unable to readdir(%s)", qtree->full_archive_path);
		return (-1);
	}

	struct dirent *de;
	while ((de = readdir(dp)) isNot NULL) {
		// dbg(LOG_INFO, "Adding entry %s", de->d_name);
		struct stat st;
		memset(&st, 0, sizeof(st));
		st.st_ino = de->d_ino;
		st.st_mode = de->d_type << 12;
		if (filler(buf, de->d_name, &st, 0))
			break;
	}

	closedir(dp);
	return (0);
}

/**
 * Read the content of the relations/ directory
 */
int tagsistant_readdir_on_relations(
	tagsistant_querytree *qtree,
	const char *path,
	void *buf,
	fuse_fill_dir_t filler,
	int *tagsistant_errno)
{
	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	struct tagsistant_use_filler_struct *ufs = g_new0(struct tagsistant_use_filler_struct, 1);
	if (ufs is NULL) {
		dbg('F', LOG_ERR, "Error allocating memory");
		*tagsistant_errno = EBADF;
		return (-1);
	}

	ufs->filler = filler;
	ufs->buf = buf;
	ufs->path = path;
	ufs->qtree = qtree;

	gchar *condition1 = NULL, *condition2 = NULL;

	if (qtree->second_tag || qtree->related_value) {
		// nothing
	} else if (qtree->related_key) {

		if (qtree->namespace && strlen(qtree->namespace))
			condition1 = g_strdup_printf("(tags1.tagname = \"%s\" and tags1.`key` = \"%s\" and tags1.value = \"%s\") ", qtree->namespace, qtree->key, qtree->value);
		else
			condition1 = g_strdup_printf("(tags1.tagname = \"%s\") ", qtree->first_tag);

		condition2 = g_strdup_printf("(tags2.tagname = \"%s\" and tags2.`key` = \"%s\") ", qtree->related_namespace, qtree->related_key);

		tagsistant_query(
			"select distinct tags2.value from tags as tags2 "
				"join relations on tags2.tag_id = relations.tag2_id "
				"join tags as tags1 on tags1.tag_id = relations.tag1_id "
				"where %s and %s and relation = \"%s\"",
			qtree->dbi,
			tagsistant_add_entry_to_dir,
			ufs,
			condition1,
			condition2,
			qtree->relation);

	} else if (qtree->related_namespace) {

		if (qtree->namespace && strlen(qtree->namespace))
			condition1 = g_strdup_printf("(tags1.tagname = \"%s\" and tags1.`key` = \"%s\" and tags1.value = \"%s\") ", qtree->namespace, qtree->key, qtree->value);
		else
			condition1 = g_strdup_printf("(tags1.tagname = \"%s\") ", qtree->first_tag);

		condition2 = g_strdup_printf("(tags2.tagname = \"%s\" ) ", qtree->related_namespace);

		tagsistant_query(
			"select distinct tags2.key from tags as tags2 "
				"join relations on tags2.tag_id = relations.tag2_id "
				"join tags as tags1 on tags1.tag_id = relations.tag1_id "
				"where %s and %s and relation = \"%s\"",
			qtree->dbi,
			tagsistant_add_entry_to_dir,
			ufs,
			condition1,
			condition2,
			qtree->relation);

	} else if (qtree->relation) {

		if (qtree->namespace && strlen(qtree->namespace))
			condition1 = g_strdup_printf("(tags1.tagname = \"%s\" and tags1.`key` = \"%s\" and tags1.value = \"%s\") ", qtree->namespace, qtree->key, qtree->value);
		else
			condition1 = g_strdup_printf("(tags1.tagname = \"%s\") ", qtree->first_tag);

		tagsistant_query(
			"select distinct tags2.tagname from tags as tags2 "
				"join relations on relations.tag2_id = tags2.tag_id "
				"join tags as tags1 on tags1.tag_id = relations.tag1_id "
				"where %s and relation = \"%s\"",
			qtree->dbi,
			tagsistant_add_entry_to_dir,
			ufs,
			condition1,
			qtree->relation);

	} else if (qtree->first_tag || qtree->value) {

		// list all relations
		filler(buf, "excludes", NULL, 0);
		filler(buf, "includes", NULL, 0);
		filler(buf, "is_equivalent", NULL, 0);
		filler(buf, "requires", NULL, 0);

	} else if (qtree->key) {

		tagsistant_query(
			"select distinct value from tags "
				"where tagname = \"%s\" and `key` = \"%s\"",
			qtree->dbi,
			tagsistant_add_entry_to_dir,
			ufs,
			qtree->namespace,
			qtree->key);

	} else if (qtree->namespace) {

		tagsistant_query(
			"select distinct `key` from tags "
				"where tagname = \"%s\"",
			qtree->dbi,
			tagsistant_add_entry_to_dir,
			ufs,
			qtree->namespace);

	} else {

		// list all tags
		tagsistant_query(
			"select distinct tagname from tags",
			qtree->dbi,
			tagsistant_add_entry_to_dir,
			ufs);

	}

	g_free_null(ufs);
	g_free_null(condition1);
	g_free_null(condition2);

	return (0);
}

/**
 * Read the content of the tags/ directory
 */
int tagsistant_readdir_on_tags(
	tagsistant_querytree *qtree,
	const char *path,
	void *buf,
	fuse_fill_dir_t filler,
	int *tagsistant_errno)
{
	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	struct tagsistant_use_filler_struct *ufs = g_new0(struct tagsistant_use_filler_struct, 1);
	if (ufs is NULL) {
		dbg('F', LOG_ERR, "Error allocating memory");
		*tagsistant_errno = EBADF;
		return (-1);
	}

	ufs->filler = filler;
	ufs->buf = buf;
	ufs->path = path;
	ufs->qtree = qtree;

	if (qtree->first_tag) {
		// nothing
	} else if (qtree->value && strlen(qtree->value)) {
		// nothing
	} else if (qtree->key && strlen(qtree->key)) {
		tagsistant_query("select distinct value from tags where tagname = \"%s\" and `key` = \"%s\"", qtree->dbi, tagsistant_add_entry_to_dir, ufs, qtree->namespace, qtree->key);
	} else if (qtree->namespace && strlen(qtree->namespace)) {
		tagsistant_query("select distinct `key` from tags where tagname = \"%s\"", qtree->dbi, tagsistant_add_entry_to_dir, ufs, qtree->namespace);
	} else {
		// list all tags
		tagsistant_query("select distinct tagname from tags", qtree->dbi, tagsistant_add_entry_to_dir, ufs);
	}

	g_free_null(ufs);
	return (0);
}

/**
 * Read the content of the stats/ directory
 */
int tagsistant_readdir_on_stats(
	tagsistant_querytree *qtree,
	const char *path,
	void *buf,
	fuse_fill_dir_t filler,
	int *tagsistant_errno)
{
	(void) path;
	(void) qtree;
	(void) tagsistant_errno;

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);
#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
	filler(buf, "cached_queries", NULL, 0);
#endif /* TAGSISTANT_ENABLE_QUERYTREE_CACHE */
	filler(buf, "configuration", NULL, 0);
	filler(buf, "connections", NULL, 0);
	filler(buf, "objects", NULL, 0);
	filler(buf, "relations", NULL, 0);
	filler(buf, "tags", NULL, 0);

	// fill with available statistics

	return (0);
}

/**
 * Read the content of the alias/ directory
 */
int tagsistant_readdir_on_alias(
	tagsistant_querytree *qtree,
	const char *path,
	void *buf,
	fuse_fill_dir_t filler,
	int *tagsistant_errno)
{
	(void) path;
	(void) qtree;
	(void) tagsistant_errno;

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	struct tagsistant_use_filler_struct *ufs = g_new0(struct tagsistant_use_filler_struct, 1);
	if (ufs is NULL) {
		dbg('F', LOG_ERR, "Error allocating memory");
		*tagsistant_errno = EBADF;
		return (-1);
	}

	ufs->filler = filler;
	ufs->buf = buf;
	ufs->path = path;
	ufs->qtree = qtree;

	tagsistant_query(
		"select alias from aliases",
		qtree->dbi,
		tagsistant_add_entry_to_dir,
		ufs,
		qtree->namespace,
		qtree->key);

	g_free(ufs);

	return (0);
}

/**
 * Read the content of the export/ directory
 */
int tagsistant_readdir_on_export(
	tagsistant_querytree *qtree,
	const char *path,
	void *buf,
	fuse_fill_dir_t filler,
	int *tagsistant_errno)
{
	/*
	 * list the object contents
	 */
	if (qtree->inode)
		return (tagsistant_readdir_on_object(qtree, path, buf, filler, tagsistant_errno));

	struct tagsistant_use_filler_struct *ufs = g_new0(struct tagsistant_use_filler_struct, 1);
	if (ufs is NULL) {
		dbg('F', LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		*tagsistant_errno = EBADF;
		return (-1);
	}

	ufs->filler = filler;
	ufs->buf = buf;
	ufs->path = path;
	ufs->qtree = qtree;

	/*
	 * list all the tags
	 */
	if (!qtree->last_tag) {
		filler(buf, ".", NULL, 0);
		filler(buf, "..", NULL, 0);

		tagsistant_query(
			"select tagname, `key`, `value` from tags where (`tagname` not like \"\%%s\") or (`value` <> \"\")",
			qtree->dbi, tagsistant_add_tag_to_export, ufs, tagsistant.namespace_suffix);
	} else {
		filler(buf, ".", NULL, 0);
		filler(buf, "..", NULL, 0);

		/*
		 * list all the objects linked by a tag with their inode prepended
		 */
		qtree->force_inode_in_filenames = 1;

		GError *error = NULL;
		GRegex *rx = g_regex_new("([^:]+:)([^=]+)=(.*)", G_REGEX_EXTENDED, 0, &error);

		if ((error is NULL) && (rx)) {
			GMatchInfo *match_info;
			g_regex_match(rx, qtree->last_tag, 0, &match_info);

			if (g_match_info_matches(match_info)) {
				gchar *namespace = g_match_info_fetch(match_info, 1);
				gchar *key = g_match_info_fetch(match_info, 2);
				gchar *value = g_match_info_fetch(match_info, 3);

				tagsistant_query(
					"select objectname, cast(objects.inode as char) from objects "
						"join tagging on tagging.inode = objects.inode "
						"join tags on tags.tag_id = tagging.tag_id "
						"where tagname = \"%s\" and `key` = \"%s\" and value = \"%s\"",
					qtree->dbi, tagsistant_add_entry_to_dir, ufs, namespace, key, value);

				g_free(namespace);
				g_free(key);
				g_free(value);
			} else {
				tagsistant_query(
					"select objectname, cast(objects.inode as char) from objects "
						"join tagging on tagging.inode = objects.inode "
						"join tags on tags.tag_id = tagging.tag_id "
						"where tagname = \"%s\"",
					qtree->dbi, tagsistant_add_entry_to_dir, ufs, qtree->last_tag);
			}

			g_match_info_free(match_info);
		}

		g_regex_unref(rx);
	}

	g_free(ufs);
	return (1);
}


/**
 * readdir equivalent (in FUSE paradigm)
 *
 * @param path the path of the directory to be read
 * @param buf buffer holding directory entries
 * @param filler libfuse fuse_fill_dir_t function to save entries in *buf
 * @param offset offset of next read
 * @param fi struct fuse_file_info passed by libfuse; unused.
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
	int res = 0, tagsistant_errno = 0;

	(void) fi;

	TAGSISTANT_START(OPS_IN "READDIR on %s", path);

	// build querytree
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 0, 1, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		TAGSISTANT_ABORT_OPERATION(ENOENT);

	} else if ((QTREE_POINTS_TO_OBJECT(qtree) && qtree->full_archive_path) || QTREE_IS_ARCHIVE(qtree)) {
		res = tagsistant_readdir_on_object(qtree, path, buf, filler, &tagsistant_errno);

	} else if (QTREE_IS_ROOT(qtree)) {

		/* insert pseudo directories: tags/ archive/ relations/ and stats/ */
		filler(buf, ".", NULL, 0);
		filler(buf, "..", NULL, 0);
		filler(buf, "alias", NULL, 0);
		filler(buf, "archive", NULL, 0);
		filler(buf, "export", NULL, 0);
		filler(buf, "relations", NULL, 0);
//		filler(buf, "retag", NULL, 0);
		filler(buf, "stats", NULL, 0);
		filler(buf, "store", NULL, 0);
		filler(buf, "tags", NULL, 0);

	} else if (QTREE_IS_STORE(qtree)) {
		res = tagsistant_readdir_on_store(qtree, path, buf, filler, offset, &tagsistant_errno);

	} else if (QTREE_IS_TAGS(qtree)) {
		res = tagsistant_readdir_on_tags(qtree, path, buf, filler, &tagsistant_errno);

	} else if (QTREE_IS_RELATIONS(qtree)) {
		res = tagsistant_readdir_on_relations(qtree, path, buf, filler, &tagsistant_errno);

	} else if (QTREE_IS_STATS(qtree)) {
		res = tagsistant_readdir_on_stats(qtree, path, buf, filler, &tagsistant_errno);

	} else if (QTREE_IS_ALIAS(qtree)) {
		res = tagsistant_readdir_on_alias(qtree, path, buf, filler, &tagsistant_errno);

	} else if (QTREE_IS_EXPORT(qtree)) {
		res = tagsistant_readdir_on_export(qtree, path, buf, filler, &tagsistant_errno);
	}

TAGSISTANT_EXIT_OPERATION:
	if ( res is -1 ) {
		TAGSISTANT_STOP_ERROR(OPS_OUT "READDIR on %s (%s): %d %d: %s", path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK(OPS_OUT "READDIR on %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}
