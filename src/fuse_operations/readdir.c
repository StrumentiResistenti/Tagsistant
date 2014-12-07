/*
   Tagsistant (tagfs) -- fuse_operations/readdir.c
   Copyright (C) 2006-2013 Tx0 <tx0@strumentiresistenti.org>

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

// filetree functions
GHashTable *tagsistant_fileset_new(qtree_or_node *query, dbi_conn conn, int is_all_path);
void tagsistant_fileset_destroy_value_list(gchar *key, GList *list_p, gpointer data);
#define tagsistant_filetree_destroy_value g_free_null

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
	if (dir == NULL) return(0);

	/*
	 * zero-length values can be returned while listing triple tags
	 * we must suppress them, but returning 1, to prevent the callback
	 * from exiting its cycle
	 */
	if (strlen(dir) == 0) return (1);

	/* check if this tag has been already listed inside the path */
	qtree_or_node *ptx = ufs->qtree->tree;
	if (ptx) {
		while (NULL != ptx->next) ptx = ptx->next; // last OR section

		qtree_and_node *and_t = ptx->and_set;
		while (NULL != and_t) {
			if (g_strcmp0(and_t->tag, dir) == 0) {
				return(0);
			}
			and_t = and_t->next;
		}
	}

	// add the entry as is if it's not an alias
	if (!ufs->is_alias) return(ufs->filler(ufs->buf, dir, NULL, 0));

	// prepend the alias identified otherwise
	gchar *entry = g_strdup_printf("=%s", dir);
	int filler_result = ufs->filler(ufs->buf, entry, NULL, 0);
	g_free(entry);

	return (filler_result);
}

/**
 * Add a file entry from a GList to the readdir() buffer
 *
 * @param name unused
 * @param fh_list the GList holding filenames
 * @param ufs a context structure
 * @return 0 always
 */
static int tagsistant_readdir_on_store_filler(gchar *name, GList *fh_list, struct tagsistant_use_filler_struct *ufs)
{
	(void) name;

	if (!fh_list) return (0);

	if (NULL == fh_list) return (0);

	if (!(fh_list->next)) {
		// just add the filename
		tagsistant_file_handle *fh = fh_list->data;

		if (!fh) return (0);

		if (ufs->qtree->force_inode_in_filenames) {
			gchar *filename = g_strdup_printf("%d%s%s", fh->inode, TAGSISTANT_INODE_DELIMITER, fh->name);
			ufs->filler(ufs->buf, filename, NULL, 0);
			g_free_null(filename);
		} else {
			ufs->filler(ufs->buf, fh->name, NULL, 0);
		}

		return (0);
	}

	// add inodes to filenames
	while (fh_list) {
		tagsistant_file_handle *fh = fh_list->data;
		if (fh) {
			gchar *filename = g_strdup_printf("%d%s%s", fh->inode, TAGSISTANT_INODE_DELIMITER, fh->name);
			ufs->filler(ufs->buf, filename, NULL, 0);
			g_free_null(filename);
		}
		fh_list = fh_list->next;
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
			/* report a file with the error message */
			filler(buf, "error", NULL, 0);
		} else {
			/* build the filetree */
			GHashTable *hash_table = tagsistant_fileset_new(qtree->tree, qtree->dbi, is_all_path);
			g_hash_table_foreach(hash_table, (GHFunc) tagsistant_readdir_on_store_filler, ufs);
			g_hash_table_foreach(hash_table, (GHFunc) tagsistant_fileset_destroy_value_list, NULL);
			g_hash_table_destroy(hash_table);
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

		} else if (qtree->value) {
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

		} else if (qtree->key) {
			filler(buf, TAGSISTANT_EQUALS_TO_OPERATOR, NULL, 0);
			filler(buf, TAGSISTANT_CONTAINS_OPERATOR, NULL, 0);
			filler(buf, TAGSISTANT_GREATER_THAN_OPERATOR, NULL, 0);
			filler(buf, TAGSISTANT_SMALLER_THAN_OPERATOR, NULL, 0);

		} else if (qtree->namespace) {
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
	if (NULL == dp) {
		*tagsistant_errno = errno;
		dbg('F', LOG_ERR, "Unable to readdir(%s)", qtree->full_archive_path);
		return (-1);
	}

	struct dirent *de;
	while ((de = readdir(dp)) != NULL) {
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
	if (ufs == NULL) {
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

		if (qtree->namespace)
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

		if (qtree->namespace)
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

		if (qtree->namespace)
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
	if (ufs == NULL) {
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
	} else if (qtree->value) {
		// nothing
	} else if (qtree->key) {
		tagsistant_query("select distinct value from tags where tagname = \"%s\" and `key` = \"%s\"", qtree->dbi, tagsistant_add_entry_to_dir, ufs, qtree->namespace, qtree->key);
	} else if (qtree->namespace) {
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
	if (ufs == NULL) {
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

	TAGSISTANT_START("READDIR on %s", path);

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

	} else if (QTREE_IS_STATS(qtree) || QTREE_IS_RETAG(qtree)) {
		res = tagsistant_readdir_on_stats(qtree, path, buf, filler, &tagsistant_errno);

	} else if (QTREE_IS_ALIAS(qtree)) {
		res = tagsistant_readdir_on_alias(qtree, path, buf, filler, &tagsistant_errno);

	}

TAGSISTANT_EXIT_OPERATION:
	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("READDIR on %s (%s): %d %d: %s", path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK("READDIR on %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}

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
 * @param is_all_path is true when the path includes the ALL/ tag
 * @return a pointer to a GHashTable of tagsistant_file_handle objects
 */
GHashTable *tagsistant_fileset_new(qtree_or_node *query, dbi_conn conn, int is_all_path)
{
	qtree_or_node *query_dup = query;

	/*
	 * If the query contains the ALL meta-tag, just select all the available
	 * objects and return them
	 */
	if (is_all_path) {
		GHashTable *file_hash = g_hash_table_new(g_str_hash, g_str_equal);
		tagsistant_query("select objectname, inode from objects", conn, tagsistant_add_to_fileset, file_hash);
		return(file_hash);
	}

	/*
	 * a NULL query can't be processed
	 */
	if (!query) {
		dbg('f', LOG_ERR, "NULL path_tree_t object provided to %s", __func__);
		return(NULL);
	}

	/*
	 * PHASE 1.
	 * Build a set of temporary tables containing all the matched objects
	 *
	 * Step 1.1. for each qtree_or_node build a temporary table
	 */
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

		qtree_and_node *related = query->and_set->related;
		while (related) {
			g_string_append(create_base_table, " or ");
			tagsistant_query_add_and_set(create_base_table, related);
			related = related->related;
		}

		/*
		 * create the table and dispose the statement GString
		 */
		tagsistant_query(create_base_table->str, conn, NULL, NULL);
		g_string_free(create_base_table, TRUE);

		/*
		 * Step 1.2.
		 * for each ->next linked node, subtract from the base table
		 * the objects not matching this node
		 */
		qtree_and_node *next = query->and_set->next;
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
			tagsistant_query(cross_tag->str, conn, NULL, NULL);
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
				tagsistant_query(cross_tag->str, conn, NULL, NULL);
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
	 * format the main statement which reads from the temporary
	 * tables using UNION and ordering the files
	 */
	GString *view_statement = g_string_sized_new(10240);

	// g_string_append(view_statement, "select q.inode, q.objectname from ("); // does not work

	query = query_dup;
	while (query) {
		g_string_append_printf(view_statement, "select objectname, inode from tv%.16" PRIxPTR, (uintptr_t) query);
		if (query->next) g_string_append(view_statement, " union ");
		query = query->next;
	}

	// g_string_append(view_statement, ") q order by q.objectname"); // does not work

	/*
	 * apply view statement
	 */
	GHashTable *file_hash = g_hash_table_new(g_str_hash, g_str_equal);
	tagsistant_query(view_statement->str, conn, tagsistant_add_to_fileset, file_hash);

	/*
	 * free the SQL statement
	 */
	g_string_free(view_statement, TRUE);

	/*
	 * PHASE 3.
	 *
	 * drop the temporary tables
	 */
	while (query_dup) {
		tagsistant_query("drop table tv%.16" PRIxPTR, conn, NULL, NULL, (uintptr_t) query_dup);
		query_dup = query_dup->next;
	}

	return(file_hash);
}

/**
 * Destroy a filetree element GList list of tagsistant_file_handle.
 * This will free the GList data structure by first calling
 * tagsistant_filetree_destroy_value() on each linked node.
 */
void tagsistant_fileset_destroy_value_list(gchar *key, GList *list, gpointer data)
{
	(void) data;

	g_free_null(key);

	if (list) g_list_free_full(list, (GDestroyNotify) g_free /* was tagsistant_filetree_destroy_value */);
}
