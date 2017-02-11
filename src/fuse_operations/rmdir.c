/*
   Tagsistant (tagfs) -- fuse_operations/rmdir.c
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

void tagsistant_sql_delete_tag_proxy(
	dbi_conn dbi,
	const gchar *namespace,
	const gchar *key,
	const gchar *value,
	tagsistant_inode unused)
{
	(void) unused;

	tagsistant_sql_delete_tag(dbi, namespace, key, value);
}

/**
 * rmdir equivalent
 *
 * @param path the tag (directory) to be removed
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_rmdir(const char *path)
{
    int res = 0, tagsistant_errno = 0;
    gboolean dispose = TRUE;
	gchar *rmdir_path = NULL;

	TAGSISTANT_START(OPS_IN "RMDIR on %s", path);

	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 1, 1, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		TAGSISTANT_ABORT_OPERATION(ENOENT);
	}

	// -- store --
	// tags/delete_this_tag
	// tags/delete_this_tag/@/
	// tags/tag/@/delete_this_dir
	// tags/tag/@/dir/delete_this_dir
	//
	if (QTREE_IS_STORE(qtree)) {
		tagsistant_querytree_check_tagging_consistency(qtree);

		if (!QTREE_IS_COMPLETE(qtree)) {
			// -- tags but incomplete (means: delete a tag) --
			tagsistant_querytree_traverse(qtree, tagsistant_sql_delete_tag_proxy, 0);
			dispose = FALSE;
		} else if (QTREE_IS_TAGGABLE(qtree)) {
			/*
			 * if object is pointed by a tags/ query, then untag it
			 * from the tags included in the query path...
			 */
			tagsistant_querytree_traverse(qtree, tagsistant_sql_untag_object, qtree->inode);

#if TAGSISTANT_ENABLE_AND_SET_CACHE
			/*
			 * invalidate the and_set cache
			 */
			tagsistant_invalidate_and_set_cache_entries(qtree);
#endif

			/*
			 * ...then check if it's tagged elsewhere...
			 * ...if still tagged, then avoid real unlink(): the object must survive!
			 * ...otherwise we can delete it from the objects table
			 */
			dispose = tagsistant_dispose_object_if_untagged(qtree);
		}

		// do a real mkdir
		if (dispose) {
			rmdir_path = qtree->full_archive_path;
			res = rmdir(rmdir_path);
			tagsistant_errno = errno;

		}

		// clean the RDS library
		tagsistant_delete_rds_involved(qtree);
	}

	// -- relations --
	else if (QTREE_IS_RELATIONS(qtree)) {
		// rmdir can be used only on third level
		// since first level is all available tags
		// and second level is all available relations
		if (qtree->second_tag || qtree->related_namespace) {
			tagsistant_inode tag1_id = 0, tag2_id = 0;

			if (qtree->first_tag)
				tag1_id = tagsistant_sql_get_tag_id(qtree->dbi, qtree->first_tag, NULL, NULL);
			else
				tag1_id = tagsistant_sql_get_tag_id(qtree->dbi, qtree->namespace, qtree->key, qtree->value);

			if (qtree->second_tag)
				tag2_id = tagsistant_sql_get_tag_id(qtree->dbi, qtree->second_tag, NULL, NULL);
			else
				tag2_id = tagsistant_sql_get_tag_id(qtree->dbi, qtree->related_namespace, qtree->related_key, qtree->related_value);

			/*
			 * check tags and the relation
			 */
			if (!(tag1_id && tag2_id && IS_VALID_RELATION(qtree->relation))) {
				TAGSISTANT_ABORT_OPERATION(EFAULT);
			}

			if (qtree->second_tag || (qtree->related_namespace && qtree->related_key && qtree->related_value)) {
				tagsistant_query(
					"delete from relations where tag1_id = '%d' and tag2_id = '%d' and relation = '%s'",
					qtree->dbi, NULL, NULL, tag1_id, tag2_id, qtree->relation);

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
				// invalidate the cache entries which involves one of the tags related
				tagsistant_invalidate_querytree_cache(qtree);
#endif

				tagsistant_invalidate_reasoning_cache(qtree->first_tag);
				tagsistant_invalidate_reasoning_cache(qtree->second_tag);

				// clean the RDS library
				tagsistant_delete_rds_involved(qtree);
			}
		} else {
			TAGSISTANT_ABORT_OPERATION(EROFS);
		}
	}

	// -- tags --
	else if (QTREE_IS_TAGS(qtree)) {
		if (!qtree->first_tag && !qtree->namespace) {
			TAGSISTANT_ABORT_OPERATION(EROFS);
		}

		if (qtree->first_tag) {
			tagsistant_sql_delete_tag(qtree->dbi, qtree->first_tag, NULL, NULL);
			tagsistant_invalidate_reasoning_cache(qtree->first_tag);
		} else if (qtree->namespace) {
			tagsistant_sql_delete_tag(qtree->dbi, qtree->namespace, qtree->key, qtree->value);
			tagsistant_invalidate_reasoning_cache(qtree->namespace);
		}

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
		// invalidate the cache entries which involves one of the tags related
		tagsistant_invalidate_querytree_cache(qtree);
#endif

	}

	// -- archive --
	// -- stats --
	// -- alias --
	else TAGSISTANT_ABORT_OPERATION(EROFS);


TAGSISTANT_EXIT_OPERATION:
	if ( res is -1 ) {
		TAGSISTANT_STOP_ERROR(OPS_OUT "RMDIR on %s (%s): %d %d: %s", path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK(OPS_OUT "RMDIR on %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}
