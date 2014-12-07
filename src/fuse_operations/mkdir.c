/*
   Tagsistant (tagfs) -- fuse_operations/mkdir.c
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
 * mkdir equivalent
 *
 * @param path the path of the directory to be created
 * @param mode directory permissions (unused, since directories are tags saved in SQL backend)
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_mkdir(const char *path, mode_t mode)
{
	(void) mode;
    int res = 0, tagsistant_errno = 0;

	TAGSISTANT_START("MKDIR on %s [mode: %d]", path, mode);

	// build querytree
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 1, 1, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		TAGSISTANT_ABORT_OPERATION(EROFS);
	}

	// -- tags --
	// -- archive
	else if (QTREE_POINTS_TO_OBJECT(qtree)) {
		tagsistant_querytree_check_tagging_consistency(qtree);

		if (QTREE_IS_TAGGABLE(qtree)) {
			// create a new directory inside tagsistant.archive directory
			// and tag it with all the tags in the qtree
			res = tagsistant_force_create_and_tag_object(qtree, &tagsistant_errno);
			if (-1 == res) goto TAGSISTANT_EXIT_OPERATION;
		}

		// do a real mkdir
		res = mkdir(qtree->full_archive_path, mode);
		tagsistant_errno = errno;
	}

	// -- tags --
	else if (QTREE_IS_TAGS(qtree)) {
		if (qtree->first_tag) {
			if (qtree->second_tag) TAGSISTANT_ABORT_OPERATION(EROFS);
			tagsistant_sql_create_tag(qtree->dbi, qtree->first_tag, NULL, NULL);
		} else if (qtree->namespace) {
			tagsistant_sql_create_tag(qtree->dbi, qtree->namespace, qtree->key, qtree->value);
		}
	}

	// -- store but incomplete (means: create a new tag) --
	else if (QTREE_IS_STORE(qtree)) {
		if (qtree->namespace) {
			tagsistant_sql_create_tag(qtree->dbi, qtree->namespace, qtree->key, qtree->value);
		} else if (qtree->last_tag) {
			tagsistant_sql_create_tag(qtree->dbi, qtree->last_tag, NULL, NULL);
		}
	}

	// -- relations --
	else if (QTREE_IS_RELATIONS(qtree)) {
		/*
		 * mkdir can be used only on third level since the first level includes
		 * all the available tags and the second level includes all the available relations
		 */
		if (qtree->second_tag || qtree->related_namespace) {
			tagsistant_inode tag1_id = 0, tag2_id = 0;

			/*
			 * get first tag id
			 */
			if (qtree->first_tag)
				tag1_id = tagsistant_sql_get_tag_id(qtree->dbi, qtree->first_tag, NULL, NULL);
			else
				tag1_id = tagsistant_sql_get_tag_id(qtree->dbi, qtree->namespace, qtree->key, qtree->value);

			/*
			 * get second tag id (create it if not exists) <----------------------------
			 */
			if (qtree->second_tag) {
				tag2_id = tagsistant_sql_get_tag_id(qtree->dbi, qtree->second_tag, NULL, NULL);
				if (!tag2_id) {
					tagsistant_sql_create_tag(qtree->dbi, qtree->second_tag, NULL, NULL);
					tag2_id = tagsistant_sql_get_tag_id(qtree->dbi, qtree->second_tag, NULL, NULL);
				}
			} else {
				tag2_id = tagsistant_sql_get_tag_id(qtree->dbi, qtree->related_namespace, qtree->related_key, qtree->related_value);
				if (!tag2_id) {
					tagsistant_sql_create_tag(qtree->dbi, qtree->related_namespace, qtree->related_key, qtree->related_value);
					tag2_id = tagsistant_sql_get_tag_id(qtree->dbi, qtree->related_namespace, qtree->related_key, qtree->related_value);
				}
			}

			/*
			 * check tags and the relation
			 */
			if (!(tag1_id && tag2_id && IS_VALID_RELATION(qtree->relation))) {
				TAGSISTANT_ABORT_OPERATION(EFAULT);
			}

			if (qtree->second_tag || (qtree->related_namespace && qtree->related_key && qtree->related_value)) {
				tagsistant_query(
					"insert into relations (tag1_id, tag2_id, relation) values (%d, %d, '%s')",
					qtree->dbi, NULL, NULL, tag1_id, tag2_id, qtree->relation);

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
				// invalidate the cache entries which involves one of the tags related
				tagsistant_invalidate_querytree_cache(qtree);
#endif

				tagsistant_invalidate_reasoning_cache(qtree->first_tag ? qtree->first_tag : qtree->namespace);
				tagsistant_invalidate_reasoning_cache(qtree->second_tag ? qtree->second_tag : qtree->related_namespace);
			}
		} else {
			TAGSISTANT_ABORT_OPERATION(EROFS);
		}
	}

	// -- stats --
	// -- alias --
	else TAGSISTANT_ABORT_OPERATION(EROFS);

TAGSISTANT_EXIT_OPERATION:
	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("MKDIR on %s (%s): %d %d: %s", path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK("MKDIR on %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}

