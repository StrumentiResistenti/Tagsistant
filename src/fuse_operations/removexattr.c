/*
   Tagsistant (tagfs) -- fuse_operations/removexattr.c
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
 * set extended attributes
 *
 * @param path the path
 * @param stbuf pointer to struct stat buffer holding data about file
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_removexattr(const char *path, const char *name)
{
    int res = 0, tagsistant_errno = 0;

	TAGSISTANT_START("SETXATTR on %s", path);

	// build querytree
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 0, 1, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree))
		TAGSISTANT_ABORT_OPERATION(ENOENT);
	
	// -- error message --
	if (qtree->error_message && g_regex_match_simple("@/error$", path, G_REGEX_EXTENDED, 0)) {
		TAGSISTANT_ABORT_OPERATION(EFAULT);
	}

	// -- archive --
	else if (QTREE_IS_ARCHIVE(qtree)) {
		if (!g_regex_match_simple(TAGSISTANT_INODE_DELIMITER, qtree->object_path, 0, 0)) {
			res = lremovexattr(qtree->object_path, name);
			tagsistant_errno = errno;
		} else if (qtree->full_archive_path) {
			res = lremovexattr(qtree->full_archive_path, name);
			tagsistant_errno = errno;
		} else {
			TAGSISTANT_ABORT_OPERATION(ENOENT);
		}
	}

	// -- object on disk --
	else if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (qtree->full_archive_path) {
			res = lremovexattr(qtree->full_archive_path, name);
			tagsistant_errno = errno;
		} else {
			TAGSISTANT_ABORT_OPERATION(ENOENT);
		}
	}

	// -- alias --
	// -- relations --
	// -- stats --
	// -- store (incomplete) --
	// -- tags --
	// -- archive (the directory itself) --
	// -- root --
	else TAGSISTANT_ABORT_OPERATION(EFAULT);

TAGSISTANT_EXIT_OPERATION:

	if ( res is -1 ) {
		TAGSISTANT_STOP_ERROR("REMOVEXATTR on %s {%s}: %d %d: %s", path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK("REMOVEXATTR on %s {%s}: OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}
