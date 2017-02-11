/*
   Tagsistant (tagfs) -- fuse_operations/chown.c
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
 * chown equivalent
 *
 * @param path the path to be chown()ed
 * @param uid new UID for path
 * @param gid new GID for path
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_chown(const char *path, uid_t uid, gid_t gid)
{
    int res = 0, tagsistant_errno = 0;

	TAGSISTANT_START(OPS_IN "CHOWN on %s [uid: %d gid: %d]", path, uid, gid);

	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 0, 1, 1);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		TAGSISTANT_ABORT_OPERATION(ENOENT);
	}

	// -- object on disk --
	else if (QTREE_POINTS_TO_OBJECT(qtree)) {
		res = chown(qtree->full_archive_path, uid, gid);
		tagsistant_errno = errno;
	}

	// -- tags --
	// -- stats --
	// -- relations --
	// -- alias --
	else TAGSISTANT_ABORT_OPERATION(EROFS);

TAGSISTANT_EXIT_OPERATION:
	if ( res is -1 ) {
		TAGSISTANT_STOP_ERROR(OPS_OUT "CHMOD %s to %d,%d (%s): %d %d: %s", qtree->full_archive_path, uid, gid, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK(OPS_OUT "CHMOD %s, %d, %d (%s): OK", path, uid, gid, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}
