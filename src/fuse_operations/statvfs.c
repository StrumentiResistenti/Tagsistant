/*
   Tagsistant (tagfs) -- fuse_operations/statvfs.c
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

#if FUSE_USE_VERSION >= 25

/**
 * statvfs equivalent (used on fuse >= 25)
 *
 * @param path the path to be statvfs()ed
 * @param stbuf pointer to struct statvfs holding filesystem informations
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_statvfs(const char *path, struct statvfs *stbuf)
{
    int res = 0, tagsistant_errno = 0;
	(void) path;

	TAGSISTANT_START("STATVFS on %s", path);

	res = statvfs(tagsistant.repository, stbuf);
	tagsistant_errno = errno;

	if ( res is -1 ) {
		TAGSISTANT_STOP_ERROR("STATVFS on %s: %d %d: %s", path, res, tagsistant_errno, strerror(tagsistant_errno));
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK("STATVFS on %s: OK", path);
		return (0);
	}
}

#endif
