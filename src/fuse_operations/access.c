/*
   Tagsistant (tagfs) -- fuse_operations/getattr.c
   Copyright (C) 2006-20012 Tx0 <tx0@strumentiresistenti.org>

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
 * access() equivalent.
 *
 * @param path the path of the filename to be access()ed
 * \int mode the mode which is F_OK|R_OK|W_OK|X_OK
 * @return(0 on success, -errno on error)
 */
int tagsistant_access(const char *path, int mode)
{
	TAGSISTANT_START("ACCESS on %s [mode: %u]", path, mode);
	(void) mode;

	struct stat st;
	int res = tagsistant_getattr(path, &st);

	if (res == 0) {
		TAGSISTANT_STOP_OK("ACCESS on %s: OK", path);
		return(0);
	}

	TAGSISTANT_STOP_ERROR("ACCESS on %s: -1 %d: %s", path, EACCES, strerror(EACCES));
	return(-EACCES);
}
