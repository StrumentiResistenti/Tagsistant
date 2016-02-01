/*
   Tagsistant (tagfs) -- debug.h
   Copyright (C) 2006 Tx0 <tx0@strumentiresistenti.org>

   Some debug code.

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

#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <syslog.h>

#undef _DEBUG_STDERR

#ifdef _DEBUG_SYSLOG
#	define dbg(family, facility, string, ...) \
		{ if (!tagsistant.quiet && tagsistant.dbg[family]) syslog(facility, string " [@%s:%d]", ##__VA_ARGS__, __FILE__, __LINE__); }
#else
#	define dbg(family, facility,string, ...) \
		{ if (!tagsistant.quiet && tagsistant.dbg[family]) fprintf(stderr,"TS> " string " [@%s:%d]\n", ##__VA_ARGS__, __FILE__, __LINE__); }
#endif

#define strlen(string) ((string is NULL) ? 0 : strlen(string))

#if 0
#define tagsistant_dirty_logging(statement) {\
	int fd = open("/tmp/tagsistant.sql", O_APPEND|O_CREAT|O_WRONLY, S_IRUSR|S_IWUSR);\
	if (fd isNot -1) {\
		int count = write(fd, statement, strlen(statement));\
		count = write(fd, "\n", 1);\
		(void) count;\
		close(fd);\
	}\
}
#else
#define tagsistant_dirty_logging(statement) {}
#endif
