/*
   tagfs -- debug_free_calls.h
   Copyright (C) 2006 Tx0 <tx0@autistici.org>

   Some debug code to catch free() related errors.

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

#ifdef _DEBUG_FREE_CALLS
#ifndef _DEBUG_FREE_CALLS_DEFINED
#define _DEBUG_FREE_CALLS_DEFINED 1

typedef struct unfreeable {
	void *address;
	struct unfreeable *next;
} unfreeable_t;

unfreeable_t *freeblock;

#define unfreeable(symbol) {\
	unfreeable_t *uf = freeblock;\
	if (uf != NULL)\
		while (uf->next != NULL)\
			uf = uf->next;\
	uf = g_malloc(sizeof(struct unfreeable));\
	if (uf != NULL) {\
		uf->next = NULL;\
		uf->address = (void *) symbol;\
	} else {\
		dbg(LOG_ERR, "Can't allocate new unfreeable_t item for %s", __STRING(symbol));\
	}\
}

#define free(symbol) {\
	unfreeable_t *uf = freeblock;\
	while (uf != NULL) {\
		if (uf->address == symbol) {\
			dbg(LOG_INFO, "Trying to free(%s), which is marked unfreeable!", __STRING(symbol));\
			break;\
		}\
		uf = uf->next;\
	}\
	dbg(LOG_INFO, "free(%s)", __STRING(symbol));\
	assert(symbol != NULL);\
	free(symbol);\
}

#endif /* _DEBUG_FREE_CALLS_DEFINED */
#else /* _DEBUG_FREE_CALLS */

#define unfreeable(symbol)

#endif /* _DEBUG_FREE_CALLS */

// vim:ts=4
