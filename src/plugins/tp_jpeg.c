/*
   Tagsistant (tagfs) -- tp_jpeg.c
   Copyright (C) 2006-2013 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant jpeg plugin which makes decisions on file MIME types.

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
 * set to 1 to return TP_STOP and avoid further processing
 */
#define EXCLUDE_OTHER_PLUGINS 0

/* declaring mime type */
char mime_type[] = "image/jpeg";

/* regex */
GRegex *rx = NULL;

/* exported init function */
int tagsistant_plugin_init()
{
	/* get the jpeg filter and prepare a regular expression */
	gchar *pattern = tagsistant_get_ini_entry("mime:image/jpeg", "filter");
	if (!pattern) pattern = "^(size|orientation)$";

	rx = g_regex_new(pattern, TAGSISTANT_RX_COMPILE_FLAGS, 0, NULL);

	return(1);
}

/* exported processor function */
int tagsistant_processor(tagsistant_querytree *qtree, tagsistant_keyword keywords[TAGSISTANT_MAX_KEYWORDS])
{
	/* default tagging */
	tagsistant_sql_tag_object(qtree->dbi, "image", NULL, NULL, qtree->inode);
	tagsistant_sql_tag_object(qtree->dbi, "image:", "format", "jpeg", qtree->inode);

	/* applying regular expression */
	tagsistant_plugin_iterator(qtree, "image:", keywords, rx);

	return(EXCLUDE_OTHER_PLUGINS ? TP_STOP : TP_OK);
}

/* exported finalize function */
void tagsistant_plugin_free()
{
	g_regex_unref(rx);
}

// vim:ts=4:autoindent:nocindent:syntax=c
