/*
   Tagsistant (tagfs) -- tp_filename_rx.c
   Copyright (C) 2015 Tx0 <tx0@strumentiresistnti.org>

   Tagsistant plugin to tag files with tags decoded from the filename.

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

/*
 * SAMPLE CONFIG from repository.ini:
 *
 * [filename_rx]
 * split=<yes|no|true|false|0|1>
 * splitter=.
 * filter=(DIRECTORSCUT|EXTENDED|UNCUT|1080p|720p|CUT|ENG|FR|DL|HD) => S:$1; (\d{4}) => M:time:year:$1; (\d{4})-(\d{2})-(\d{2}) => M:time:year:$1, M:time:month:$2, M:time:day:$3;
 *
 * EXPLANATION:
 *
 *   "split" controls filename splitting. If "yes", "true" or "1", the
 *   filename is splitted into one or more tokens by "splitter".
 *   If "no", "false" or "0", no splitting is done and the whole
 *   filename is used. If omitted, it defaults to 1.
 *
 *   "splitter" is a string used to split the filename in tokens.
 *   If omitted, it defaults to "."
 *
 *   "filter" describes a set of rules separated by ';'. Each rule
 *   is applied on each token created splitting the filename
 *   by the string declared in splitter. A rule has the form:
 *
 *     regexp => actions
 *
 *   where 'actions' is a list separated by ',' of actions. Each action
 *   starting by S generates a single tag. A prefix of M generates a
 *   machine tag. Another way to remember the distinction between S and M
 *   is thinking S for single tag and M for multiple tag.
 *
 *   The "regexp" pattern usually contains one or more pairs of parenthesis
 *   to delimit a part to be used in the tags. The first pair of
 *   parenthesis can be referred in the actions as $1, the second
 *   as $2 and so on.
 *
 *   That said, the three rules in the example mean:
 *
 *   (1)
 *   (DIRECTORSCUT|EXTENDED|UNCUT|1080p|720p|CUT|ENG|FR|DL|HD) => S:$1;
 *
 *     if the whole token matches one of the alternatives in the regexp,
 *     tag the file with a Single tag (the S: at the beginning of the action)
 *     corresponding at the matched text (the whole token in this case,
 *     represented by $1).
 *
 *   (2)
 *   (\d{4}) => M:time:year:$1;
 *
 *     if the token is a string of exactly four digits, apply a multiple
 *     tag (the M at the beginning) with namespace 'time', key 'year' and
 *     as value the whole token ($1).
 *
 *   (3)
 *   (\d{4})-(\d{2})-(\d{2}) => M:time:year:$1, M:time:month:$2, M:time:day:$3;
 *
 *     if the token matches, then tag the file with three multiple tags:
 *
 *       time:/year/eq/<the first match>
 *       time:/month/eq/<the second match>
 *       time:/day/eq/<the third match>
 *
 *     A more sophisticated alternative to the previous rule could be:
 *
 *   (3bis)
 *   ((\d{4})-(\d{2})-(\d{2})) => M:time:date:$1; M:time:year:$2, M:time:month:$3, M:time:day:$4;
 *
 *     here the first pair of parenthesis wraps the whole token, while
 *     the pairs from 2 to 4 only include parts. On a token like
 *     "2014-11-24", the rule will apply four tags:
 *
 *       time:/date/eq/2014-11-24/
 *       time:/year/eq/2014/
 *       time:/month/eq/11/
 *       time:/day/eq/24/
 *
 * NOTE: By prefixing each action by M: or S:, single and machine tags
 * can be intermixed in the same action.
 *
 * NOTE: The same $N placeholder can be used in more than one action.
 *
 * An example to explain both notes:
 *
 *   (1080p|720p) => S:res-$1; M:video:resolution:$1;
 *
 *     A filename with 720p in its name would receive both:
 *
 *       res-720p/
 *       video:/resolution/eq/720p/
 *
 * Some more examples.
 *
 * Example 1: the filename Stargate.DIRECTORSCUT.1080p.DL.1994.mkv
 * would receive the tags:
 *
 *   (1) DIRECTORSCUT/
 *   (1) 1080p/
 *   (1) DL/
 *   (2) time:/year/eq/1994/
 * 
 * The numbers in parenthesis refer to the rule that generated the tags.
 *
 * Example 2: the filename Venice.HD.2012-07-21.jpg would receive the tags:
 *
 *   (1) HD/
 *   (3) time:/year/eq/2012/
 *   (3) time:/month/eq/07/
 *   (3) time:/day/eq/21/
 * 
 * The filter syntax can be expressed in BNF as:
 *
 *      filter ::= {<rule>;}
 *        rule ::= <pattern> "=>" <actions>
 *     pattern ::= "valid regular expression"
 *     actions ::= {<action>,}
 *      action ::= <single-tag> | <machine-tag>
 *  single-tag ::= "S:" {"text" | <placeholder>}
 * machine-tag ::= "M:" {"text" | <placeholder>} ":" {"text" | <placeholder>} ":" {"text" | <placeholder>}
 * placeholder ::= "$" <digit>
 *       digit ::= 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9
 *
 */

#include "../tagsistant.h"

/*
 * declaring mime type
 */
char mime_type[] = "*/*";

/**
 * Describes an action to be taken if its pattern matches
 */
typedef struct rule_action {
	struct rule_action *next;
	gchar *format;
} rule_action;

/**
 * Describes a rule, formed by a pattern (translated into a regexp)
 * and a chain of actions
 */
typedef struct plugin_rule {
	struct plugin_rule *next;

	/* the pattern with its compiled regular expression */
	gchar *pattern;
	GRegex *rx;

	/* the chain of actions */
	rule_action *actions;
} plugin_rule;

/**
 * The rule chain
 */
plugin_rule *plugin_rules = NULL;

/*
 * Define some constant with string splitters
 */
const gchar rule_splitter[] = "\\s*;\\s*";
const gchar pattern_actions_splitter[] = "\\s*=>\\s*";
const gchar action_splitter[] = "\\s*,\\s*";

/*
 * Is the plugin enabled and ready to tag files?
 */
gboolean plugin_enabled = FALSE;

/*
 * The plugin pattern
 */
gchar *pattern = NULL;

/*
 * The token splitter
 */
gchar *splitter = NULL;

/*
 * Split filenames?
 */
int split = 1;

/*
 * This regular expression is used to identify the value place holders inside the
 * rules. A placeholder has the format $N where N is a number between 0 and 9.
 */
GRegex *value_replacer_rx = NULL;

/*
 * Initialize the plugin
 */
int tagsistant_plugin_init()
{
	gchar *tmp_split;

	/* get the config parameters from the .ini file */
	pattern   = tagsistant_get_ini_entry("filename_rx", "filter");
	splitter  = tagsistant_get_ini_entry("filename_rx", "splitter");
	tmp_split = tagsistant_get_ini_entry("filename_rx", "split");

	/* check if filename splitting is requested */
	if (g_regex_match_simple("^(no|false|0)$", tmp_split, G_REGEX_CASELESS, 0)) split = 0;
	g_free_null(tmp_split);

	/* return without initializing the plugin */
	if (!pattern || !strlen(pattern)) return(0);

	/* set a default token splitter */
	if (!splitter || !strlen(splitter)) splitter = g_strdup(".");

	/* split the filter into rules */
	gchar **rules = g_regex_split_simple(rule_splitter, pattern, G_REGEX_EXTENDED, 0);
	gchar **rule = rules;

	plugin_rule *tmp_rule = NULL;

	while (*rule) {
		gchar **pattern_and_actions = g_regex_split_simple(pattern_actions_splitter, *rule, G_REGEX_EXTENDED, 0);
		gchar *pattern = pattern_and_actions[0];

		if (pattern && strlen(pattern)) {
			/* initialize a new rule */
			plugin_rule *prule = g_new0(plugin_rule, 1);

			/* save the pattern and generate the GRegex object */
			prule->pattern = g_strdup(pattern);
			GError *err = NULL;
			prule->rx = g_regex_new(pattern, G_REGEX_CASELESS|G_REGEX_EXTENDED|G_REGEX_ANCHORED|G_REGEX_OPTIMIZE, 0, &err);
			if (err) {
				g_free(prule->pattern);
				g_free(prule);
				continue;
			}

			/* split the actions */
			gchar **actions = g_regex_split_simple(action_splitter, pattern_and_actions[1], G_REGEX_EXTENDED, 0);
			gchar **action = actions;
			rule_action *last_saved_action = NULL;

			while (*action) {
				/* save each action */
				rule_action *raction = g_new0(rule_action, 1);
				raction->format = g_strdup(*action);

				/* link the action to the rule action list */
				if (last_saved_action) {
					last_saved_action->next = raction;
				} else {
					prule->actions = raction;
				}
				last_saved_action = raction;

				action++;
			}

			g_strfreev(actions);

			if (!plugin_rules) {
				tmp_rule = plugin_rules = prule;
			} else {
				tmp_rule->next = prule;
				tmp_rule = prule;
			}
		}

		rule++;
		g_strfreev(pattern_and_actions);
	}
	g_strfreev(rules);

	/*
	 * Init the value-replacing regular expression used inside apply_rules()
	 */
	GError *err = NULL;
	value_replacer_rx = g_regex_new("([^\\$]*)\\$([0-9])(.*)", G_REGEX_CASELESS|G_REGEX_EXTENDED|G_REGEX_OPTIMIZE, 0, &err);
	if (err) return (0);

	/*
	 * The plugin is ready
	 */
	plugin_enabled = TRUE;

	return(1);
}

/**
 * A callback called by g_regex_replace_eval() inside apply_rules().
 * Replaces every placeholder ($N) with the corresponding parameter extracted by
 *
 * @param info GMatchInfo object to access regexp placeholders
 * @param result a GString object to fill with the resulting string
 * @param data a generic pointer that will be casted to a GMatchInfo
 *   object to access the rule expression placeholders
 * @return always FALSE
 */
gboolean apply_rules_callback(const GMatchInfo *info, GString *result, gpointer data)
{
	GMatchInfo *rule_info = (GMatchInfo *) data;

	/*
	 * Get the parts of the rule pattern, that is the placeholder
	 * ($N) and the substrings on its left and its right.
	 */
	gchar *left = g_match_info_fetch(info, 1);
	gchar *placeholder = g_match_info_fetch(info, 2);
	gchar *right = g_match_info_fetch(info, 3);

	/*
	 * Convert the placeholder number into an int, then fetch from
	 * the rule GMatchInfo the corresponding value
	 */
	int placeholder_number = atoi(placeholder);
	gchar *rule_value = g_match_info_fetch(rule_info, placeholder_number);

	/*
	 * Append to the result GString the left substring, the value
	 * from the rule GMatchInfo and finally the right substring
	 */
	g_string_append_printf(result, "%s%s%s", left, rule_value, right);

	/*
	 * Free and return
	 */
	g_free(left);
	g_free(placeholder);
	g_free(right);
	g_free(rule_value);

	return (FALSE);
}

/**
 * Called on every token, applies all the rules defined by the "filter" ini parameter
 *
 * @param token the token to be matched
 * @param qtree the tagsistant_querytree object to be tagged if token matches
 */
void apply_rules(const gchar *token, const tagsistant_querytree *qtree)
{
	plugin_rule *rule = plugin_rules;

	while (rule) {
		GMatchInfo *info;
		if (g_regex_match(rule->rx, token, 0, &info)) {
			/*
			 * The rule pattern matched. We must take every action related.
			 */
			rule_action *action = rule->actions;
			while (action) {
				/*
				 * Use g_regex_replace_eval() to replace all the place holders
				 * in the format with corresponding matching values
				 */
				GError *err = NULL;
				gchar *format = g_regex_replace_eval(value_replacer_rx, action->format,
					-1, 0, 0, apply_rules_callback, (gpointer) info, &err);

				/*
				 * Split the format by ":"
				 */
				gchar **format_elements = g_strsplit(format, ":", 4);

				if (strcmp("S", format_elements[0]) == 0) {

					/*
					 * simple tag
					 */
					if (format_elements[1] && strlen(format_elements[1])) {
						tagsistant_sql_tag_object(qtree->dbi, format_elements[1], NULL, NULL, qtree->inode);
					} else {
						dbg('p', LOG_ERR, "Wrong action %s on rule %s: single tag not defined",
							action->format, rule->pattern);
					}

				} else if (strcmp("M", format_elements[0]) == 0) {

					/*
					 * machine tag
					 */
					if (
						format_elements[1] && strlen(format_elements[1]) &&
						format_elements[2] && strlen(format_elements[2]) &&
						format_elements[3] && strlen(format_elements[3])
					) {
						gchar *namespace = g_strdup_printf("%s:", format_elements[1]);

						tagsistant_sql_tag_object(
							qtree->dbi,
							namespace,
							format_elements[2],
							format_elements[3],
							qtree->inode);

						g_free(namespace);
					} else {
						dbg('p', LOG_ERR, "Wrong action %s on rule %s: machine tag wrongly or not defined",
							action->format, rule->pattern);
					}

				} else {
					dbg('p', LOG_ERR, "Wrong action %s on rule %s: unknown tag type %s",
						action->format, rule->pattern, format_elements[0]);
				}
				g_strfreev(format_elements);

				g_free(format);
				action = action->next;
			}
		}
		g_match_info_free(info);
		rule = rule->next;
	}
}

/*
 * plugin kernel
 */
int tagsistant_processor(tagsistant_querytree *qtree, tagsistant_keyword keywords[TAGSISTANT_MAX_KEYWORDS])
{
	/*
	 * Since this plugin uses the filaname as source for tags,
	 * the keywords array is not used
	 */
	(void) keywords;

	/*
	 * Do not process if the plugin has not been properly initialized
	 */
	if (!plugin_enabled) return (TP_NULL);

	dbg('p', LOG_INFO, "Using tp_filename_rx on %s", qtree->object_path);

	/*
	 * Split the filename into tokens by splitter, if requested.
	 *
	 * The second call to g_strsplit() uses a patter which is unlikely
	 * to appear in any filename, and sets the max number of tokens
	 * to 1. Hence, the remainder of the string after the first token is
	 * appended to the last token, which is the first again, returning
	 * the whole filename as a single token.
	 */
	gchar **tokens = (split)
		? g_strsplit(qtree->object_path, splitter, 0)
		: g_strsplit(qtree->object_path, "/\t\n\r/\t\n\r/\t\n\r", 1);

	/*
	 * match each rule on each token
	 */
	gchar **token = tokens;
	while (*token) {
		apply_rules(*token, qtree);
		token++;
	}

	/*
	 * free the token array
	 */
	g_strfreev(tokens);

	return(TP_OK);
}

/* 
 * Clean and finalize. Mainly for memory leaks hunting, since plugins
 * are freed only when Tagsistant is being shutdown and their structures
 * will be destroyed anyway.
 */
void tagsistant_plugin_free()
{
	g_free_null(pattern);
	g_free_null(splitter);

	g_regex_unref(value_replacer_rx);

	plugin_rule *rule = plugin_rules;
	while (rule) {
		plugin_rule *tmp = rule;
		rule = rule->next;

		rule_action *action = tmp->actions;
		while (action) {
			rule_action *atmp = action;
			action = action->next;

			g_free_null(atmp->format);
			g_free_null(atmp);
		}

		g_free_null(tmp->pattern);
		g_regex_unref(tmp->rx);
		g_free_null(tmp);
	}
}
