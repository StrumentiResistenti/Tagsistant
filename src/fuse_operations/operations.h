/*
   Tagsistant (tagfs) -- fuse_operations/operations.h
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

#define TAGSISTANT_ABORT_OPERATION(set_errno) \
	{ res = -1; tagsistant_errno = set_errno; goto TAGSISTANT_EXIT_OPERATION; }

extern int tagsistant_getattr(const char *path, struct stat *stbuf);
extern int tagsistant_readlink(const char *path, char *buf, size_t size);
extern int tagsistant_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
extern int tagsistant_mknod(const char *path, mode_t mode, dev_t rdev);
extern int tagsistant_mkdir(const char *path, mode_t mode);
extern int tagsistant_unlink(const char *path);
extern int tagsistant_rmdir(const char *path);
extern int tagsistant_rename(const char *from, const char *to);
extern int tagsistant_symlink(const char *from, const char *to);
extern int tagsistant_link(const char *from, const char *to);
extern int tagsistant_chmod(const char *path, mode_t mode);
extern int tagsistant_chown(const char *path, uid_t uid, gid_t gid);
extern int tagsistant_truncate(const char *path, off_t size);
extern int tagsistant_utime(const char *path, struct utimbuf *buf);
extern int tagsistant_access(const char *path, int mode);
extern int tagsistant_open(const char *path, struct fuse_file_info *fi);
extern int tagsistant_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
extern int tagsistant_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
extern int tagsistant_flush(const char *path, struct fuse_file_info *fi);
extern int tagsistant_release(const char *path, struct fuse_file_info *fi);
extern int tagsistant_getxattr(const char *path, const char *name, char *value, size_t size);
extern int tagsistant_setxattr(const char *path, const char *name, const char *value, size_t size, int flags);
extern int tagsistant_listxattr(const char *path, char *list, size_t size);
extern int tagsistant_removexattr(const char *path, const char *name);

#define tagsistant_internal_open(qtree, flags, res, internal_errno) {\
	if ((!qtree) || (!qtree->full_archive_path)) {\
		dbg(LOG_ERR, "Null qtree or qtree->full_archive path");\
		res = -1;\
		internal_errno = EFAULT;\
	} else {\
		res = open(qtree->full_archive_path, flags);\
		internal_errno = errno;\
	}\
}

#if FUSE_USE_VERSION >= 25
	extern int tagsistant_statvfs(const char *path, struct statvfs *stbuf);
#else
	extern int tagsistant_statfs(const char *path, struct statfs *stbuf);
#endif
