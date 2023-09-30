#include <stdio.h>
#include <string.h>

#include <sqlite3.h>

typedef struct globals {
    unsigned int page_size;

    char const *src_path;
    char const *dst_path;

    sqlite3 *db;
} globals;

static globals const default_globals =
{
    0,

    NULL,
    NULL,

    NULL
};

/*
  When reviewing this code, open unpacking.sql
  and read it in parallel with this file.
*/

#include "unpacking.h"

static char const oom_msg[] =
    "Out of memory or something\n";

/*
  Create the destination database;
  attach the source database using the identifier "source";
  set the page size;
  start a transaction.
*/

static int open_db(
    globals *g)
{
    sqlite3 *db=NULL;
    sqlite3_stmt *attach=NULL;
    char *set_page_size_sql=NULL;
    char *errmsg=NULL;
    int status;

    status=sqlite3_open_v2(
        g->dst_path,&db,SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,NULL);
    if (status!=SQLITE_OK) {
        if (db) {
            fprintf(stderr,"%s: sqlite3_open: %s\n",
                    g->dst_path,sqlite3_errmsg(db));
        } else {
            fprintf(stderr,"%s: sqlite3_open: %s\n",
                    g->dst_path,sqlite3_errstr(status));
        }
        return -1;
    }

    status=sqlite3_prepare_v2(db,attach_sql,sizeof attach_sql,&attach,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(attach): %s\n",sqlite3_errmsg(db));
        return -1;
    }
    status=sqlite3_bind_text(attach,1,g->src_path,-1,SQLITE_STATIC);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_bind_text(attach): %s\n",sqlite3_errmsg(db));
        return -1;
    }
    status=sqlite3_step(attach);
    if (status!=SQLITE_DONE) {
        fprintf(stderr,"sqlite3_step(attach): %s\n",sqlite3_errmsg(db));
        return -1;
    }
    sqlite3_finalize(attach);
    attach=NULL;

    if (!g->page_size) {
        sqlite3_stmt *get_page_size=NULL;

        status=sqlite3_prepare_v2(
            db,get_page_size_sql,sizeof get_page_size_sql,&get_page_size,NULL);
        if (status!=SQLITE_OK) {
            fprintf(stderr,"sqlite3_prepapre(get_page_size): %s\n",
                    sqlite3_errmsg(db));
            return -1;
        }
        status=sqlite3_step(get_page_size);
        if (status!=SQLITE_ROW) {
            fprintf(stderr,"sqlite3_step(page_size): %s\n",
                    sqlite3_errmsg(db));
            return -1;
        }
        g->page_size=sqlite3_column_int(get_page_size,0);
        sqlite3_finalize(get_page_size);
    }

    set_page_size_sql=sqlite3_mprintf(set_page_size_fmt,g->page_size);
    if (!set_page_size_sql) {
        fputs(oom_msg,stderr);
        return -1;
    }
    status=sqlite3_exec(db,set_page_size_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"Failed to set page size: %s\n",errmsg);
        return -1;
    }
    sqlite3_free(set_page_size_sql);

    status=sqlite3_exec(db,begin_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"Failed to start transaction: %s\n",errmsg);
        return -1;
    }

    g->db=db;
    return 0;
}

static int transfer_data(
    globals *g)
{
    sqlite3_stmt *extract=NULL;
    sqlite3_stmt *insert=NULL;
    char *errmsg=NULL;
    int status;

    status=sqlite3_exec(g->db,create_blob_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"Failed to create destination table: %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    status=sqlite3_prepare_v2(
        g->db,extract_frags_sql,sizeof extract_frags_sql,&extract,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(extract_frags): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    status=sqlite3_prepare_v2(
        g->db,insert_blob_sql,sizeof insert_blob_sql,&insert,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(insert_blob): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    for (;;) {
        sqlite3_int64 blob_id;
        sqlite3_value *head,*tail;

        status=sqlite3_step(extract);
        if (status!=SQLITE_ROW)
            break;
        blob_id=sqlite3_column_int64(extract,0);
        sqlite3_bind_int64(insert,1,blob_id);

        head=sqlite3_column_value(extract,1);
        if (!head) {
            fputs(oom_msg,stderr);
            return -1;
        }
        tail=sqlite3_column_value(extract,2);
        if (!head) {
            fputs(oom_msg,stderr);
            return -1;
        }
        if (sqlite3_value_type(head)==SQLITE_NULL) {
            status=sqlite3_bind_null(insert,2);
        } else if (sqlite3_value_type(tail)==SQLITE_NULL) {
            status=sqlite3_bind_value(insert,2,head);
        } else {
            sqlite3_int64 head_size,tail_size,blob_size;
            char const *head_blob,*tail_blob;
            char *blob;

            head_blob=sqlite3_value_blob(head);
            head_size=sqlite3_value_bytes(head);
            tail_blob=sqlite3_value_blob(tail);
            tail_size=sqlite3_value_bytes(tail);
            if (!head_blob && head_size>0
                    || !tail_blob && tail_size>0) {
                fputs(oom_msg,stderr);
                return -1;
            }
            blob_size=head_size+tail_size;
            blob=sqlite3_malloc64(blob_size);
            if (!blob) {
                fputs(oom_msg,stderr);
                return -1;
            }
            if (head_size>0)
                memcpy(blob,head_blob,head_size);
            if (tail_size>0)
                memcpy(blob+head_size,tail_blob,tail_size);
            status=sqlite3_bind_blob64(insert,2,blob,blob_size,sqlite3_free);
        }
        if (status!=SQLITE_OK) {
            fprintf(stderr,"sqlite3_bind(insert): %s\n",sqlite3_errmsg(g->db));
            return -1;
        }
        status=sqlite3_step(insert);
        if (status!=SQLITE_DONE) {
            fprintf(stderr,"sqlite3_step(insert): %s\n",sqlite3_errmsg(g->db));
            return -1;
        }
        sqlite3_reset(insert);
        sqlite3_clear_bindings(insert);
    }
    if (status!=SQLITE_DONE) {
        fprintf(stderr,"sqlite3_step(extract_frags): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    sqlite3_finalize(extract);
    sqlite3_finalize(insert);
    return 0;
}

static int close_db(
    globals *g)
{
    char *errmsg;
    int status;

    status=sqlite3_exec(g->db,commit_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"Failed to commit transaction: %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }
    status=sqlite3_close_v2(g->db);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_close: %s\n",sqlite3_errmsg(g->db));
        return -1;
    }
    g->db=NULL;
    return 0;
}

static int parse_args(
    globals *g,
    int argc,
    char **argv)
{
    int argi;
    char const *arg;

    for (argi=1; argi<argc; ) {
        unsigned int page_size;

        arg=argv[argi];
        if (arg[0]!='-')
            break;
        argi++;
        if (!strcmp(arg,"--"))
            break;
        if (!strcmp(arg,"--page-size")) {
            if (argi>=argc)
                goto missing;
            if (!sscanf(argv[argi],"%u",&page_size)
                    || page_size!=512 && page_size!=1024 && page_size!=2048
                        && page_size!=4096 && page_size!=8192
                        && page_size!=16384 && page_size!=32768
                        && page_size!=65536) {
                fprintf(stderr,"Invalid page size %s\n",argv[argi]);
                return -1;
            }
            g->page_size=page_size;
            argi++;
        } else {
            fprintf(stderr,"Unknown option %s\n",arg);
            goto usage;
        }
    }
    if (argc-argi<2)
        goto usage;
    g->src_path=argv[argi++];
    g->dst_path=argv[argi++];
    return 0;

missing:
    fprintf(stderr,"Missing value for option %s\n",arg);
    return -1;

usage:
    {
        char *progname;

        progname=strrchr(argv[0],'/');
        if (progname) {
            progname++;
        } else {
            progname=argv[0];
        }
        fprintf(stderr,"Usage: %s [ options ] src-path dst-path\n",progname);
    }
    fputs("    Options:\n"
          "        --page-size         number\n",
          stderr);
    return -1;
}

int main(
    int argc,
    char **argv)
{
    globals g=default_globals;

    if (parse_args(&g,argc,argv))
        return 11;
    if (open_db(&g))
        return 1;
    if (transfer_data(&g))
        return 1;
    if (close_db(&g))
        return 1;
    return 0;
}

