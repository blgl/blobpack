#include <stdio.h>
#include <string.h>
#include <assert.h>

#include <sqlite3.h>

static int varint_size(
    sqlite3_int64 val)
{
    if (val<0)
        return 9;
    if (val<0x80)
        return 1;
    if (val<0x4000)
        return 2;
    if (val<0x200000)
        return 3;
    if (val<0x10000000)
        return 4;
    if (val<0x800000000)
        return 5;
    if (val<0x40000000000)
        return 6;
    if (val<0x2000000000000)
        return 7;
    if (val<0x100000000000000)
        return 8;
    return 9;
}

/*
  payload size for a single row of a table like:

      create table t (id integer primary key, val blob not null);

  1 byte for the header size
  1 byte for the null column type of the rowid alias placeholder
  a varying number of bytes for the blob column type
  the blob bytes themselves
*/

static sqlite3_int64 blob_rec_size(
    sqlite3_int64 blob_len)
{
    return 2+varint_size(blob_len*2+12)+blob_len;
}

/*
  for the above-mentioned kind of row:
      leaf page cell size
      overflow page count
      unused space on the last overflow page
*/

typedef struct space {
    int cell_size;
    unsigned int overflow_cnt;
    int unused_space;
} space;

static space blob_space(
    sqlite3_int64 rowid,
    sqlite3_int64 blob_len,
    int page_size)
{
    sqlite3_int64 rec_size;
    space result;

    rec_size=blob_rec_size(blob_len);
    if (rec_size<=page_size-35) {
        result.cell_size=2+varint_size(rec_size)+varint_size(rowid)+rec_size;
        result.overflow_cnt=0;
        result.unused_space=0;
    } else {
        int K,M,inline_size;
        sqlite3_int64 overflow_cnt;

        M=(page_size-12)*32/255-23;
        K=(int)(M+(rec_size-M)%(page_size-4));
        if (K<=page_size-35) {
            inline_size=K;
        } else {
            inline_size=M;
        }
        overflow_cnt=(rec_size-inline_size+page_size-5)/(page_size-4);
        assert(overflow_cnt<0xFFFFFFFF);
        result.cell_size=
            2+varint_size(rec_size)+varint_size(rowid)+inline_size+4;
        result.overflow_cnt=overflow_cnt;
        result.unused_space=(page_size-4)*overflow_cnt-(rec_size-inline_size);
    }
    return result;
}

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
  When reviewing this code, open packing.sql
  and read it in parallel with this file.
*/

#include "packing.h"

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

/*
  The set of blobs to be split has two disjoint subsets:
  A) The cell size would exceed half the leaf page cell space,
     making it harder to pack things efficiently.
  B) The last overflow page would have unused space in it.
 */

static int generate_frags(
    globals *g)
{
    sqlite3_stmt *list=NULL;
    sqlite3_stmt *split=NULL;
    sqlite3_stmt *frag=NULL;
    char *errmsg=NULL;
    int status;
    sqlite3_int64 frag_cnt,frag_id;
    int half_space;

    fputs("Generating fragments...\n",stderr);
    status=sqlite3_exec(g->db,create_temps_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"Failed to create temporary tables: %s\n",errmsg);
        return -1;
    }

    status=sqlite3_prepare_v2(
        g->db,list_blobs_sql,sizeof list_blobs_sql,&list,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(list_blobs): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    status=sqlite3_prepare_v2(
        g->db,insert_temp_split_sql,sizeof insert_temp_split_sql,&split,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(temp_insert_split): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    status=sqlite3_prepare_v2(
        g->db,insert_temp_frag_sql,sizeof insert_temp_frag_sql,&frag,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(temp_insert_frag): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    half_space=(g->page_size-8)/2;
    frag_cnt=0;
    for (;;) {
        status=sqlite3_step(list);
        if (status!=SQLITE_ROW)
            break;
        if (sqlite3_column_type(list,1)!=SQLITE_NULL) {
            space head_space;
            sqlite3_int64 size;

            size=sqlite3_column_int64(list,1);
            head_space=blob_space(-1,size,g->page_size);
            if (head_space.cell_size>half_space || head_space.unused_space>0) {
                frag_cnt+=2;
            } else {
                frag_cnt+=1;
            }
        }
    }
    if (status!=SQLITE_DONE) {
        fprintf(stderr,"sqlite3_step(list_blobs): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }
    sqlite3_reset(list);

    frag_id=0;
    for (;;) {
        sqlite3_int64 split_id;

        status=sqlite3_step(list);
        if (status!=SQLITE_ROW)
            break;
        split_id=sqlite3_column_int64(list,0);
        sqlite3_bind_int64(split,1,split_id);
        status=sqlite3_step(split);
        if (status!=SQLITE_DONE) {
            fprintf(stderr,"sqlite3_step(insert_temp_split): %s\n",
                    sqlite3_errmsg(g->db));
            return -1;
        }
        sqlite3_reset(split);

        if (sqlite3_column_type(list,1)!=SQLITE_NULL) {
            sqlite3_int64 size,head_size,tail_size;
            space head_space,tail_space;
            sqlite3_int64 lo,hi;

            size=sqlite3_column_int64(list,1);
            head_space=blob_space(frag_cnt,size,g->page_size);
            if (head_space.cell_size>half_space) {
                /*
                  Subset A:
                  Pick a head size that leaves the tail with the same
                  number of overflow pages as the unsplit version.
                  The cell sizes will fall in the approximate range
                  1/4 to 1/2 of the page size.
                */
                lo=g->page_size/8;
                hi=g->page_size*5/8;
            } else if (head_space.unused_space>0) {
                /*
                  Subset B:
                  Pick a head size that leaves the tail with one less
                  overflow page than the unsplit version.
                  The cell sizes will fall in the approximate range
                  1/2 to 9/16 of the page size.
                 */
                lo=g->page_size*17/32;
                hi=g->page_size*19/32;
            } else {
                hi=lo=size;
            }
            while (hi-lo>1) {
                sqlite3_int64 mid;

                mid=(lo+hi)/2;
                head_space=blob_space(frag_cnt,mid,g->page_size);
                tail_space=blob_space(frag_cnt,size-mid,g->page_size);
                if (tail_space.cell_size<head_space.cell_size) {
                    hi=mid;
                } else {
                    lo=mid;
                }
            }

            head_size=lo;
            head_space=blob_space(frag_cnt,head_size,g->page_size);
            assert(head_space.unused_space==0);
            frag_id++;
            sqlite3_bind_int64(frag,1,frag_id);
            sqlite3_bind_int(frag,2,0);
            sqlite3_bind_int64(frag,3,head_size);
            sqlite3_bind_int(frag,4,head_space.cell_size);
            sqlite3_bind_int64(frag,5,split_id);
            status=sqlite3_step(frag);
            if (status!=SQLITE_DONE) {
                fprintf(stderr,"sqlite3_step(insert_temp_frag): %s\n",
                        sqlite3_errmsg(g->db));
                return -1;
            }
            sqlite3_reset(frag);

            tail_size=size-head_size;
            if (tail_size>0) {
                tail_space=blob_space(frag_cnt,tail_size,g->page_size);
                assert(tail_space.unused_space==0);
                frag_id++;
                sqlite3_bind_int64(frag,1,frag_id);
                sqlite3_bind_int64(frag,2,head_size);
                sqlite3_bind_int64(frag,3,tail_size);
                sqlite3_bind_int(frag,4,tail_space.cell_size);
                status=sqlite3_step(frag);
                if (status!=SQLITE_DONE) {
                    fprintf(stderr,"sqlite3_step(insert_temp_frag): %s\n",
                            sqlite3_errmsg(g->db));
                    return -1;
                }
                sqlite3_reset(frag);
            }
        }
    }
    if (status!=SQLITE_DONE) {
        fprintf(stderr,"sqlite3_step(list_blobs): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    sqlite3_finalize(list);
    sqlite3_finalize(split);
    sqlite3_finalize(frag);
    return 0;
}

/*
  For f fragments and p pages, looping over pages is O((f+p) log(f))
  while looping over fragments is O(f log(p)).
  Since p<=f, the second approach wins.
*/

static int fill_pages(
    globals *g)
{
    sqlite3_stmt *size;
    sqlite3_stmt *list=NULL;
    sqlite3_stmt *find=NULL;
    sqlite3_stmt *insert=NULL;
    sqlite3_stmt *update=NULL;
    sqlite3_stmt *assign=NULL;
    char *errmsg=NULL;
    int status;
    unsigned int max_space,min_size;
    sqlite3_int64 next_page;

    fputs("Packing fragments into pages...\n",stderr);
    status=sqlite3_prepare_v2(
        g->db,min_size_sql,sizeof min_size_sql,&size,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(min_size): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }
    status=sqlite3_step(size);
    if (status!=SQLITE_ROW) {
        fprintf(stderr,"sqlite3_step(min_size): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }
    min_size=sqlite3_column_int(size,0);
    sqlite3_finalize(size);
    size=NULL;

    status=sqlite3_exec(g->db,create_temp_page_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"Failed to create temporary page table: %s\n",errmsg);
        return -1;
    }

    status=sqlite3_prepare_v2(
        g->db,list_frags_sql,sizeof list_frags_sql,&list,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(list_frags): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    status=sqlite3_prepare_v2(
        g->db,find_page_sql,sizeof find_page_sql,&find,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(find_page): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    status=sqlite3_prepare_v2(
        g->db,insert_page_sql,sizeof insert_page_sql,&insert,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(insert_page): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    status=sqlite3_prepare_v2(
        g->db,update_page_sql,sizeof update_page_sql,&update,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(update_page): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    status=sqlite3_prepare_v2(
        g->db,assign_page_sql,sizeof assign_page_sql,&assign,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(assign_page): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    max_space=g->page_size-8;
    next_page=0;
    for (;;) {
        sqlite3_int64 frag_id, page_id;
        unsigned int cell_size, cell_space;
        sqlite3_stmt *page;

        status=sqlite3_step(list);
        if (status!=SQLITE_ROW)
            break;
        frag_id=sqlite3_column_int64(list,0);
        cell_size=sqlite3_column_int(list,1);
        sqlite3_bind_int(find,1,cell_size);
        status=sqlite3_step(find);
        switch (status) {
        case SQLITE_ROW:
            page_id=sqlite3_column_int64(find,0);
            cell_space=sqlite3_column_int(find,1);
            page=update;
            break;
        case SQLITE_DONE:
            page_id=++next_page;
            cell_space=max_space;
            page=insert;
            break;
        default:
            fprintf(stderr,"sqlite3_step(find_page): %s\n",
                    sqlite3_errmsg(g->db));
            return -1;
        }
        sqlite3_reset(find);

        cell_space-=cell_size;
        sqlite3_bind_int64(page,1,page_id);
        if (cell_space>=min_size) {
            sqlite3_bind_int(page,2,cell_space);
        } else {
            sqlite3_bind_null(page,2);
        }
        status=sqlite3_step(page);
        if (status!=SQLITE_DONE) {
            fprintf(stderr,"sqlite3_step(*_page): %s\n",
                    sqlite3_errmsg(g->db));
            return -1;
        }
        sqlite3_reset(page);

        sqlite3_bind_int64(assign,1,frag_id);
        sqlite3_bind_int64(assign,2,page_id);
        status=sqlite3_step(assign);
        if (status!=SQLITE_DONE) {
            fprintf(stderr,"sqlite3_step(assign_page): %s\n",
                    sqlite3_errmsg(g->db));
            return -1;
        }
        sqlite3_reset(assign);

    }
    if (status!=SQLITE_DONE) {
        fprintf(stderr,"sqlite3_step(list_frags): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    sqlite3_finalize(list);
    sqlite3_finalize(find);
    sqlite3_finalize(insert);
    sqlite3_finalize(update);
    sqlite3_finalize(assign);

/*
  A split is undone if either
  a) both fragments end up on the same page, or
  b) both fragments end up as the only ones on their respective pages.

  This is all done in SQL.  The cell_size column isn't updated,
  but it's not used after this step.
*/

    status=sqlite3_exec(g->db,unsplit_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"Failed to remove useless splits: %s\n",errmsg);
        return -1;
    }

    return 0;
}

/*
  Goal: for each split, select head and tail ids that are close together.

  Consider splits and pages as the vertex subsets of a bipartite graph
  with fragments as the edges.  Do a breadth-first traversal of each
  connected subgraph, ordering pages as found.

  Generate the final fragment order from the page order.
*/

static int order_frags(
    globals *g)
{
    sqlite3_stmt *one_split=NULL;
    sqlite3_stmt *more_pages=NULL;
    sqlite3_stmt *more_splits=NULL;
    char *errmsg=NULL;
    int status;
    sqlite3_int64 page_cnt,split_cnt,split_id;
    sqlite3_int64 changes;

    fputs("Ordering pages...\n",stderr);
    status=sqlite3_exec(g->db,create_order_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"Failed to create ordering tables: %s\n",
                errmsg);
        return -1;
    }
    status=sqlite3_prepare_v2(
        g->db,one_split_sql,sizeof one_split_sql,&one_split,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(one_split): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }
    status=sqlite3_prepare_v2(
        g->db,more_pages_sql,sizeof more_pages_sql,&more_pages,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(more_pages): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }
    status=sqlite3_prepare_v2(
        g->db,more_splits_sql,sizeof more_splits_sql,&more_splits,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(more_splits): %s\n",
                sqlite3_errmsg(g->db));
        return -1;
    }

    page_cnt=split_cnt=0;
    split_id=-9223372036854775807-1;
    for (;;) {
        sqlite3_bind_int64(one_split,1,split_id);
        status=sqlite3_step(one_split);
        if (status!=SQLITE_ROW)
            break;
        split_id=sqlite3_column_int64(one_split,0)+1;
        sqlite3_reset(one_split);
        changes=1;

        for (;;) {
            sqlite3_bind_int64(more_pages,1,split_cnt);
            split_cnt+=changes;
            status=sqlite3_step(more_pages);
            if (status!=SQLITE_DONE) {
                fprintf(stderr,"sqlite3_step(more_pages): %s\n",
                        sqlite3_errmsg(g->db));
                return 1;
            }
            changes=sqlite3_changes64(g->db);
            sqlite3_reset(more_pages);
            if (changes<=0)
                break;

            sqlite3_bind_int64(more_splits,1,page_cnt);
            page_cnt+=changes;
            status=sqlite3_step(more_splits);
            if (status!=SQLITE_DONE) {
                fprintf(stderr,"sqlite3_step(more_splits): %s\n",
                        sqlite3_errmsg(g->db));
                return 1;
            }
            changes=sqlite3_changes64(g->db);
            sqlite3_reset(more_splits);
            if (changes<=0)
                break;
        }
    }
    if (status!=SQLITE_DONE) {
        fprintf(stderr,"sqlite3_step(one_page): %s\n",sqlite3_errmsg(g->db));
        return -1;
    }

    sqlite3_finalize(one_split);
    sqlite3_finalize(more_pages);
    sqlite3_finalize(more_splits);

    fputs("Ordering fragments...\n",stderr);
    status=sqlite3_exec(g->db,order_frags_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"Failed to assign final fragment ids: %s\n",
                errmsg);
        return -1;
    }

    return 0;
}

/*
  Write the output tables in rowid order.  Yes, this means that
  split source blobs are read twice, but since the destination database
  won't need vacuuming, it's a net win measured by total i/o.
*/

static int write_output(
    globals *g)
{
    char *errmsg=NULL;
    int status;

    fputs("Writing output splits...\n",stderr);
    status=sqlite3_exec(g->db,write_splits_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"Failed to populate splits table: %s\n",errmsg);
        return -1;
    }

    fputs("Writing output fragments...\n",stderr);
    status=sqlite3_exec(g->db,write_frags_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"Failed to populate frags table: %s\n",errmsg);
        return -1;
    }

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
    if (generate_frags(&g))
        return 1;
    if (fill_pages(&g))
        return 1;
    if (order_frags(&g))
        return 1;
    if (write_output(&g))
        return 1;
    if (close_db(&g))
        return 1;
    return 0;
}

