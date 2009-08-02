#pragma once
#include <Windows.h>
#include <Winbase.h>
#include "db_cxx.h"

#define FREECELL	0
#define MAX_RES_LOG 12
#define MAXRES		(1 << MAX_RES_LOG) /* available Resources */
#define MAXCRITICAL 3


class HashIdx
{
private:
	struct VElt
	{
		VElt()
		{
			key = NULL;
			length = 0;
			dbptr = NULL;
			flink = blink = NULL;
		}
		~VElt()
		{
			flink = blink = NULL;
		}
		char *key;
		int  length;
		Db   *dbptr;
		struct VElt *flink, *blink;
	};

	struct HashElt
	{
		HashElt()
		{
			concurrent = NULL;
			ptr = NULL;
		};
		CRITICAL_SECTION *concurrent;
		struct VElt *ptr;
	};

	struct ArrayElt
	{
		struct VElt *arrayptr;
		struct ArrayElt *next;
	};

	struct ArrayElt *MallocHead;
	struct VElt freenodes[MAXRES];
	struct HashElt hashtable[MAXRES];
	CRITICAL_SECTION crtl[MAXCRITICAL];
	CRITICAL_SECTION protect_malloc;
	
	int h_hash(register char *key, register int length);
	void first_init_res_node(VElt *, VElt *, VElt *);
	void init_res_node(VElt *, VElt *, VElt *, char *, int, Db *DbHandle);
	void init_resources(void);
	void alloc_more_node(void);
	struct VElt* malloc_node(void);
	void free_bucket(VElt *cell);
	void release_node(VElt *);
	void InitHashTable(void);
	struct VElt *HashIdx::findobj(char *key, int length);
	struct VElt *insertobj(char *, int, Db*);

	bool deleteobj(char *key, int length);

public:
	Db  *Get(char *key, int length);
	int Put(char *key, int length, Db *);
	int Delete(char *key, int length);
	int DeleteOneKeyAndReturnDB(Db *&);
	HashIdx(void);
	~HashIdx(void);
};
