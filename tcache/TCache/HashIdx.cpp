/*
 * Copyright (c) 2009
 *   TrojansLab.  All rights reserved.
 *   Contributors:  Shahram Ghandeharizadeh, Jason Yap, Showvick Kalra, Jorge Gonzalez, Igor Shvager.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1.  Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 * 2.  Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Redistributions in any form must be accompanied by information on
 *    how to obtain complete source code for the TCache software and any accompanying
 *    software that uses the TCache software.  The source code must either be
 *    included in the distribution or be available for no more than the cost
 *    of distribution plust a nominal fee, and must be freely redistributable
 *    under reasonable conditions.  For an executable file, complete source
 *    means the source code for all modules it contains.  It does not include
 *    source code for all modules it contains.  It does not include source
 *    code for modules or files that typically accompany the major components
 *    of the operating system on which the executable file runs.
 *
 * This software is provided by TrojansLab LLC and contributors AS IS and any express or
 * implied warranties, including, but not limited to, the implied warranties
 * of merchantability, fitness for a particular purpose, or non-infringement,
 * are disclaimed.  In no even shall TrojansLab LLC and contributors be liable for
 * any direct, indirect, incidental, speical, exemplary, or consequential damages
 * (including, but not limited to, procurement of substitute goods or services;
 * loss of use, data, or profits; or business interuption) however caused and or any
 * theory of liability, whether in contract, strict liability, or tort (including
 * negligence or otherwise) arising in any way out of the use of this software,
 * even if advised of the possibility of such damage.
 */

#include "StdAfx.h"
#include "HashIdx.h"

bool bernsteinHash=true;

int HashIdx::h_hash(register const char *key, register int length)

/* hash fouction : h(key) = sum(k1, k2,...) mod (2 ** bit length)
                        where key = k1.k2.k3...                  */
{
#ifdef VERBOSE
		printf("HashIdx:h_hash key=%d, length=%d", 
				(int) *key, length);
#endif
        register int    value;  /* accumulator */

        for (value = 0; length > 0; length--) value += (int)(*(key++));
        if (value < 0) value = -value;

#ifdef VERBOSE
		printf("HashIdx:h_hash value=%d, h=%d", 
				value, value%MAXRES);
#endif
        return(value % MAXRES);       /* return rightmost bits */

}





/*************************** IMPLEMENTS MALLOC **********************************/
void HashIdx::first_init_res_node(VElt *source, struct VElt *flink, struct VElt *blink)
{
#ifdef VALIDATE
	if (source == NULL)
			printf("HashIdx:first_init_res_node: ERROR, source is NULL.");
		
#endif
#ifdef VERBOSE
	printf("HashIdx:first_init_res_node invoked.");
#endif
    source -> flink = flink;
    source -> blink = blink;
	source -> key = NULL;
	source -> length = 0;

#ifdef VERBOSE
	printf("HashIdx:first_init_res_node Done and leaving");
#endif
};


void HashIdx::init_res_node(VElt *source, struct VElt *flink, struct VElt *blink, const char *inputkey, int length, Db* dbptr,short numOfPartitions)
{
#ifdef VALIDATE
	if (dbptr < 0 || source == NULL)
		if (pval != -0.01)
			printf("HashIdx:init_res_node: ERROR, registering pval (%.20f) less than zero OR source is NULL.", pval);
		
#endif
#ifdef VERBOSE
	printf("HashIdx:init_res_node invoked with key=%d, length=%d", 
			(int) *inputkey, length);
#endif
    source -> flink = flink;
    source -> blink = blink;
	source -> dbptr = dbptr;
	source ->numOfPartition=numOfPartitions;

	if (length > 0 && length > source->length)
	{
		if (source->key != NULL) free(source->key);
		source->key = (char *) malloc(length);
		if (source->key == NULL)
		{
			printf("HashIdx:init_res_node: ERROR, malloc failed.");
		}
	} 

	if (length > 0)
	{
		if (inputkey != NULL)
			memcpy(source->key, inputkey, length);
		source -> length = length;
	}


#ifdef VERBOSE
	printf("init_res_node (objid=%d, p=%lf, TS1=%lld, TS2=%lld)", 
			(int) *inputkey, pval, source->TS1, source->TS2);
#endif
};



void HashIdx::init_resources(void)
{
	register int counter;
	first_init_res_node (&freenodes[FREECELL], &freenodes[FREECELL + 1], &freenodes[MAXRES - 1]); //, NULL, 0, -0.01);

    /* Initialize the rest of the list */
    for (counter = 1; counter < MAXRES; counter ++)
    {
        first_init_res_node (&freenodes[counter], &freenodes[counter + 1], &freenodes[counter - 1]); //, NULL, 0, -0.01);
    }

    /* Initialize the last free element */
    first_init_res_node (&freenodes[MAXRES - 1], &freenodes[FREECELL], &freenodes[MAXRES - 2]); //, NULL, 0, -0.01);
}

void HashIdx::alloc_more_node(void)
{
	register struct VElt *p, *perm;
	register short            counter;
	struct ArrayElt *newElt;

	p = (struct VElt *) malloc(sizeof(freenodes));
	if (p == NULL)
	{
		printf("HashIdx:alloc_more_node: ERROR, malloc failed in");
		return;
	}

	perm = p;
	first_init_res_node(perm, perm+1, &freenodes[FREECELL]); //, NULL, 0, -0.01);
    for (counter = 2, p++; counter < MAXRES; counter ++, p ++)
            first_init_res_node (p, p + 1, p - 1); //, NULL, 0, -0.01);
    first_init_res_node (&freenodes[FREECELL], perm, p); //, NULL, 0, -0.01);
    first_init_res_node (p, &freenodes [FREECELL], p - 1); //, NULL, 0, -0.01);
	
	newElt = (struct ArrayElt *) malloc(sizeof(ArrayElt));
	newElt->arrayptr = p;
	newElt->next = MallocHead;
	MallocHead = newElt;
}

struct HashIdx::VElt *HashIdx::malloc_node(void)
{
	struct VElt *found;
#ifdef VERBOSE
	printf("HashIdx:malloc_node() entered");
#endif

	EnterCriticalSection(&protect_malloc);

    if (freenodes [FREECELL].flink == &freenodes[FREECELL])
        alloc_more_node ();

    found = freenodes [FREECELL].flink;

    /* disconnect found node from free list */
    freenodes [FREECELL].flink = found -> flink;
    found -> flink -> blink = &freenodes [FREECELL];
    found -> flink = found -> blink = NULL;


	LeaveCriticalSection(&protect_malloc);
#ifdef VERBOSE
	printf("HashIdx:malloc_node exited");
#endif
	return found;
}

void HashIdx::free_bucket(VElt *cell)
{
	EnterCriticalSection(&protect_malloc);

    init_res_node (cell, freenodes [0].flink, &freenodes [0], NULL, 0,NULL,-1); 
    freenodes [0].flink -> blink = cell;
    freenodes [0].flink = cell;

	LeaveCriticalSection(&protect_malloc);
}

void HashIdx::release_node(VElt *bucket)
{
	if (bucket-> blink != NULL)
		bucket->blink->flink = bucket->flink;
	if (bucket -> flink != NULL)
		bucket -> flink -> blink = bucket -> blink;
	free_bucket (bucket);
}


/********************************* HASHTABLE ***********************************/
void HashIdx::InitHashTable(void)
{
	register int counter;

	/*
	 * Initialize the critical sections
	 */
	for (counter = 0; counter < MAXCRITICAL; counter++)
	{
		//InitializeCriticalSectionAndSpinCount(&crtl[counter], 0x8040000);
		InitializeCriticalSection(&crtl[counter]);
	}

	/*
	 * Initialize the hash table
	 */
	for (counter = 0; counter < MAXRES; counter++)
	{
		hashtable[counter].concurrent = & crtl[counter%MAXCRITICAL];
		hashtable[counter].ptr = NULL;
	}

	/*
	 * Initialize the free list and malloc synchronizer
	 */
	//InitializeCriticalSectionAndSpinCount(&protect_malloc, 0x8040000);
	InitializeCriticalSection(&protect_malloc);
	init_resources();
}

struct HashIdx::VElt *HashIdx::findobj(const char *key, int length)
{

	struct      HashIdx::VElt    *finger;
	struct      HashIdx::VElt    *result;
	struct      HashElt *hashEltptr;
    short       done;

	if (key == NULL || length == 0)
	{
#ifdef VERBOSE
		printf("HashIdx:findobj: findobj invoked with a null key and/or length %d",length);
#endif
		return NULL;
	}

	done = FALSE;
    result = NULL;
	int h;
	if(bernsteinHash==true)
		h=TCUtility::bernsteinHash(key,length,MAXRES);
	else
		h = h_hash(key, length);
	hashEltptr = &(hashtable[h]);

	EnterCriticalSection(hashEltptr->concurrent);

	finger = hashEltptr->ptr;
    for ( ; (finger != NULL && (done != TRUE)); finger = finger -> flink)
    {
		if (finger -> length == length && memcmp(finger->key, key, length)==0)
        {
                result = finger;
                done = TRUE;
        }
    }

	LeaveCriticalSection(hashEltptr->concurrent);
#ifdef VERBOSE
		printf("HashIdx:findobj: findobj invoked with key %d and length %d... not found",
				(int) *key, length);
#endif

    return (result);
}



struct HashIdx::VElt *HashIdx::insertobj(const char *key, int length, Db *dbptr,short numOfPartitions)
{
	if (key == NULL || length == 0)
	{
		return NULL;
	}

	struct VElt *finger = malloc_node();
#ifdef VALIDATE
	if (finger == NULL)
	{
		printf("HashIdx:insertobj: ERROR, malloc noded failed with key %d and length %d", (int) *key, length);
		return NULL;
	}
#endif
	int Mindex;
	if(bernsteinHash==true)
		Mindex=TCUtility::bernsteinHash(key,length,MAXRES);
	else
		Mindex = h_hash(key, length);

	init_res_node(finger, NULL, NULL, key, length,dbptr,numOfPartitions); 
#ifdef VERBOSE
	printf("HashIdx:insertobj: hash index %d, length %d, pval %.20f\n",Mindex, length, pval);
#endif
	
	EnterCriticalSection(hashtable[Mindex].concurrent);

	if (hashtable[Mindex].ptr == NULL)
		hashtable[Mindex].ptr = finger;
	else {
		finger->flink = hashtable[Mindex].ptr;
		hashtable[Mindex].ptr -> blink = finger;
		hashtable[Mindex].ptr = finger;
	}

	LeaveCriticalSection(hashtable[Mindex].concurrent);
	return finger;
}

bool HashIdx::deleteobj(const char *key, int length)
{
	struct      HashIdx::VElt    *finger;
	struct      HashIdx::VElt    *result;
	struct      HashElt *hashEltptr;
    short       done;

	if (key == NULL || length == 0)
		return false;

	done = FALSE;
    result = NULL;
	
	if(bernsteinHash==true)
		hashEltptr = &(hashtable[TCUtility::bernsteinHash(key,length,MAXRES)]);
	else
		hashEltptr = &(hashtable[h_hash(key, length)]);

	EnterCriticalSection(hashEltptr->concurrent);

	finger = hashEltptr->ptr;
    for ( ; (finger != NULL && (done != TRUE)); finger = finger -> flink)
    {
		if (finger -> length == length && memcmp(finger->key, key, length)==0)
        {
                result = finger;
                done = TRUE;
        }
    }

	if (result != NULL){
		finger = result;

		if (finger->blink != NULL) finger -> blink -> flink = finger -> flink;
		if (finger->flink != NULL) finger -> flink -> blink = finger -> blink;

		if (hashEltptr->ptr == finger) hashEltptr-> ptr = finger -> flink;

		release_node(finger);
	}
	LeaveCriticalSection(hashEltptr->concurrent);

#ifdef VALIDATE
	if (result == NULL){
		printf("HashIdx:deleteobj: deleteobj did not find the referenced object %d to delete\n", (int) *key);
		return false;
	}
#endif
	return true;
}

/***************************** PUBLIC METHODS ************************************/

int HashIdx::Delete(const char *key, int length)
{
	struct HashIdx::VElt *res = findobj(key, length);
	if (res == NULL)
	{
		return DB_NOTFOUND;
	}

	deleteobj(key, length);
	return 0;
}

//Db *GlobalDBHandle=NULL;

Db *HashIdx::Get(const char *key, int length,short &numOfPartitions)
{
	//return GlobalDBHandle;
	int ret = 0;
	struct HashIdx::VElt *res = findobj(key, length);	//=>
	if (res==NULL)
	{
#ifdef VERBOSE
		printf("HashIdx:Get() key=%d not found.", (int) *key);
#endif
		return NULL;
	}
	numOfPartitions= res->numOfPartition;
	return res->dbptr;
}

int HashIdx::Put(const char *key, int length, Db *DbHandle,short numOfPartitions)
{
	if (key == NULL)
	{
		printf("HashIdx:Put: ERROR, Put failed because its input key is NULL!\n");
		return EXIT_FAILURE;
	}
	/*else if (DbHandle == NULL)
	{
		printf("HashIdx:Put: ERROR, Put failed because its input DbHandle is NULL!\n");
		return EXIT_FAILURE;
	}*/
	else if (length==0)
	{
		printf("HashIdx:Put: ERROR, Put failed because its input length is %d!\n",length);
		return EXIT_FAILURE;
	}

	//GlobalDBHandle = DbHandle;
	//return 0;

	struct HashIdx::VElt *res = findobj(key, length);
#ifdef VERBOSE
	printf("HashIdx:Put() invoked with (key=%s, length=%d)\n", 
			*key, length);
	}
#endif

	//Key does not exist in the hash index
	if (res == NULL)
	{
#ifdef VERBOSE
		printf("HashIdx:Put() res is null and inserting NEW (key=%d)\n", (int) *key);
#endif
		res = insertobj(key, length, DbHandle,numOfPartitions);	//=>
		if (res == NULL)
		{
			printf("HashIdx:Put: ERROR, insertobj failed by producing NULL!\n");
			return EXIT_FAILURE;
		}
#ifdef VERBOSE
		printf("HashIdx:Put() inserted NEW (key=%d, p=%lf)\n", (int) *key, res->p);
#endif
		return 0;
	} 
	else 
	{
#ifdef VERBOSE
		printf("HashIdx:Put() res is NOT null and inserting NEW (key=%d, p=%lf)\n", (int) *key, res->p);
#endif

		return DB_KEYEXIST;


#ifdef VERBOSE
		printf("HashIdx:Put(): UPDATED existing elt (key=%d, existing elt=%d)\n", 
				(int) *key, res->key);
#endif
	}
	return 0;
}

int HashIdx::DeleteOneKeyAndReturnDB(Db *&Dbptr)
{
	bool done = false;
	struct HashElt *hashEltptr;
	struct HashIdx::VElt    *finger = NULL;
	struct HashIdx::VElt    *result = NULL;

#ifdef VERBOSE
	printf("HashIdx:DeleteOneKeyAndReturnDB() invoked.\n");
#endif
	
	for (int i = 0; !done; i++)
	{
		if (i < MAXRES) {
			hashEltptr = &(hashtable[i]);
			EnterCriticalSection(hashEltptr->concurrent);

			finger = hashEltptr->ptr;
			if (finger != NULL)
			{
#ifdef EXTRA_DEBUG
				printf("Found %s.\n",finger->key);
				//printf(" %d\n",finger->numOfPartition);
#endif
				result = finger;
				done = TRUE;
				Dbptr = result->dbptr;
			}

			LeaveCriticalSection(hashEltptr->concurrent);
		} else done = true;
	}
	if (result == NULL)
	{
		//printf("HashIdx::DeleteOneKeyAndReturnDB Error:  No more elements.\n");
		return -1;
	}
	if (result->key == NULL || result->length == 0)
	{
		printf("HashIdx::DeleteOneKeyAndReturnDB Error:  length is %d; key might be null.\n",result->length);
		return -1;
	}
	if (deleteobj(result->key, result->length) != true)
	{
		printf("HashIdx::DeleteOneKeyAndReturnDB Error:  failed to delete the found key %s.\n",result->key);
		return -1;
	}
	return 0;
}


HashIdx::HashIdx(void)
{
	InitHashTable();
	MallocHead = NULL;
}

HashIdx::~HashIdx(void)
{
}
