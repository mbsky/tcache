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
#include "ContextManager.h"

#define TCM_NUM_ELEMENTS_TO_ACCESS 16
#define TCM_MAX_SIMULTANEOUS_THREADS 10000
#define TCM_SEED 5
#define TCM_NUM_SIMULTANEOUS_THREADS	16
#define TCM_MAXTHREADS 32
#define TCM_MAX_SLEEP_TIME 1000




ContextManager cm;

int MaxSize()
{
	int max=CM_DEFAULT_QDB_KEY_SIZE;
	if (max<CM_DEFAULT_QDB_VALUE_SIZE)
		max=CM_DEFAULT_QDB_VALUE_SIZE;

	if (max<CM_DEFAULT_MD_LOOKUP_VALUE_SIZE)
		max=CM_DEFAULT_MD_LOOKUP_VALUE_SIZE;

	if (max<CM_DEFAULT_MD_LOOKUP_VALUE_SIZE)
		max=CM_DEFAULT_MD_LOOKUP_VALUE_SIZE;

	if (max<CM_DEFAULT_CACHEDB_KEY_SIZE)
		max=CM_DEFAULT_CACHEDB_KEY_SIZE;
	
	return max;
}

// Generate a pattern and it has to freed by the caller 
// pattern:- The pattern is generated in this variable, it has to be freed by the caller
// seed :- Defualt is set to TCM_SEED (5) 
// max:- The size of the pattern to be generated, by defualt it is max size of QDB or CacheDb key

void GeneratePattern(char *&pattern, int seed=TCM_SEED,int max=-1)
{
	if(max==-1)
		max=MaxSize();

	pattern=new char[max];
	for(int k=seed,j=0;k<seed+max/(int)(sizeof(int));k++,j+=sizeof(int))
	{
		*(int *)(pattern+j)=k;
	}

	/*for(int k=0,j=0;k<max/sizeof(int);k++,j+=sizeof(int))
	{
		printf("%d",*(int *)(pattern+j));
	
	}*/
}

// Simple test to run "artifical" NumSimultaneous threads 
int ArtificalSimultaneousThreads (int NumSimultaneousThreads)
{
	ContextStructure* cs[TCM_MAX_SIMULTANEOUS_THREADS];
	//if(NumSimultaneousThreads<MAX_SIMULTANEOUS_THREADS)
	int i,ret=0;

	//Get context 
	for (i=0;i<NumSimultaneousThreads;i++)
	{
		ret=cm.GetFreeContext(cs[i]);
		if(ret)
		{
			printf("Error creating new context\n");
			return -1;
		}
	}

	// Generate the pattern
	char *pattern;
	GeneratePattern(pattern);
	/*int max=64;
	for(int k=0,j=0;k<max/(int)(sizeof(int));k++,j+=sizeof(int))
	{
		printf("%d",*(int *)(pattern+j));
	}*/

	//Copy pattern to context
	for (i=0;i<NumSimultaneousThreads;i++)
	{
		memcpy(cs[i]->cachedb_key,pattern,CM_DEFAULT_CACHEDB_KEY_SIZE);
		memcpy(cs[i]->md_lookup_key,pattern,CM_DEFAULT_CACHEDB_KEY_SIZE);
		memcpy(cs[i]->md_lookup_value,pattern,CM_DEFAULT_MD_LOOKUP_VALUE_SIZE);
		memcpy(cs[i]->qdb_key,pattern,CM_DEFAULT_QDB_KEY_SIZE);
		memcpy(cs[i]->qdb_value,pattern,CM_DEFAULT_QDB_VALUE_SIZE);

	}

	//Compare if the context is correct 
	for (i=0;i<NumSimultaneousThreads;i++)
	{
		if(memcmp(cs[i]->cachedb_key,pattern,CM_DEFAULT_CACHEDB_KEY_SIZE)!=0)
		{
			printf("Pattern does not match created value\n");
			return -2;
		}
		if(memcmp(cs[i]->md_lookup_key,pattern,CM_DEFAULT_CACHEDB_KEY_SIZE)!=0)
		{
			printf("Pattern does not match created value\n");
			return -2;
		}
		if(memcmp(cs[i]->md_lookup_value,pattern,CM_DEFAULT_MD_LOOKUP_VALUE_SIZE)!=0)
		{
			printf("Pattern does not match created value\n");
			return -2;
		}
		if(memcmp(cs[i]->qdb_key,pattern,CM_DEFAULT_QDB_KEY_SIZE)!=0)
		{
			printf("Pattern does not match created value\n");
			return -2;
		}
		if(memcmp(cs[i]->qdb_value,pattern,CM_DEFAULT_QDB_VALUE_SIZE)!=0)
		{
			printf("Pattern does not match created value\n");
			return -2;
		}
	}

	// Release Context
	for(i=0;i<NumSimultaneousThreads;i++)
	{
		ret=cm.ReleaseContext(cs[i]);
		if(ret)
		{
			printf("Error releasing context\n");
			return -2;
		}
	}
	delete [] pattern;
	return 0;
}

	
DWORD WINAPI ContextWorkerThread( LPVOID lpParam ) 
{	
	// Thread ID
	int id = *((int*)lpParam); 
	int NumSimultaneousThreads=TCM_NUM_ELEMENTS_TO_ACCESS;
	ContextStructure* cs[TCM_MAX_SIMULTANEOUS_THREADS];
	//if(NumSimultaneousThreads<MAX_SIMULTANEOUS_THREADS)
	int i,ret=0;

	
	//Get context 
	for (i=0;i<NumSimultaneousThreads;i++)
	{
		
		ret=cm.GetFreeContext(cs[i]);
		if(ret)
		{
			printf("Error creating new context\n");
			return -1;
		}
	}

	Sleep( rand() % TCM_MAX_SLEEP_TIME );

	// Generate the pattern
	char *pattern;
	GeneratePattern(pattern);
	/*int max=64;
	for(int k=0,j=0;k<max/(int)(sizeof(int));k++,j+=sizeof(int))
	{
		printf("%d",*(int *)(pattern+j));
	}*/

	//Copy pattern to context
	for (i=0;i<NumSimultaneousThreads;i++)
	{
		memcpy(cs[i]->cachedb_key,pattern,CM_DEFAULT_CACHEDB_KEY_SIZE);
	//	memcpy(cs[i]->md_lookup_key,pattern,CM_DEFAULT_CACHEDB_KEY_SIZE);
		memcpy(cs[i]->md_lookup_value,pattern,CM_DEFAULT_MD_LOOKUP_VALUE_SIZE);
		memcpy(cs[i]->qdb_key,pattern,CM_DEFAULT_QDB_KEY_SIZE);
		memcpy(cs[i]->qdb_value,pattern,CM_DEFAULT_QDB_VALUE_SIZE);

	}

	//Compare if the context is correct 
	for (i=0;i<NumSimultaneousThreads;i++)
	{
		if(memcmp(cs[i]->cachedb_key,pattern,CM_DEFAULT_CACHEDB_KEY_SIZE)!=0)
		{
			printf("Pattern does not match created value\n");
			return -2;
		}
		//if(memcmp(cs[i]->md_lookup_key,pattern,CM_DEFAULT_CACHEDB_KEY_SIZE)!=0)
		//{
		//	printf("Pattern does not match created value\n");
		//	return -2;
		//}
		if(memcmp(cs[i]->md_lookup_value,pattern,CM_DEFAULT_MD_LOOKUP_VALUE_SIZE)!=0)
		{
			printf("Pattern does not match created value\n");
			return -2;
		}
		if(memcmp(cs[i]->qdb_key,pattern,CM_DEFAULT_QDB_KEY_SIZE)!=0)
		{
			printf("Pattern does not match created value\n");
			return -2;
		}
		if(memcmp(cs[i]->qdb_value,pattern,CM_DEFAULT_QDB_VALUE_SIZE)!=0)
		{
			printf("Pattern does not match created value\n");
			return -2;
		}
	}

	
	// Release Context
	for(i=0;i<NumSimultaneousThreads;i++)
	{
		ret=cm.ReleaseContext(cs[i]);
		if(ret)
		{
			printf("Error releasing context\n");
			return -2;
		}
	}
	printf("Thread %d passed test\n",id);
	delete [] pattern;
	return 0;
	
}


int Test3()
{
	int ret=0;
	for(int i=0;i<1000000;i++)
	{
		int numSt=16;
		ret=ArtificalSimultaneousThreads(numSt);
		if(ret)
			return ret;
	}
	return 0;
}

// Realloc test
int Test4(int reallocCount)
{
	int ret;
	int new_size = 0;

	ContextStructure* cs;
	ret=cm.GetFreeContext(cs);
	if(ret)
	{
		printf("Error creating new context\n");
		return -1;
	}
	cs->qdb_key_size = 5;
	for(int i=0;i<reallocCount;i++)
	{
		new_size = cs->qdb_key_size + 5*i;
		ret=cm.ReallocMemory(cs->qdb_key, cs->qdb_key_size, new_size);
		//ret = cm.ResizeMemory( cs->qdb_key, new_size );
		cs->qdb_key_size = new_size;
		if(ret)
		{
			printf("Error reallocating \n");
			return -1;
		}
		// If there is a error it will print a Access Violation writing
		memset(cs->qdb_key,36,new_size);
	}
	return 0;
}
	
int ContextTestMain()
{
	int ret=0;
	// Test 1 
	ret=ArtificalSimultaneousThreads(8);
	if(ret)
		printf("Test 1 failed\n");
	else
		printf("Test 1 Passed\n");
	
	//Test 2 
	ret=ArtificalSimultaneousThreads(32);
	if(ret)
		printf("Test 2 failed\n");
	else
		printf("Test 2 Passed\n");
	
	/*
	// Test3
	// Repeat the process of ArtificalSimulatonoes Threads=16 million times
	ret=Test3();
	if(ret)
		printf("Test 3 failed\n");
	else
		printf("Test 3 Passed\n");
	//*/
	
	//*
	// Test 4 Realloc test 
	int reallocCount=1000;
	ret=Test4(reallocCount);
	if(ret)
		printf("Test 4 failed\n");
	else
		printf("Test 4 Passed\n");
	//*/
	
	// Test 5 
	// Create NumThreads and check if it is working
	int NumThreads=TCM_NUM_SIMULTANEOUS_THREADS;
	int IDs[TCM_MAXTHREADS];
	HANDLE Array_Of_Thread_Handles[TCM_MAXTHREADS];

	// Create the threads
	for (int i = 0; i < NumThreads; i++)
	{
		IDs[i] = i;
		Array_Of_Thread_Handles[i] = CreateThread( NULL, 0, 
           ContextWorkerThread, &(IDs[i]), 0, NULL);
		if ( Array_Of_Thread_Handles[i] == NULL)
		{
			ExitProcess(IDs[i]);
		}
	}

	WaitForMultipleObjects( NumThreads, Array_Of_Thread_Handles, TRUE, INFINITE);
	system("pause");

	cm.printStats();

	return 0;
}

