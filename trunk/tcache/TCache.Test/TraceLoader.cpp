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

#include "stdafx.h"
#include "TraceLoader.h"
#include "TCache.h"


TraceLoader::TraceLoader(	std::string filename, 
							const int& iNumThreads, 
							const int& iPrefetchSeconds,
							const int& iNumEltsPerThread,
							LPTHREAD_START_ROUTINE worker_thread_function, 
							char separator )
	: m_separator(separator)
{
	m_filename = filename;
	NumThreads = iNumThreads;
	PrefetchSeconds = iPrefetchSeconds;
	NumEltsPerThread = iNumEltsPerThread;
	m_workerThreadFunction = worker_thread_function;

	InitializeCriticalSection( &PL );

	// Initialize memory for Array of EachSecond
	ArrayOfTraces = (EachSecond *) malloc (sizeof(EachSecond) * PrefetchSeconds);
	//ArrayOfTraces = new EachSecond[PrefetchSeconds];

	for (int i = 0; i < MAXTHREADS; i++)
	{
		Util[i].BytesFromMem = Util[i].BytesInserted = Util[i].TotalByte = Util[i].TotalReq = Util[i].TotalReqFromMem = 0;
	}

	//Initialize the DZ data structure
	for (int i = 0; i < NUMZONES; i++) {
		FileName[i] = 'A'+i;
		DZ[i].set_data(&FileName[i]);
		DZ[i].set_size(sizeof(char));
	}
}

TraceLoader::~TraceLoader()
{
	//Free the allocated memory
	for (int i = 0; i < PrefetchSeconds ; i++)
	{
		for (int j = 0; j < NumThreads; j++)
		{
			free(ArrayOfTraces[i].key[j]);
			free(ArrayOfTraces[i].zoneid[j]);
			free(ArrayOfTraces[i].size[j]);
			free(ArrayOfTraces[i].cmnd[j]);
		}
	}
	free(ArrayOfTraces);
}


int TraceLoader::PopulateOneSecond(struct EachSecond *elt, GetOneTraceElt *scanner, int numthreads, int EltsPerThread)
{
	bool TraceEOF = false;
	if (elt == NULL || scanner == NULL)
	{
		printf("Error in PopulateOneSecond, either elt or scanner is NULL.\n");
		return -1;
	}

	for (int i = 0; i < numthreads; i++)
	{
		if (elt->NumTraceElts == NULL || elt->Complete == NULL || elt->Assigned == NULL)
		{
			printf("Error, either NumTraceElts or Complete or Assigned is NULL.\n");
			return -1;
		}

		elt->NumTraceElts[i] = 0;
		elt->Complete[i] = '0';
		elt->Assigned[i] = '0';
	}

	for (int i = 0; i < numthreads && !TraceEOF; i++)
	{
		for (int j = 0; j < EltsPerThread && !TraceEOF; j++)
		{
			(elt->key[i])[j] = scanner->GetKey();
			(elt->zoneid[i])[j] = scanner->GetDZid();
			(elt->size[i])[j] = scanner->GetSZ();
			elt->NumTraceElts[i]++;

			switch (scanner->GetCommand())
			{
				case 'G':
					(elt->cmnd[i])[j] = Get;
					break;
				case 'S':
					(elt->cmnd[i])[j] = Insert;
					break;
				case 'D':
					(elt->cmnd[i])[j] = Delete;
					break;
				default :
					printf("Command %c is unknown.\n", scanner->GetCommand());
					break;
			}

			if (scanner->GetNext() < 0){
				if (VERBOSE) printf("GetNext method of scanner encountered an error - most likely and EOF.\n");
				TraceEOF = true;
			}
		}
	}

	for (int i = 0; i < numthreads; i++)
		if (elt->NumTraceElts[i] > 0) elt->Assigned[i] = '1';
	
	if (TraceEOF) return -1;
	
	return 1;
}

int TraceLoader::run()
{
	LARGE_INTEGER begin, end, num_ticks;
	QueryPerformanceFrequency( &num_ticks);
	//QueryPerformanceFrequency(&TCache::m_num_ticks);

	bool TraceEOF = false;
	bool OneSecondDone = false;
	int IDs[MAXTHREADS];

	struct EachSecond *current, *last;
	HANDLE Array_Of_Thread_Handles[MAXTHREADS];
	HANDLE SyncEvent = CreateEvent ( NULL , false , false , (LPCWSTR) "SyncEvent" );
	    if ( !SyncEvent ) { 
		printf("Error, failed to setup the synchronization mechanism.\n");
		return -1; 
	}

	//Error checking
	if (NumThreads > MAXTHREADS){
		printf("Number of threads %d may not exceed MAXTHREADS %d.\n", NumThreads, MAXTHREADS);
		printf("Increase the constant MAXTHREADS to %d, recompile and run again.", NumThreads+1);
		exit(1);
	}

	//Initialize the ArrayOfTraces
	for (int i = 0; i < PrefetchSeconds ; i++)
	{
		ArrayOfTraces[i].next = &ArrayOfTraces[i+1];
		for (int j = 0; j < NumThreads; j++)
		{
			ArrayOfTraces[i].key[j] = (int *) malloc(sizeof(int) * NumEltsPerThread);
			ArrayOfTraces[i].zoneid[j] = (int *) malloc(sizeof(int) * NumEltsPerThread);
			ArrayOfTraces[i].size[j] = (int *) malloc(sizeof(int) * NumEltsPerThread);
			ArrayOfTraces[i].cmnd[j] = (cmndType *) malloc(sizeof(cmndType) * NumEltsPerThread);
			ArrayOfTraces[i].NumTraceElts[j] = 0;
			ArrayOfTraces[i].Assigned[j] = '0';
			ArrayOfTraces[i].Complete[j] = '0';
		}
	}
	ArrayOfTraces[PrefetchSeconds-1].next = NULL;
	current = &ArrayOfTraces[0];
	last = &ArrayOfTraces[PrefetchSeconds-1];
	
	// Open the input stream for the trace file
	GetOneTraceElt *scanner = new GetOneTraceElt(m_filename, m_separator);

	for (int i = 0; i < PrefetchSeconds && TraceEOF != true; i++)
	{
		if (PopulateOneSecond(&ArrayOfTraces[i], scanner, NumThreads, NumEltsPerThread) < 0)
		{
			printf("PopulateOneSecond failed during prefetch stage when populating element %d.\n",i);
			TraceEOF = true;
		}
	}

	for (int i = 0; i < NumThreads; i++)
	{
		IDs[i] = i;
		Array_Of_Thread_Handles[i] = CreateThread( NULL, 0, 
           m_workerThreadFunction, &(IDs[i]), 0, NULL);
		if ( Array_Of_Thread_Handles[i] == NULL)
		{
			ExitProcess(IDs[i]);
		}
	}

	QueryPerformanceCounter( &begin );
	//QueryPerformanceCounter (&TCache::m_start);

	while (!TraceEOF) {
		for (int i = 0; i < NumThreads; i++)
			WaitForSingleObject(SyncEvent, INFINITE);

		EnterCriticalSection(&PL);

		OneSecondDone = true;
		while (OneSecondDone)
		{
			for (int i = 0; i < NumThreads; i++)
				if (current->Complete[i] == '0') OneSecondDone = false;

			if (OneSecondDone){
				if (PopulateOneSecond(current, scanner, NumThreads, NumEltsPerThread) < 0)
				{
					//If the trace file has been exhausted then we are done
					OneSecondDone = false;
					TraceEOF = true;
				}
				
				//Fix the linked list of prefetch elements
				last->next = current;
				current = current->next;
				last = last -> next;
				last->next = NULL;
			}
		}
		LeaveCriticalSection(&PL);
	}

	    
	WaitForMultipleObjects( NumThreads, 
        Array_Of_Thread_Handles, TRUE, INFINITE);

	QueryPerformanceCounter( &end );

	scanner->ShutDown();

	//Shutdown the V Storage Manager
	if (VERBOSE) printf("Shutdown the V storage manager.\n");

	

    if (VERBOSE) printf("Since All threads executed" 
           " lets close their handles \n");

	for (int i = 0; i < NumThreads; i++)
		CloseHandle(Array_Of_Thread_Handles[i]);

	//cout<< "Total time taken: " << time_end - time_begin << " seconds\n";
	cout<< "Total time taken: " << (double)(end.QuadPart - begin.QuadPart)/num_ticks.QuadPart * 1000 << " miliseconds\n";	

	ofstream fout;
	fout.open( "Runtimes.txt", ofstream::out | ofstream::app );
	fout<< "Total time taken: " << (double)(end.QuadPart - begin.QuadPart)/num_ticks.QuadPart * 1000 << " miliseconds\n";
	fout.close();
	fout.clear();

	/* Prints Stats about the Cache Manager:  Total Number of Requests, Cache Hit Rate, Byte Hit Rate, */
	int TotalReq=0;
	int TotalReqFromMem = 0;
	long long BytesFromMem =0;
	long long BytesInserted =0;
	long long TotalByte = 0;
	for (int i=0; i< MAXTHREADS; i++)
	{
		TotalReq += Util[i].TotalReq;
		TotalReqFromMem += Util[i].TotalReqFromMem;
		BytesFromMem += Util[i].BytesFromMem;
		BytesInserted += Util[i].BytesInserted;
		TotalByte += Util[i].TotalByte;
	}
	printf("\n The Chach Manager statictics:");
	printf("\n\t Total Number of Reque(sts: %d ",TotalReq);
	printf("\n\t Cach Hit Rate: %f ", ((double)TotalReqFromMem/(double)TotalReq)*100.0);
	printf("\n\t Byte Hit Rate: %f \n", ((double)BytesFromMem/(double)TotalByte)*100.0);

	//ofstream fout;
	fout.open( "Memcached-Experiments.csv", ofstream::app );

	fout<< "TotalReq,TotalReqFromMem,BytesFromMem,BytesInserted,TotalBytes,CacheHitRate,ByteHitRate\n";
	fout<< TotalReq << ","
		<< TotalReqFromMem << ","
		<< BytesFromMem << ","
		<< BytesInserted << ","
		<< TotalByte << ","
		<< ((double)TotalReqFromMem/(double)TotalReq)*100.0 << ","
		<< ((double)BytesFromMem/(double)TotalByte)*100.0 << ","
		<< endl;
	
	fout.close();

	return -1;
}