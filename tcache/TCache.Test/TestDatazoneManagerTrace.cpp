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

// 2ndTraceGen.cpp : Defines the entry point for the console application.
//


#include "stdafx.h"


#include <windows.h>
#include <strsafe.h>
#include <stdio.h>
#include <time.h>


#include "DatazoneManager.h"
#include "TraceLoader.h"
#include "ConfigParameters.h"

using namespace std;

DatazoneManager dzManager;
//TraceElts dzManagerTrace;

EachSecond* DZESptr;
CRITICAL_SECTION* DZPL;
Vdt* DZ_DZ;

int create_datazones()
{
	int ret;
	Vdt	DZ[NUMZONES];
	char FileName[NUMZONES];

	for (int i = 0; i < NUMZONES; i++) {
		FileName[i] = 'A'+i;
		DZ[i].set_data(&FileName[i]);
		DZ[i].set_size(sizeof(char));
	}

	//system("pause");
	for (int i = 0; i < NUMZONES; i++) {
		if (VERBOSE && DZ[i].get_data() != NULL)
		{
			printf("Invoke Create method of V with filename %c.\n",DZ[i].get_data()[0]);
		}

		ret=dzManager.Create( DZ[i] );
	}
	return 0;

}

bool close_handle_test()
{

	Vdt dz;
	char d[]="alpha";
	dz.set_data((char *)d);
	dz.set_size(strlen(d)+1);
	dzManager.Create(dz);

	Vdt key;
	int val=10;
	key.set_data((char *) &val);
	key.set_size(sizeof(int));

	Db *handle;
	dzManager.Get(dz,key,handle);
	
	dzManager.CloseHandle(dz,key);

	bool bool_ret=dzManager.checkIfHandleExistsInHashIndex(dz,key);
	if(bool_ret==false)
		cout<<"Close Handle Test passed"<<endl;
	else
		cout<<"Close Handle Test failed"<<endl;

	return bool_ret;
}

DWORD WINAPI WorkerThreadDatazoneTraceRandomOpenClose( LPVOID lpParam ) 
{
	char* dataBuffer = new char[MAX_OBJECT_SIZE];

	struct EachSecond *tmpPtr;
    int     ID = 0;
    int     count = 0;
	Vdt *FileVdt;
	Vdt vdtKey, vdtValue, vdtGetValue;
	char* cptr = dataBuffer;
	int iter_cnt = 1;
	
	vdtValue.set_data( dataBuffer );
	vdtGetValue.set_data( dataBuffer );
	vdtGetValue.set_size( MAX_OBJECT_SIZE );
	

    ID = *((int*)lpParam); 

	HANDLE SyncEvent = OpenEvent ( EVENT_ALL_ACCESS , false, (LPCWSTR) "SyncEvent" );
	if (!SyncEvent) {
		printf ("WorkerThread %d:  Failed to establish the synchronization mechanism.\n", ID);
		return -1;
	}

	tmpPtr = DZESptr;
	while (tmpPtr != NULL)
	{
		if (tmpPtr->Assigned[ID] == '1')
		{
			count = tmpPtr->NumTraceElts[ID];
			if (VERBOSE || DISPLAY_ITERATION )
				printf("WorkerThread %d, iter %d:  Number of elements to process is %d.\n", ID, iter_cnt, count);

			for (int i = 0; i < count; i++)
			{
				if (tmpPtr->zoneid[ID][i] > NUMZONES)
				{
					printf("Trace contains a zone-id (%d) that exceeds the constant NUMZONES.\n", tmpPtr->zoneid[ID][i]);
					printf("Aborting the experiment.  Increase NUMZONES to exceed zone-id, recompile, and re-run.\n");
					return -1;
				}

				FileVdt = &DZ_DZ[ tmpPtr->zoneid[ID][i] ];

				if (VERBOSE) 
					printf("iter=%d, cnt=%d: ",iter_cnt,i);
								
				vdtKey.set_data( (char*)&tmpPtr->key[ID][i] );
				vdtKey.set_size( sizeof(int) );

				// Reset vdtGetValue to the buffer
				vdtGetValue.set_data( dataBuffer );
				vdtGetValue.set_size( MAX_OBJECT_SIZE );

				int temp_int, temp_int2;
			
				Db* handle;
				int coin_flip,ret;	
				switch(tmpPtr->cmnd[ID][i])
				{
				case Get:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Get Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);


				case Insert:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Insert Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
		

				case Delete:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Delete Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
					coin_flip=rand()%2;
					if(coin_flip==0)
					{
						dzManager.Get(*FileVdt,vdtKey,handle);
						if(handle==NULL)
							cout<<"Unable to retreive handle"<<endl;
					}
					else
					{
						ret=dzManager.CloseHandle(*FileVdt,vdtKey);
						if(ret==0)
						{
							bool bool_ret=dzManager.checkIfHandleExistsInHashIndex(*FileVdt,vdtKey);
							if(bool_ret==true)
								cout<<"Close Handle failed"<<endl;
						}
					}
					break;
				default :
					printf("WorkerThread %d:  Error, command not recognized.",ID);
					break;
				}
			}
		}
		//If tmpPtr is null, try to enter a critical section to ensure the main thread is trully done
		if (tmpPtr->next == NULL)
		{
			//Sleep(1);
			EnterCriticalSection(DZPL);
			//Synchronize with the main thread that might be populating the Prefetch List
			LeaveCriticalSection(DZPL);
		}

		//Set the flag that this thread is done with the EachSecond element
		tmpPtr->Complete[ID] = '1';

		//Signal the main thread that this WorkerThread is done with the current element
		SetEvent(SyncEvent);

		tmpPtr = tmpPtr->next;
		iter_cnt ++;


	}

	if (VERBOSE || DISPLAY_ITERATION)
		printf("Thread %d is done and exiting.\n",ID);

	delete [] dataBuffer;
    
    return 0; 
}

DWORD WINAPI WorkerThreadDatazoneTrace( LPVOID lpParam ) 
{
	char* dataBuffer = new char[MAX_OBJECT_SIZE];

	struct EachSecond *tmpPtr;
    int     ID = 0;
    int     count = 0;
	Vdt *FileVdt;
	Vdt vdtKey, vdtValue, vdtGetValue;
	char* cptr = dataBuffer;
	int iter_cnt = 1;
	
	vdtValue.set_data( dataBuffer );
	vdtGetValue.set_data( dataBuffer );
	vdtGetValue.set_size( MAX_OBJECT_SIZE );
	

    ID = *((int*)lpParam); 

	HANDLE SyncEvent = OpenEvent ( EVENT_ALL_ACCESS , false, (LPCWSTR) "SyncEvent" );
	if (!SyncEvent) {
		printf ("WorkerThread %d:  Failed to establish the synchronization mechanism.\n", ID);
		return -1;
	}

	tmpPtr = DZESptr;
	while (tmpPtr != NULL)
	{
		if (tmpPtr->Assigned[ID] == '1')
		{
			count = tmpPtr->NumTraceElts[ID];
			if (VERBOSE || DISPLAY_ITERATION )
				printf("WorkerThread %d, iter %d:  Number of elements to process is %d.\n", ID, iter_cnt, count);

			for (int i = 0; i < count; i++)
			{
				if (tmpPtr->zoneid[ID][i] > NUMZONES)
				{
					printf("Trace contains a zone-id (%d) that exceeds the constant NUMZONES.\n", tmpPtr->zoneid[ID][i]);
					printf("Aborting the experiment.  Increase NUMZONES to exceed zone-id, recompile, and re-run.\n");
					return -1;
				}

				FileVdt = &DZ_DZ[ tmpPtr->zoneid[ID][i] ];

				if (VERBOSE) 
					printf("iter=%d, cnt=%d: ",iter_cnt,i);
								
				vdtKey.set_data( (char*)&tmpPtr->key[ID][i] );
				vdtKey.set_size( sizeof(int) );

				// Reset vdtGetValue to the buffer
				vdtGetValue.set_data( dataBuffer );
				vdtGetValue.set_size( MAX_OBJECT_SIZE );

				int temp_int, temp_int2;

				Db* handle;
				
				switch(tmpPtr->cmnd[ID][i])
				{
				case Get:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Get Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
					//tsm->Get( FileVdt, vdtKey, &vdtGetValue );
					dzManager.Get(*FileVdt,vdtKey,handle);

					if(handle==NULL)
							cout<<"Unable to retreive handle"<<endl;
					temp_int = *(int*)cptr;
					temp_int2 = tmpPtr->size[ID][i];

					// Check to make sure inserted value is correct
					//if( tmpPtr->size[ID][i] != *(int*)vdtGetValue.get_data() )
						//printf("**ERROR** Retrieved value for key \"%d\" was incorrect\n", tmpPtr->key[ID][i]);

					//tsm->DumpCache();
					break;
				case Insert:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Insert Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);

					//dzManager.Get(dzname,handle);
					//// Set the size as the value in the first 4 bytes of the data
					//*(int*)cptr = tmpPtr->size[ID][i];
					//vdtValue.set_size( tmpPtr->size[ID][i] );

					//tsm->Insert( FileVdt, vdtKey, vdtValue );
					

					break;
				case Delete:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Delete Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
					//dzManager.Get(dzname,handle);
					break;
				default :
					printf("WorkerThread %d:  Error, command not recognized.",ID);
					break;
				}
			}
		}
		//If tmpPtr is null, try to enter a critical section to ensure the main thread is trully done
		if (tmpPtr->next == NULL)
		{
			//Sleep(1);
			EnterCriticalSection(DZPL);
			//Synchronize with the main thread that might be populating the Prefetch List
			LeaveCriticalSection(DZPL);
		}

		//Set the flag that this thread is done with the EachSecond element
		tmpPtr->Complete[ID] = '1';

		//Signal the main thread that this WorkerThread is done with the current element
		SetEvent(SyncEvent);

		tmpPtr = tmpPtr->next;
		iter_cnt ++;


	}

	if (VERBOSE || DISPLAY_ITERATION)
		printf("Thread %d is done and exiting.\n",ID);

	delete [] dataBuffer;
    
    return 0; 
}

DbEnv * create_env()
{
	u_int32_t open_flags = 0;
	open_flags |= DB_CREATE;
	open_flags |= DB_THREAD;
	open_flags |= DB_INIT_MPOOL;
	open_flags |= DB_INIT_TXN |DB_LOG_AUTO_REMOVE|DB_THREAD;
	open_flags |= DB_INIT_LOG|DB_INIT_LOCK;

	string envHome("c:\\Env" );
	string dataDir("c:\\Env\\database");

	DbEnv *dataBaseEnv = new DbEnv( 0 );
	// Note: The directory structure has to be created before-hand,
	//		 otherwise BDB will have a dir not found error.
	dataBaseEnv->set_data_dir( dataDir.c_str());

	dataBaseEnv->open( envHome.c_str(), open_flags, 0 ); 

	return dataBaseEnv;
}

//int _tmain(int argc, _TCHAR* argv[])
//int runSecondTrace( const int &mem_size_MB, const int &algorithm_ID )
int runDatazoneTrace()
{

	DbEnv *DiskEnv=create_env();

	ConfigParametersV cp;
	memset( &cp, 0, sizeof(cp) );

	cp.DiskBDB.DatabaseConfigs.Partitions = 10;

	int ret=dzManager.Initialize(DiskEnv,cp);


	//Create the datazones
	create_datazones();


	//Run the trace
	int NumThreads = 8;
	int PrefetchSeconds = 20;
	int NumEltsPerThread = 30000;
	string trace_filename = "C:\\traces\\585\\MySpace\\trace_original.txt";
	//string trace_filename = "C:\\traces\\585\\MySpace\\simple_test.txt";
	//string trace_filename = "C:\\traces\\585\\MySpace\\RelayNodelog4091313.txt";
	//string trace_filename = "C:\\traces\\585\\Trace1M\\Trace1M.500KGet";


	TraceLoader trace( trace_filename, NumThreads, PrefetchSeconds, NumEltsPerThread, WorkerThreadDatazoneTrace );
	//TraceLoader trace( trace_filename, NumThreads, PrefetchSeconds, NumEltsPerThread, WorkerThreadDatazoneTraceRandomOpenClose );

	DZESptr = trace.getESptr();
	DZPL = trace.getCriticalSection();
	DZ_DZ = trace.getDZ();

	ret = trace.run();

	//close_handle_test();

	dzManager.Shutdown();
	
	// Close the envirnment
	DiskEnv->close(0);
	
	system("pause");
	return ret;
}



	/*bool TraceEOF = false;
	bool OneSecondDone = false;
	int IDs[MAXTHREADS];
	struct EachSecond *ArrayOfTraces = (EachSecond *) malloc (sizeof(EachSecond) * PrefetchSeconds);
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

	//Initialize the V storage manager with appropriate amount of cache size
	if (VERBOSE)
		printf("Initialize the V storage manager with appropriate amount of cache size.\n");


	//Initialize the Prefetch List critical section
	InitializeCriticalSection(&PL);

	//Initialize the DZ data structure
	for (int i = 0; i < NUMZONES; i++) {
		FileName[i] = 'A'+i;
		DZ[i].set_data(&FileName[i]);
		DZ[i].set_size(sizeof(char));
	}

	//system("pause");
	for (int i = 0; i < NUMZONES; i++) {
		if (VERBOSE && DZ[i].get_data() != NULL)
		{
			printf("Invoke Create method of V with filename %c.\n",DZ[i].get_data()[0]);
		}

		ret=dzManager.Create( DZ[i] );
	}

	//system("pause");
	//Initialize the ArrayOfTraces
	ESptr = ArrayOfTraces;
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
	
	GetOneTraceElt *scanner = new GetOneTraceElt(trace_filename);

	for (int i = 0; i < PrefetchSeconds && TraceEOF != true; i++)
	{
		if (PopulateOneSecond2(&ArrayOfTraces[i], scanner, NumThreads, NumEltsPerThread) < 0)
		{
			printf("PopulateOneSecond failed during prefetch stage when populating element %d.\n",i);
			TraceEOF = true;
		}
	}

	for (int i = 0; i < NumThreads; i++)
	{
		IDs[i] = i;
		Array_Of_Thread_Handles[i] = CreateThread( NULL, 0, 
           WorkerThreadDatazoneTrace, &(IDs[i]), 0, NULL);
		if ( Array_Of_Thread_Handles[i] == NULL)
		{
			ExitProcess(IDs[i]);
		}
	}

	//time_begin = time(NULL);
	QueryPerformanceCounter( &begin );

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
				if (PopulateOneSecond2(current, scanner, NumThreads, NumEltsPerThread) < 0)
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

	//time_end = time(NULL);
	QueryPerformanceCounter( &end );



	//system("pause");
	scanner->ShutDown();
	
	dzManager.Shutdown();
	
	//Shutdown the V Storage Manager
	if (VERBOSE) printf("Shutdown the V storage manager.\n");

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

    if (VERBOSE) printf("Since All threads executed" 
           " lets close their handles \n");

	for (int i = 0; i < NumThreads; i++)
		CloseHandle(Array_Of_Thread_Handles[i]);

	//cout<< "Total time taken: " << time_end - time_begin << " seconds\n";
	cout<< "Total time taken: " << (double)(end.QuadPart - begin.QuadPart)/num_ticks.QuadPart * 1000 << " miliseconds\n";	
*/
	 


