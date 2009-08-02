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

// TCLock.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <stdlib.h>
#include <fstream>
#include <string>
#include <iostream>
#include "TCLock.h"

#define MAXTHREADS 10
#define NUM_WORKER_THREADS 4
#define MAX_SLEEP_TIME 50

Vdt g_datazone, g_sharedResource, g_sharedResource2, g_writerLock;
int g_counter = 1;
double g_counter2 = 1.0;
const int g_numWorkerIterations = 50;
TCLock lock1(1000,4), lock2(1000,4);

DWORD WINAPI WorkerThreadLock( LPVOID lpParam ) 
{	
	// Thread ID
	int ID = *((int*)lpParam); 

	//std::string filename = "output" + intToString(ID) + ".txt";
	//fout.open( filename.c_str(), std::ofstream::out );

	HANDLE SyncEvent = OpenEvent ( EVENT_ALL_ACCESS , false, (LPCWSTR) "SyncEvent" );
	if (!SyncEvent) {
		printf ("WorkerThread %d:  Failed to establish the synchronization mechanism.\n", ID);
		return -1;
	}

	int temp = 0;
	std::string num_spaces = "";
	for( int j = 0; j < ID; j++ )
		num_spaces += ' ';

	for( int i = 0; i < g_numWorkerIterations; i++ )
	{
		lock1.acquireWait( g_datazone, g_sharedResource );
		//if( lock1.acquireFail( g_datazone, g_sharedResource ) != 0 )
		//	continue;

		Sleep( rand() % MAX_SLEEP_TIME );

		temp = g_counter;
		
		//std::cout<< "Thread: " << ID << " Counter: " << temp << std::endl;
		//std::cout.flush();
		//fout<< "Thread: " << ID << " Counter: " << temp << std::endl;
		//fout.flush();		

		Sleep( rand() % MAX_SLEEP_TIME );
			
		g_counter = temp + 1;		

		lock1.release( g_datazone, g_sharedResource );

		lock2.acquireWait( g_datazone, g_writerLock );
		std::cout<< num_spaces << "Thread: " << ID << " Counter: " << temp << std::endl;
		std::cout.flush();
		lock2.release( g_datazone, g_writerLock );
	}


	//Signal the main thread that this WorkerThread is done with the current element
	SetEvent(SyncEvent);

	return 0;
}

DWORD WINAPI WorkerThreadLock2( LPVOID lpParam ) 
{	
	// Thread ID
	int ID = *((int*)lpParam); 

	//std::string filename = "output" + intToString(ID) + ".txt";
	//fout.open( filename.c_str(), std::ofstream::out );

	HANDLE SyncEvent = OpenEvent ( EVENT_ALL_ACCESS , false, (LPCWSTR) "SyncEvent" );
	if (!SyncEvent) {
		printf ("WorkerThread %d:  Failed to establish the synchronization mechanism.\n", ID);
		return -1;
	}

	double temp = 0;

	std::string num_spaces = "";
	for( int j = 0; j < ID; j++ )
		num_spaces += ' ';

	for( int i = 0; i < g_numWorkerIterations; i++ )
	{
		lock1.acquireWait( g_datazone, g_sharedResource2 );
		//if( lock1.acquireFail( g_datazone, g_sharedResource2 ) != 0 )
		//	continue;

		Sleep( rand() % MAX_SLEEP_TIME );

		temp = g_counter2;
		
		//std::cout<< "Thread: " << ID << " Counter: " << temp << std::endl;
		//std::cout.flush();
		//fout<< "Thread: " << ID << " Counter: " << temp << std::endl;
		//fout.flush();
		Sleep( rand() % MAX_SLEEP_TIME );
			
		g_counter2 = temp + 0.01;		

		lock1.release( g_datazone, g_sharedResource2 );

		lock2.acquireWait( g_datazone, g_writerLock );
		std::cout<< num_spaces << "Thread: " << ID << " Counter: " << temp << std::endl;
		std::cout.flush();
		lock2.release( g_datazone, g_writerLock );
	}

	//Signal the main thread that this WorkerThread is done with the current element
	SetEvent(SyncEvent);

	return 0;
}

int lockTestMain()
{
	
	int NumThreads = NUM_WORKER_THREADS;
	int num_iterations = 0;

	// Random seed. Set to a constant value so all test runs are the same
	srand( 12 );	

	std::string dzName = "testLockDZ";
	g_datazone.set_data( (char*) dzName.c_str() );
	g_datazone.set_size( (int) dzName.length() );
	
	int temp = 17;
	g_sharedResource.set_data( (char*)&temp );
	g_sharedResource.set_size( sizeof(int) );

	int temp2 = 23;
	g_sharedResource2.set_data( (char*)&temp2 );
	g_sharedResource2.set_size( sizeof(int) );

	int temp3 = 43;
	g_writerLock.set_data( (char*)&temp3 );
	g_writerLock.set_size( sizeof(int) );



	int IDs[MAXTHREADS];
	HANDLE Array_Of_Thread_Handles[MAXTHREADS];
	HANDLE SyncEvent = CreateEvent ( NULL , false , false , (LPCWSTR) "SyncEvent" );
	if ( !SyncEvent ) { 
		printf("Error, failed to setup the synchronization mechanism.\n");
		return -1; 
	}

	// Create the threads
	for (int i = 0; i < NumThreads; i++)
	{
		IDs[i] = i;
		Array_Of_Thread_Handles[i] = CreateThread( NULL, 0, 
           WorkerThreadLock, &(IDs[i]), 0, NULL);
		if ( Array_Of_Thread_Handles[i] == NULL)
		{
			ExitProcess(IDs[i]);
		}
	}

	for (int i = NumThreads; i < NumThreads*2; i++)
	{
		IDs[i] = i;
		Array_Of_Thread_Handles[i] = CreateThread( NULL, 0, 
           WorkerThreadLock2, &(IDs[i]), 0, NULL);
		if ( Array_Of_Thread_Handles[i] == NULL)
		{
			ExitProcess(IDs[i]);
		}
	}

	//while( num_iterations < 100 )
	//{
		for (int i = 0; i < NumThreads*2; i++)
			WaitForSingleObject(SyncEvent, INFINITE);

		//num_iterations++;
	//}

	WaitForMultipleObjects( NumThreads*2, 
        Array_Of_Thread_Handles, TRUE, INFINITE);

	lock1.printStats();

	return 0;
}


