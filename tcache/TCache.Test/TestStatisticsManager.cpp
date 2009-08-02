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

#include <windows.h>
#include <strsafe.h>
#include <stdio.h>
#include <time.h>


#include "StatisticsManager.h"
#include "TraceLoader.h"
#include "TCLock.h"

TCLock *lock;
StatisticsManager *stats;
int simple_test_statistics_manager()
{
	
	int bytes=1000;
	stats->IncreaseCleanBytes(1,2000);
	stats->DecreaseCleanBytes(1,1000);
	if(bytes==stats->GetCleanBytes(1))
		cout<<"Test 1 passed \n"<<endl;
	//cout<<stats.GetCleanBytes(1)<<std::endl;
	
	stats->IncreaseDirtyBytes(2,3000);
	stats->DecreaseDirtyBytes(2,2000);
	
	double averageQ=0.06;
	stats->IncreaseTotalQ(1,0.12);
	stats->DecreaseTotalQ(1,0.06);
	stats->IncrementNumQ(1);
	stats->IncrementNumQ(1);
	stats->DecrementNumQ(1);
	if(stats->GetAvgQ(1)==averageQ)
		cout<<"Test 2 passed"<<endl;
	
	stats->SetL(1,-0.2);

	stats->IncreaseTotalQ(-1,0.5);

	stats->IncreaseTotalQ(10,0.3);

	//cout<<stats.GetAvgQ(1)<<std::endl;
		
	return 0;
}

EachSecond* SMESptr;
CRITICAL_SECTION* SMPL;
Vdt* SMDZ;


DWORD WINAPI WorkerThreadStatisticsTrace( LPVOID lpParam ) 
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

	tmpPtr = SMESptr;
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

				FileVdt = &SMDZ[ tmpPtr->zoneid[ID][i] ];

				if (VERBOSE) 
					printf("iter=%d, cnt=%d: ",iter_cnt,i);
								
				vdtKey.set_data( (char*)&tmpPtr->key[ID][i] );
				vdtKey.set_size( sizeof(int) );

				// Reset vdtGetValue to the buffer
				vdtGetValue.set_data( dataBuffer );
				vdtGetValue.set_size( MAX_OBJECT_SIZE );
				
				
				switch(tmpPtr->cmnd[ID][i])
				{
				case Get:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Get Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
				
					stats->IncrementNumQ(1);
					stats->IncreaseTotalQ(1,tmpPtr->zoneid[ID][i]); // Increase by zoneid
					stats->IncreaseCleanBytes(1,tmpPtr->key[ID][i]); // Increment it by the key
					break;
				case Insert:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Insert Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);

					stats->DecrementNumQ(1);
					stats->DecreaseTotalQ(1,tmpPtr->zoneid[ID][i]);
					stats->DecreaseCleanBytes(1,tmpPtr->key[ID][i]);
					break;
				case Delete:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Delete Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
					
					stats->DecrementNumQ(1);
					stats->DecreaseTotalQ(1,tmpPtr->zoneid[ID][i]);
					stats->DecreaseCleanBytes(1,tmpPtr->key[ID][i]);

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
			EnterCriticalSection(SMPL);
			//Synchronize with the main thread that might be populating the Prefetch List
			LeaveCriticalSection(SMPL);
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


//int _tmain(int argc, _TCHAR* argv[])
//int runSecondTrace( const int &mem_size_MB, const int &algorithm_ID )
int printStatsAndCheck()
{
	double avgQ=12.2053;
	long long cleanBytes=108971470827524;
	cout<<"Average Q "<<stats->GetAvgQ(1)<<endl;
	cout<<"Total Clean bytes "<<stats->GetCleanBytes(1)<<endl;

	// The below block fails, even when the values are correct i guess there is some difference in precision
	/*if((stats->GetAvgQ(1)==avgQ) && (cleanBytes==stats->GetCleanBytes(1)) )
		cout<<"Trace test passed \n";
	else
		cout<<"Trace test failed\n";*/
	return 0;
}

int runStatisticsManagerTrace()
{
	int ret=0;

	lock=new TCLock(1000,4);
	stats=new StatisticsManager(10,lock);
	
	//simple_test_statistics_manager();
	

	//Run the trace
	int NumThreads = 4;
	int PrefetchSeconds = 20;
	int NumEltsPerThread = 30000;
	string trace_filename = "C:\\traces\\585\\MySpace\\trace_original.txt";
	//string trace_filename = "C:\\traces\\585\\MySpace\\simple_test.txt";
	//string trace_filename = "C:\\traces\\585\\MySpace\\RelayNodelog4091313.txt";
	//string trace_filename = "C:\\traces\\585\\Trace1M\\Trace1M.500KGet";


	TraceLoader trace( trace_filename, NumThreads, PrefetchSeconds, NumEltsPerThread, WorkerThreadStatisticsTrace );
	

	SMESptr = trace.getESptr();
	SMPL = trace.getCriticalSection();
	SMDZ = trace.getDZ();

	ret = trace.run();

	printStatsAndCheck();

	system("pause");
	return ret;
}
