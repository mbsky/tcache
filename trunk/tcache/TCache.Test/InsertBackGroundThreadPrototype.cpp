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


#include "TraceLoader.h"
#include "ContextManager.h"
#include "QDatabase.h"
#include "CachingAlgorithm.h"
#include "ConfigurationLoader.h"
#include "StatisticsManager.h"
#include "TCache.h"


EachSecond* BGESptr;
CRITICAL_SECTION* BGPL;
Vdt* BGDZ;


const int OBJECT_SIZE=1000;
long len,compressLen,unCompressLen; 
char *compressBuffer,*unCompressBuffer;



//Num of Semaphore is the same as the number of partitions
const int num_partitions=10;
HANDLE Semaphore;

extern DbEnv* memoryEnv;
extern void setupEnv( const ConfigParametersV& cp );
QDatabase qdb;

ContextManager cm2(1,1,4);
long num_signal=0;
long num_clean_to_dirty=0;


bool shutdown_flag=false;

TCache vcm;
//CompressionManager compressVcm;

int createDatazones()
{
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
		vcm.Create( &DZ[i] );
	}
	return 0;
}

DWORD WINAPI WorkerThreadInsertPrototype( LPVOID lpParam ) 
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

	tmpPtr = BGESptr;
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

				FileVdt = &BGDZ[ tmpPtr->zoneid[ID][i] ];

				if (VERBOSE) 
					printf("iter=%d, cnt=%d: ",iter_cnt,i);
								
				vdtKey.set_data( (char*)&tmpPtr->key[ID][i] );
				vdtKey.set_size( sizeof(int) );

				// Reset vdtGetValue to the buffer
				vdtGetValue.set_data( dataBuffer );
				vdtGetValue.set_size( MAX_OBJECT_SIZE );

				int ret;
				
				switch(tmpPtr->cmnd[ID][i])
				{
				case Get:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Get Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
					
					//ret=vcm.m_diskDB->Get(*FileVdt, vdtKey, &vdtGetValue );
					//vdtGetValue.set_size(tmpPtr->size[ID][i]);
					//ret=compressVcm.Get(FileVdt, vdtKey, &vdtGetValue );
					ret=vcm.Get(FileVdt, vdtKey, &vdtGetValue );
					if(ret==0)
					{
						if( tmpPtr->size[ID][i] != *(int*)vdtGetValue.get_data() )
						{
							printf("**ERROR** Retrieved value for key \"%d\" was incorrect\n", tmpPtr->key[ID][i]);
							printf("Expected %d Got %d \n",tmpPtr->size[ID][i],*(int*)vdtGetValue.get_data());
							//cout<<"Expected "<<tmpPtr->size[ID][i]<<" got "<<*(int*)vdtGetValue.get_data()<<endl;
						}
					}
					else{
						cout<<"Get failed Key "<<tmpPtr->key[ID][i]<<endl;
						if (ret==DB_BUFFER_SMALL)
							printf("Size should be %d\n",vdtGetValue.get_size());
							//	cout<<"Size should be "<<vdtGetValue.get_size()<<endl;
					}
					
							

					break;
				case Insert:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Insert Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
			
					ret=0;
					*(int*)cptr = tmpPtr->size[ID][i];
					vdtValue.set_size(tmpPtr->size[ID][i] );
					
					ret=vcm.Insert(FileVdt,vdtKey,vdtValue);	
					//ret=vcm.SynchronousInsert(*FileVdt,vdtKey,vdtValue,false);	
					//ret=compressVcm.Insert(FileVdt,vdtKey,vdtValue);	
					//ret=vcm.m_diskDB->Put(*FileVdt,vdtKey,vdtValue);	
					
					if(ret!=0) {
						printf("Insert failed key %d ret %d\n",tmpPtr->key[ID][i],ret);
					}
					break;
				case Delete:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Delete Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
					


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
			EnterCriticalSection(BGPL);
			//Synchronize with the main thread that might be populating the Prefetch List
			LeaveCriticalSection(BGPL);
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





int runBGPrototypeTrace(int argc, _TCHAR* argv[])
{
	int ret=0;

	ConfigParametersV cp;
	ConfigParametersTraceGen cpt;
	ConfigurationLoader config;
	

	if (argc==3)
	{
		USES_CONVERSION;
		config.loadVConfigFile(W2A(argv[1]),cp);
		config.loadTraceConfigFile(W2A(argv[2]),cpt);
	}
	else {
		config.loadVConfigFile( "V.xml", cp );
		config.loadTraceConfigFile( "TraceGen.xml", cpt );
	}
	/*if(argc == 2)
	{
		//cp.DiskBDB.CacheSize.Bytes=_ttoi( argv[1] )* 1024 * 1024;
		//printf("Disk cache size %d\n",cp.DiskBDB.CacheSize.Bytes);
		cp.MemBDB.CacheSize.Bytes = _ttoi( argv[1] ) * 1024 * 1024;
	}*/
	




	char st[]="Showvickkal The Project Gutenberg EBook of Searchlights on Health by B. G. Jefferis and J. L. Nichols  This eBook is for the use of anyone anywhere at no cost and with almost no restrictions whatsoever.  You may copy it, give it away or re-use it under the terms of the Project Gutenberg License included with this eBook or online at www.gutenberg.net   Title: Searchlights on Health        The Science of Eugenics  Author: B. G. Jefferis and J. L. Nichols  Release Date: September 12, 2004 [EBook #13444]  Language: English  Character set encoding: ASCII  *** START OF THIS PROJECT GUTENBERG EBOOK SEARCHLIGHTS ON HEALTH ***     Produced by Juliet Sutherland, Alicia Williams, and the Online Distributed Proofreading Team.SEARCHLIGHTS ON HEALTH  THE SCIENCE OF EUGENICS          *       *       *       *       *    A Guide to Purity and Physical Manhood   Advice to Maiden, Wife and Mother   Love, Courtship, and Marriage          *       *       * *       *   By  PROF. B.G. JEFFERIS, M.D., PH. D";
	//printf("strlen %d\n",strlen(st));
	compressBuffer=(char *) malloc (OBJECT_SIZE*2+1);
	unCompressBuffer=(char *) malloc (OBJECT_SIZE*2+1);

	len=(long) strlen(st)+1;
	compressLen=OBJECT_SIZE*2;
	//ret= compress ((Bytef *)compressBuffer,&compressLen, (const Bytef*)st, len);
	//printf("Compression lenght %d\n",compressLen);
	unCompressLen=OBJECT_SIZE*2;
	




	vcm.Initialize( cp );
	//compressVcm.Initialize(cp,&vcm);
	
	createDatazones();

	
	bool runTraceFile=true;

	if(runTraceFile)
	{
		//Run the trace
		int NumThreads=cpt.NumThreads;
		int PrefetchSeconds=cpt.PrefetchSeconds;
		int NumEltsPerThread=cpt.NumEltsPerThread;
		string trace_filename=cpt.filename;
		/*int NumThreads = 1;
		int PrefetchSeconds = 20;
		int NumEltsPerThread = 50000;

		//string trace_filename = "C:\\traces\\585\\Trace1M\\1MObjs.Save";
		//string trace_filename = "C:\\traces\\585\\Trace1M\\100K.Save";
		//string trace_filename = "C:\\traces\\585\\Trace99\\99Objs.Save";
		//string trace_filename = "C:\\traces\\585\\Trace99\\Trace99.1KGet";

		//string trace_filename = "C:\\jyap\\585\\Trace1M\\1MObjs.Save";
		//string trace_filename = "C:\\jyap\\585\\Trace99\\99Objs.Save";
		string trace_filename = "C:\\traces\\585\\MySpace\\trace_original.txt";
		//string trace_filename = "C:\\traces\\585\\MySpace\\simple_test.txt";
		//string trace_filename = "C:\\traces\\585\\MySpace\\RelayNodelog4091313.txt";
		//string trace_filename = "C:\\traces\\585\\Trace1M\\Trace1M.10KGet";
		//string trace_filename="C:\\traces\\585\\Trace1M\\10.Save";
		//string trace_filename="C:\\traces\\585\\Trace1M\\Trace1M.1MGet";
		*/

		TraceLoader trace( trace_filename, NumThreads, PrefetchSeconds, NumEltsPerThread, WorkerThreadInsertPrototype );


		BGESptr = trace.getESptr();
		BGPL = trace.getCriticalSection();
		BGDZ = trace.getDZ();

		for(int i=0;i<cpt.NumIterations;i++)
			ret = trace.run();

		/*vcm.Shutdown();


		vcm.Initialize( cp );

		NumThreads = 1;
		PrefetchSeconds = 20;
		NumEltsPerThread = 10000;
		trace_filename = "C:\\traces\\585\\Trace1M\\100K.Get";
		//trace_filename = "C:\\traces\\585\\Trace99\\99Objs.Get";
		TraceLoader trace_get( trace_filename, NumThreads, PrefetchSeconds, NumEltsPerThread, WorkerThreadInsertPrototype );

		BGESptr = trace_get.getESptr();
		BGPL = trace_get.getCriticalSection();
		BGDZ = trace_get.getDZ();

		ret = trace_get.run();
		*/
	}
	vcm.Shutdown();

	return ret;
}
