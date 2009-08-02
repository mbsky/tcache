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
#include "DiskBDB.h"
#include "TCLock.h"
#include "ConfigParameters.h"

using namespace std;

DatazoneManager dznameManager;
DiskBDB diskDB;
//TraceElts dzManagerTrace;
TCLock lock_disk(1000,4);

EachSecond* DiskESptr;
CRITICAL_SECTION* DiskPL;
Vdt* DiskDZ;

DbEnv *disk_envp;

Db* global_dbp=NULL;
Db* global_dbp2=NULL;

CRITICAL_SECTION display;

int create_disk_datazones()
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
		ret=dznameManager.Create( DZ[i] );
	}
	return 0;
}


Db * openDbHandle(char * dz_name){
	Db *disk_dbp=NULL;
	disk_dbp = new Db(disk_envp, 0);

	// Now open the database */
	int open_flags = DB_AUTO_COMMIT;          // Allow autocommit
	//int open_flags=0;

	try{
	disk_dbp->open(NULL,       // Txn pointer
		"B",   // File name
		NULL,       // Logical db name
		DB_HASH,   // Database type (using hash)
		open_flags,  // Open flags
		0);         // File mode. Using defaults
	}catch(DbException &e) {
       cout << e.what() << endl;
        //e.get_errno()
	}
	return disk_dbp;
}

int Insert1 (const Vdt& dzname, const Vdt& Key, const Vdt& Value)
{
	Db *disk_dbp=NULL;
	float q=0.0;
	//flag to mean that some other on disk db was in use
	int flag=0;    
	try{
		Vdt dz=dzname;
		char* dz_name=(char *)(dz.get_data());

		disk_dbp = openDbHandle(dz_name);
		flag = 1;
		      
		Vdt key1=Key,value1=Value;
		char* key=(char *) (key1.get_data());
		char* value= (char *) value1.get_data();       
		int size= value1.get_size();

		

		//Now insert the key and data to the disk. All insert go to the disk
		//Specify the disk key the data key
		Dbt disk_key(key, strlen(key)+1);
		Dbt disk_data(value,size);
		disk_dbp->put(NULL,&disk_key,&disk_data,0);
		//close some other db
		if (flag){
			disk_dbp->close(0);
		}

	}
	catch(DbException &e) {
		cout << e.what() << endl;
		if (flag) disk_dbp->close(0);
		return (-1);
	} catch(exception &e) {
		cout << e.what() << endl;
		return (-1);
	}   
	return 0;
}

DWORD WINAPI WorkerThreadDiskTrace( LPVOID lpParam ) 
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

	tmpPtr = DiskESptr;
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

				FileVdt = &DiskDZ[ tmpPtr->zoneid[ID][i] ];

				if (VERBOSE) 
					printf("iter=%d, cnt=%d: ",iter_cnt,i);
								
				vdtKey.set_data( (char*)&tmpPtr->key[ID][i] );
				vdtKey.set_size( sizeof(int) );

				// Reset vdtGetValue to the buffer
				vdtGetValue.set_data( dataBuffer );
				vdtGetValue.set_size( MAX_OBJECT_SIZE );

				int temp_int, temp_int2;
				int ret;
				Db* handle;
				
				switch(tmpPtr->cmnd[ID][i])
				{
				case Get:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Get Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
					ret=diskDB.Get(*FileVdt, vdtKey, &vdtGetValue );
					if(ret==0)
					{
						temp_int = *(int*)cptr;
						temp_int2 = tmpPtr->size[ID][i];
					
						if( tmpPtr->size[ID][i] != *(int*)vdtGetValue.get_data() )
						{
							printf("**ERROR** Retrieved value for key \"%d\" was incorrect\n", tmpPtr->key[ID][i]);
							cout<<"Expected "<<tmpPtr->size[ID][i]<<" got "<<*(int*)vdtGetValue.get_data()<<endl;
						}
					}
					else{
						cout<<"Get failed"<<endl;
						if (ret==DB_BUFFER_SMALL)
							cout<<"Size should be "<<vdtGetValue.get_size()<<endl;
					}
					//tsm->Get( FileVdt, vdtKey, &vdtGetValue );
					//dznameManager.Get(*FileVdt,vdtKey,handle);

					//if(handle==NULL)
					//		cout<<"Unable to retreive handle"<<endl;
					//temp_int = *(int*)cptr;
					//temp_int2 = tmpPtr->size[ID][i];

					// Check to make sure inserted value is correct
					//if( tmpPtr->size[ID][i] != *(int*)vdtGetValue.get_data() )
						//printf("**ERROR** Retrieved value for key \"%d\" was incorrect\n", tmpPtr->key[ID][i]);

					//tsm->DumpCache();
					break;
				case Insert:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Insert Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
					
					// Set the size as the value in the first 4 bytes of the data
					*(int*)cptr = tmpPtr->size[ID][i];
					vdtValue.set_size(tmpPtr->size[ID][i] );
					//if( lock_disk.acquireWait( *FileVdt, vdtKey ) == 0 )
					//{
						ret=0;
						ret=diskDB.Put(*FileVdt,vdtKey,vdtValue);
					//	lock_disk.release( *FileVdt, vdtKey );
					//}

					if(ret!=0)
						cout<<"Insert failed"<<endl;
					//dzManager.Get(dzname,handle);
					//// Set the size as the value in the first 4 bytes of the data
					//*(int*)cptr = tmpPtr->size[ID][i];
					//vdtValue.set_size( tmpPtr->size[ID][i] );

					//tsm->Insert( FileVdt, vdtKey, vdtValue );

					/*{
						// Set the value
						int size=tmpPtr->size[ID][i];
						char *value= new char[size];
						sprintf_s(value,size,"%d",size);
						Vdt Value;
						Value.set_size(size);
						Value.set_data(value);
						
						char key[20];
						sprintf_s(key,20,"%d",tmpPtr->key[ID][i]);
						Vdt Key;
						Key.set_size(strlen(key)+1);
						Key.set_data(key);
						
						int ret=Insert1 (*FileVdt, Key, Value);
						if(ret==0 ) cout<<"Insert Successful"<<endl;
						else
							cout<<"***Error maybe their were deadlock"<<endl;
						free(value);
					}*/
					

					break;
				case Delete:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Delete Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
					ret=diskDB.Delete(*FileVdt,vdtKey);
					if(ret!=0)
						cout<<"Delete failed"<<endl;
					
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
			EnterCriticalSection(DiskPL);
			//Synchronize with the main thread that might be populating the Prefetch List
			LeaveCriticalSection(DiskPL);
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


DbEnv * create_disk_env()
{
	u_int32_t open_flags = 0;
	open_flags |= DB_CREATE;
	open_flags |= DB_THREAD;
	open_flags |= DB_INIT_MPOOL;
	open_flags |= DB_INIT_TXN |DB_LOG_AUTO_REMOVE;
	open_flags |=	DB_INIT_LOG|DB_INIT_LOCK;
	
	//open_flags |= DB_INIT_TXN ;

	string envHome("c:\\DiskEnv" );
	string dataDir("c:\\DiskEnv\\database");

	FILE *outfile;
	char *outname = "err_file.txt";
	outfile = fopen(outname, "w");
	DbEnv *dataBaseEnv = new DbEnv( 0 );
	// Note: The directory structure has to be created before-hand,
	//		 otherwise BDB will have a dir not found error.
	dataBaseEnv->set_data_dir( dataDir.c_str());
	dataBaseEnv->set_cachesize(0,256*1024*1024,1);
	dataBaseEnv->set_lk_detect(DB_LOCK_MINWRITE);
	//dataBaseEnv->d`
	dataBaseEnv->set_errfile(outfile);
	dataBaseEnv->open( envHome.c_str(), open_flags, 0 ); 

	return dataBaseEnv;
}


//int _tmain(int argc, _TCHAR* argv[])
//int runSecondTrace( const int &mem_size_MB, const int &algorithm_ID )
int runDiskTrace()
{
	InitializeCriticalSection( &display);

	DbEnv *DiskEnv=create_disk_env();
	ConfigParametersV cp;
	memset( &cp, 0, sizeof(cp) );

	cp.DiskBDB.DatabaseConfigs.Partitions = 10;
	cp.DiskBDB.MaxDeadlockRetries = 10;

	int ret=dznameManager.Initialize(DiskEnv,cp);
	//int ret=dznameManager.Initialize(DiskEnv,10/5/3);
	diskDB.Initialize(DiskEnv,&dznameManager,cp);

	//Create the datazones
	create_disk_datazones();
	
	
	Db* DbHandle;
	//ret=dznameManager.GetHandleForPartition("B0",2,DbHandle);
	if(ret!=0)
	{
		cout<<"Datazone handle not retrieved from datazone manager"<<endl;
		return ret;
	}
	// Create another envirnment to orthgonal to the code. 
	// This enviroment is passed to the put as a global variable. 
	
	int open_flags =
			DB_CREATE     |  /* Create the environment if it does not exist */
			DB_INIT_LOCK  |  /* Initialize the locking subsystem */
			DB_INIT_LOG   |  /* Initialize the logging subsystem */
			DB_INIT_MPOOL |  /* Initialize the memory pool (in-memory cache) */
			DB_INIT_TXN   |
			DB_LOG_AUTO_REMOVE|
			//DB_RECOVER|
			DB_THREAD;

		// Create and open the environment
        disk_envp = new DbEnv(0);
		
	disk_envp->set_cachesize(0, 256*1024*1024, 1);
	
	disk_envp->set_lk_detect(DB_LOCK_MINWRITE);
    disk_envp->open(NULL, open_flags, 0);

	

	// Initialize the B0 database for the open and close put everytime
	Db *dbp= new Db( disk_envp, 0 );
	
	open_flags = DB_CREATE|DB_AUTO_COMMIT;           // Allow autocommit
	ret = dbp->open(	NULL, 
					//(dzname+intToString(i)).c_str(),
					"B0",
					NULL,
					DB_HASH,		
					open_flags,
					0 );
	if( ret != 0 )
		return ret;

	dbp->close(0);
	delete dbp;		


	/*Vdt number;
	int num=10;
	number.set_data((char *)&num);
	number.set_size(sizeof(int));*/

	//Initialize the global databse for trying with if global handles works 
	global_dbp=new Db( disk_envp, 0 );
	open_flags=DB_CREATE|DB_AUTO_COMMIT;  
	ret = global_dbp->open(	NULL, 
					//(dzname+intToString(i)).c_str(),
					"C0",
					NULL,
					DB_HASH,		
					open_flags,
					0 );
	
	if( ret != 0 )
		return ret;

	global_dbp2=new Db(disk_envp,0);
	open_flags=DB_CREATE|DB_AUTO_COMMIT;
	ret = global_dbp2->open(	NULL, 
					//(dzname+intToString(i)).c_str(),
					"C0",
					NULL,
					DB_HASH,		
					open_flags,
					0 );
	
	
	if( ret != 0 )
		return ret;
	
	//Test 1 check if alpha read first time from catalog and then from the hash table
	printf("Test 1 check if alpha read first time from catalog and then from the hash table\n");
	Vdt dzname;
	char *name="alpha";
	dzname.set_data(name);
	dzname.set_size(strlen(name));

	//dznameManager.Create(dzname);
	short a=-1;
	a=dznameManager.GetNumPartition(dzname);
	cout<<"Number of partitions first time "<<a <<endl;

	a=dznameManager.GetNumPartition(dzname);
	cout<<"Number of partitions second time "<<a <<endl;



	//Test 2  Run the trace
	printf("Test 2 Run the trace \n");
	int NumThreads = 1;
	int PrefetchSeconds = 20;
	int NumEltsPerThread = 1000;
	//string trace_filename = "C:\\traces\\585\\Trace1M\\1MObjs.Save";
	//string trace_filename = "C:\\traces\\585\\Trace99\\99Objs.Save";
	string trace_filename = "C:\\traces\\585\\Trace99\\Trace99.10KGet";
	//string trace_filename = "C:\\traces\\585\\Trace99\\99Objs.Delete";
	//string trace_filename = "C:\\traces\\585\\MySpace\\trace_original.txt";
	//string trace_filename = "C:\\traces\\585\\MySpace\\simple_test.txt";
	//string trace_filename = "C:\\traces\\585\\MySpace\\RelayNodelog4091313.txt";
	//string trace_filename = "C:\\traces\\585\\Trace1M\\Trace1M.500KGet";


	TraceLoader trace( trace_filename, NumThreads, PrefetchSeconds, NumEltsPerThread, WorkerThreadDiskTrace );
	//TraceLoader trace( trace_filename, NumThreads, PrefetchSeconds, NumEltsPerThread, WorkerThreadDatazoneTraceRandomOpenClose );

	DiskESptr = trace.getESptr();
	DiskPL = trace.getCriticalSection();
	DiskDZ = trace.getDZ();

	ret = trace.run();

	//close_handle_test();

	

	/*
	// Test 3 Use the existing alpha datazones create a datazone show with 5 partitions and vick with 3 
	// check whether the retrieval is correct. 
	
	// ***** Ensure that the datazones are created ****** 
	printf("Test if show has 5 partitions\n");
	Vdt show;
	char *name1="show";
	show.set_data(name1);
	show.set_size(strlen(name1));

	//dznameManager.Create(show);
	short a_show=-1;
	a_show=dznameManager.GetNumPartition(show);
	cout<<"Number of partitions first time "<<a_show<<endl;

	a_show=dznameManager.GetNumPartition(show);
	cout<<"Number of partitions second time "<<a_show<<endl;


	printf("\nTest if vick has 3 partitions\n");
	Vdt vick;
	char *name2="vick";
	vick.set_data(name2);
	vick.set_size(strlen(name2));

	//dznameManager.Create(vick);
	short a_vick=-1;
	a_vick=dznameManager.GetNumPartition(vick);
	cout<<"Number of partitions first time "<<a_vick<<endl;

	a_vick=dznameManager.GetNumPartition(vick);
	cout<<"Number of partitions second time "<<a_vick<<endl;
	*/


	dznameManager.Shutdown();
	
	try{

		// Close the actual code envirnment
		DiskEnv->close(0);


		// Close the disk environment close the global_dbp
		if(global_dbp!=NULL)
		{
			global_dbp->close(0);
			delete global_dbp;		
		}
		if(global_dbp2!=NULL)
		{
			global_dbp2->close(0);
			delete global_dbp2;
		}

		disk_envp->close(0);

	}
	catch(DbException &e) {
		cout << e.what() << endl;
		ret=e.get_errno();
	} catch(exception &e) {
		cout << e.what() << endl;
		ret=-1;
	}   
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
	 


