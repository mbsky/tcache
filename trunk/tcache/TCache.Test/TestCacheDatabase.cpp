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
#include "CacheDatabase.h"
#include "ConfigurationLoader.h"
#include "TCLock.h"
#include "TraceLoader.h"

#include <iostream>
using namespace std;

int sizeof_rec_in = 1024, sizeof_rec_out = 1024;

extern DbEnv* memoryEnv;

CacheDatabase* cachedb_ptr;
ContextManager* cdb_cm_ptr;
TCLock	lock_cachedb(1000, 4);

// Global variables used by worker thread
//LARGE_INTEGER g_timestamp;
int g_cdb_numPartitions;
bool g_cdb_insertStage;
//extern char g_headerFlag;

EachSecond* cdbESptr;
CRITICAL_SECTION* cdbPL;
Vdt* cdb_DZ;

void setupEnv( const ConfigParametersV& cp );
void simpleTestCacheDatabase( DbEnv* memEnv, ContextManager& cm, const ConfigParametersV& cp );
void testCacheDbLoop( DbEnv* memEnv, ContextManager& cm, const ConfigParametersV& cp );
void testCacheDBtraceTests( DbEnv* memoryDbEnv, const ConfigParametersV& cp, ContextManager& cm );
DWORD WINAPI WorkerThreadCacheDBTrace( LPVOID lpParam );

void testCacheDatabaseMain()
{
	ConfigParametersV cp;
	ConfigurationLoader config;
	config.loadVConfigFile( "V_test_cache.xml", cp );

	ContextManager cm(1,1,4);

	setupEnv( cp );

	// Test the basic functions
	//simpleTestCacheDatabase( memoryEnv, cm, cp );

	//Test Filling single loop
	//testCacheDbLoop( memoryEnv, cm, cp );

	// Test with multiple threads and trace file
	testCacheDBtraceTests( memoryEnv, cp, cm );
	
	//return -1;
};


void simpleTestCacheDatabase( DbEnv* memEnv, ContextManager& cm, const ConfigParametersV& cp )
{
	cout<< "-- CacheDatabase Test: Running simple tests --\n";

	CacheDatabase cache_db;
	cache_db.Initialize( memEnv, cp );

	ContextStructure* c_struct, *c_struct2;
	cm.GetFreeContext( c_struct );
	cm.GetFreeContext( c_struct2 );

	CacheKey cacheKey( c_struct );
	CacheValue cacheValue( c_struct ), CacheRetrieveVal( c_struct2 );

	string strDzName = "testCacheDz", strKey = "testCacheKey";
	Vdt dzName = TCUtility::convertToVdt( strDzName );
	Vdt key = TCUtility::convertToVdt( strKey );

	int bufsize = 1024;
	char bufVal[1024];
	char bufValres[1024];
	char data[] = "121212";
	memcpy(&bufVal, &data, 7); 

	LARGE_INTEGER timestamp;
	QueryPerformanceCounter( &timestamp );

	cacheKey.setValues(dzName, key );
	cacheValue.setDataRecValues( &bufVal, bufsize );
	CacheRetrieveVal.setDataRecValues( &bufValres, 1024 );

	if( cache_db.InsertDataRec( cacheKey, cacheValue, 0 ) != 0 )
		cout<< "Error. Could not insert\n";

	if( cache_db.GetDataRec( cacheKey, CacheRetrieveVal, 0 ) != 0 )
		cout<< "Error. Could not get\n";

	//get Data_rec value from Cache
	void* data_p; 
	char* rvalue;
	int rsize = 0;
	CacheRetrieveVal.getDataRec(data_p, rsize);
	rvalue = (char*)data_p;

	//Set MD values to insert in Cache
	char dirtybit = '1';
	double qval = (double)1/1024;
	cacheKey.setValues(dzName, key );
	cacheValue.setMDValues( dirtybit, qval );
	//CacheRetrieveVal.setMDValues( &bufValres, bufsize );

	if( cache_db.InsertMDLookup( cacheKey, cacheValue, 0 ) != 0 )
		cout<< "Error. Could not insert\n";

	if( cache_db.GetMDLookup( cacheKey, CacheRetrieveVal, 0 ) != 0 )
		cout<< "Error. Could not get\n";

	//get MD values value from Cache
	char dirty = CacheRetrieveVal.getDirtyBit();
	//res += sizeof(dirty);
	TCQValue qback = CacheRetrieveVal.getQValue();

	cacheKey.setValues(dzName, key );
	if( cache_db.DeleteDataRec( cacheKey, 0 ) != 0 )
		cout<< "Error. Could not delete\n";

	if( cache_db.GetDataRec( cacheKey, CacheRetrieveVal, 0 ) == 0 )
		cout<< "Error. Deleted record was found\n";

	
	cacheKey.setValues( dzName, key );
	if( cache_db.DeleteMDLookup( cacheKey, 0 ) != 0 )
		cout<< "Error. Could not delete\n";

	if( cache_db.GetMDLookup( cacheKey, CacheRetrieveVal, 0 ) == 0 )
		cout<< "Error. Deleted record was found\n";

	cache_db.Shutdown();

	cm.ReleaseContext( c_struct );
	cm.ReleaseContext( c_struct2 );

	cout<< "-- CacheDatabase Test: End of simple tests. --\n";
}

void testCacheDbLoop( DbEnv* memEnv, ContextManager& cm, const ConfigParametersV& cp )
{
	cout<< "-- CacheDatabase Test: Running loop tests. --\n";

	CacheDatabase cache_db;
	cache_db.Initialize( memEnv, cp );

	int num_ofrecords = 1000000000;
	int number_of_partitions = cp.MemBDB.DatabaseConfigs.Partitions;
	LARGE_INTEGER begin, end, num_ticks;
	QueryPerformanceFrequency( &num_ticks );

	ContextStructure* c_struct, *c_struct2;
	cm.GetFreeContext( c_struct );
	cm.GetFreeContext( c_struct2 );

	CacheKey cacheKey( c_struct );
	CacheValue cacheValue( c_struct ), CacheRetrieveVal( c_struct2 );

	string strDzSeedName = "testLoopDz", strKey = "testLoopKey", strValue = "testLoopValue";
	Vdt dzName, key, Value; // = TCUtility::convertToVdt( strDzName );
	string sDZName, sKey, sValue;

	
	int partition_number;
	int dataRec_inserted = 0, dataRec_retrieved = 0;
	int md_inserted = 0, md_retrieved = 0;
	int dataRec_deleted = 0, md_deleted = 0;
	TCQValue qval(0, 0.0);
	char dirtybit = '1';
	char cleanbit = '0';
	bool quitloop = false;
	char* dataBuf = new char[sizeof_rec_in];
	
	std::cout << "Inserting records in Cache..." << std::endl;

	QueryPerformanceCounter( &begin );
	for(int i = 1; i < num_ofrecords && !quitloop; i++)
	{			
		sDZName = strDzSeedName + TCUtility::intToString(i);
		dzName = TCUtility::convertToVdt( sDZName );
		sKey = strKey + TCUtility::intToString(i);
		key = TCUtility::convertToVdt( sKey );
		
		sValue = strValue + TCUtility::intToString(i);
		memcpy(dataBuf, (char*)sValue.c_str(), sValue.length());
		Value.set_data(dataBuf);
		Value.set_size(sizeof_rec_in);
		//Value = TCUtility::convertToVdt( sValue );

		cacheKey.setValues( dzName, key );
		cacheValue.setDataRecValues( Value );

		partition_number = i % number_of_partitions;
		if( cache_db.InsertDataRec( cacheKey, cacheValue, partition_number) == 0 )
			dataRec_inserted++;
		else
		{
			std::cout << "Cache Data_rec is full" << std::endl;
			quitloop = true;
		}

		qval = (double)1 / (i * number_of_partitions);
		
		if(i/num_ofrecords == 0)
			cacheValue.setMDValues( dirtybit, qval );
		else
			cacheValue.setMDValues( cleanbit, qval );

		if( cache_db.InsertMDLookup( cacheKey, cacheValue, partition_number) == 0 )
			md_inserted++;
		else
		{
			std::cout << "Cache MD is full" << std::endl;
			quitloop = true;
		}
	}
	QueryPerformanceCounter( &end );
	cout << "Records Inserted in DataRec: " << dataRec_inserted << std::endl;
	cout << "Records Inserted in MD: " << md_inserted << std::endl;
	cout << "Time taken (seg): " << (double)(end.QuadPart - begin.QuadPart) / num_ticks.QuadPart << std::endl << std::endl;
	delete [] dataBuf;
	dataBuf = NULL;


	string sDZName2, sKey2;
	dataBuf = new char[sizeof_rec_out];
	//qval = 0.0;
	qval.setFraction(0.0);
	qval.setInt(0);
	TCQValue qvalr(0,0.0);
	quitloop = false;
	std::cout << "Retriving records from Cache..." << std::endl;
	char bit;
	string datar;
	
	QueryPerformanceCounter( &begin );
	for(int i = 1; i < num_ofrecords && !quitloop; i++)
	{
		sDZName2 = strDzSeedName + TCUtility::intToString(i);
		dzName = TCUtility::convertToVdt( sDZName2 );
		sKey2 = strKey + TCUtility::intToString(i);
		key = TCUtility::convertToVdt( sKey2 );
		
		sValue = strValue + TCUtility::intToString(i);
		
		Value.set_data(dataBuf);
		Value.set_size(sizeof_rec_out);
		
		cacheKey.setValues( dzName, key );
		CacheRetrieveVal.setDataRecValues( Value );

		partition_number = i % number_of_partitions;
		if( cache_db.GetDataRec( cacheKey, CacheRetrieveVal, partition_number) == 0 )
			dataRec_retrieved++;
		else
		{
			std::cout << "Records in Data_rec not Found" << std::endl;
			quitloop = true;
			break;
		}

		datar.clear();
		datar.insert(0, (char*)Value.get_data(), sValue.length());
		if(sValue.compare(datar) != 0)
			std::cout << "Record's value in Data_rec don't match" << std::endl;

		qval.setFraction( (double)1 / (i * number_of_partitions) );
		if( cache_db.GetMDLookup( cacheKey, CacheRetrieveVal, partition_number) == 0 )
			md_retrieved++;
		else
		{
			std::cout << "Records in MD not Found" << std::endl;
			quitloop = true;
			break;
		}
		qvalr = CacheRetrieveVal.getQValue();
		if(qval != qvalr)
			std::cout << "Wrong Qval Found" << std::endl;

		bit = CacheRetrieveVal.getDirtyBit();
		if(i/num_ofrecords == 0)
			dirtybit = '1';
		else
			dirtybit = '0';

		if(bit != dirtybit)
			std::cout << "Wrong DirtyBit Found" << std::endl;

	}
	QueryPerformanceCounter( &end );

	cout << "Records retrieved from DataRec: " << dataRec_retrieved << std::endl;
	cout << "Records retrieved from MD: " << md_retrieved << std::endl;
	cout << "Time taken (seg): " << (double)(end.QuadPart - begin.QuadPart) / num_ticks.QuadPart  << std::endl << std::endl;
	delete [] dataBuf;
	dataBuf = NULL;

	qval = 0.0;
	quitloop = false;
	//Deletes records
	QueryPerformanceCounter( &begin );
	for(int i = 1; i < num_ofrecords && !quitloop ; i++)
	{
		sDZName2 = strDzSeedName + TCUtility::intToString(i);
		dzName = TCUtility::convertToVdt( sDZName2 );
		sKey2 = strKey + TCUtility::intToString(i);
		key = TCUtility::convertToVdt( sKey2 );

		cacheKey.setValues( dzName, key );

		partition_number = i % number_of_partitions;
		if( cache_db.DeleteDataRec( cacheKey, partition_number) == 0 )
			dataRec_deleted++;
		else
		{
			std::cout << "Records in Data_rec not deleted" << std::endl;
			quitloop = true;
			break;
		}

		qval = (double)1 / (i * number_of_partitions);
		if( cache_db.DeleteMDLookup( cacheKey, partition_number) == 0 )
			md_deleted++;
		else
		{
			std::cout << "Records in MD not deleted" << std::endl;
			quitloop = true;
			break;
		}

	}

	QueryPerformanceCounter( &end );

	cout << "Records deleted from Data_Rec: " << dataRec_deleted << std::endl;
	cout << "Records deleted from MD: " << md_deleted << std::endl;
	cout << "Time taken (seg): " << (double)(end.QuadPart - begin.QuadPart) / num_ticks.QuadPart  << std::endl;

	cm.ReleaseContext( c_struct );
	cm.ReleaseContext( c_struct2 );
	
	
	cout<< "-- CacheDatabase Test: End of loop tests. --\n";
}


void testCacheDBtraceTests( DbEnv* memoryDbEnv, const ConfigParametersV& cp, ContextManager& cm )
{
	cout<< "-- CacheDatabase Test: Running trace tests --\n";

	// Set up Cache Database
	CacheDatabase cache_db;
	cache_db.Initialize( memoryDbEnv, cp );

	cachedb_ptr = &cache_db;
	cdb_cm_ptr = &cm;
	//QueryPerformanceCounter( &g_timestamp );
	//g_headerFlag = 'C';
	g_cdb_numPartitions = cp.MemBDB.DatabaseConfigs.Partitions;

	// Set up Trace Loader
	int NumThreads = 2; 
	int PrefetchSeconds = 20;
	int NumEltsPerThread = 50000;
	//string trace_filename = "C:\\jyap\\585\\MySpace\\trace_original.txt";
	string trace_filename = "C:\\jyap\\585\\Trace1M\\Trace1M.1MGet";
	//string trace_filename = "C:\\jyap\\585\\Trace1M\\1MObjs.Save";
	
	
	cout<< "Inserting records from trace\n";
	TraceLoader traceInsert( trace_filename, NumThreads, PrefetchSeconds, NumEltsPerThread, WorkerThreadCacheDBTrace, ',' );

	// These functions must be called to allow the WorkerThread to see the loaded data objects
	cdbESptr = traceInsert.getESptr();
	cdbPL = traceInsert.getCriticalSection();
	cdb_DZ = traceInsert.getDZ();

	g_cdb_insertStage = true;
	int ret = traceInsert.run();

	cout<< "\n Retrieving inserted records \n";
	TraceLoader traceGet( trace_filename, NumThreads, PrefetchSeconds, NumEltsPerThread, WorkerThreadCacheDBTrace, ',' );

	// These functions must be called to allow the WorkerThread to see the loaded data objects
	cdbESptr = traceGet.getESptr();
	cdbPL = traceGet.getCriticalSection();
	cdb_DZ = traceGet.getDZ();

	g_cdb_insertStage = false;
	ret = traceGet.run();


	lock_cachedb.printStats();

	cache_db.Shutdown();

	cout<< "-- CacheDatabase Test: End of trace tests --\n";
}

DWORD WINAPI WorkerThreadCacheDBTrace( LPVOID lpParam ) 
{
	
	char* dataBuffer = new char[MAX_OBJECT_SIZE];

	struct EachSecond *tmpPtr;
    int     ID = 0;
    int     count = 0;
	Vdt *FileVdt;
	Vdt vdtKey, vdtValue, vdtGetValue;
	char* cptr = dataBuffer;
	int iter_cnt = 1;

	string resvalue;
	char* heyptr;

	bool continueLoop = true;
	int numData_inserted = 0, numMD_inserted = 0;
	int numData_retrieved = 0, numMD_retrieved = 0;
	int numData_deleted = 0, numMD_deleted = 0;
	int total_record_size = 0;
	int ret = -1;
	
	/*vdtValue.set_data( dataBuffer );
	vdtValue.set_size( MAX_OBJECT_SIZE );
	vdtGetValue.set_data( dataBuffer );
	vdtGetValue.set_size( MAX_OBJECT_SIZE );*/
	

    ID = *((int*)lpParam); 

	HANDLE SyncEvent = OpenEvent ( EVENT_ALL_ACCESS , false, (LPCWSTR) "SyncEvent" );
	if (!SyncEvent) {
		printf ("WorkerThread %d:  Failed to establish the synchronization mechanism.\n", ID);
		return -1;
	}

	ContextStructure* c_struct = NULL;
	cdb_cm_ptr->GetFreeContext( c_struct );

	CacheKey cacheKey( c_struct );
	CacheValue cacheValue( c_struct );
	CacheValue cacheMDValue( c_struct );

	tmpPtr = cdbESptr;
	while (tmpPtr != NULL && continueLoop)
	{
		if (tmpPtr->Assigned[ID] == '1')
		{
			count = tmpPtr->NumTraceElts[ID];
			if (VERBOSE || DISPLAY_ITERATION )
				printf("WorkerThread %d, iter %d:  Number of elements to process is %d.\n", ID, iter_cnt, count);

			for (int i = 0; i < count && continueLoop; i++)
			{
				if (tmpPtr->zoneid[ID][i] > NUMZONES)
				{
					printf("Trace contains a zone-id (%d) that exceeds the constant NUMZONES.\n", tmpPtr->zoneid[ID][i]);
					printf("Aborting the experiment.  Increase NUMZONES to exceed zone-id, recompile, and re-run.\n");
					return -1;
				}

				FileVdt = &cdb_DZ[ tmpPtr->zoneid[ID][i] ];

				if (VERBOSE) 
					printf("iter=%d, cnt=%d: ",iter_cnt,i);

				int curr_key = tmpPtr->key[ID][i];
								
				vdtKey.set_data( (char*)&tmpPtr->key[ID][i] );
				vdtKey.set_size( sizeof(int) );

				int temp_int, temp_int2;

				// Initialize the key to be inserted into the Q database
				cacheKey.setValues( *FileVdt, vdtKey );

				// Initialize insert values. During Get stage, these are used to verify correctness
				int record_size = tmpPtr->size[ID][i];

				// Reset vdtGetValue to the buffer
				vdtValue.set_data( dataBuffer );
				vdtValue.set_size( record_size );

				LARGE_INTEGER timestamp;
				//timestamp.QuadPart = g_timestamp.QuadPart + curr_key;

				//Set Data cache Value
				cacheValue.setDataRecValues( vdtValue );

				double qval = (double)1 / (curr_key * g_cdb_numPartitions);
				char dbit = '1';
				if( g_cdb_insertStage )
				{					
					cacheMDValue.setMDValues( dbit, qval );
				}
				

				switch(tmpPtr->cmnd[ID][i])
				{
				case Get:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Get Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
					//tsm->Get( FileVdt, vdtKey, &vdtGetValue );
					if( lock_cachedb.acquireWait( *FileVdt, vdtKey ) == 0 )
					{
						
						if( g_cdb_insertStage )
						{
							ret = cachedb_ptr->InsertDataRec( cacheKey, cacheValue, curr_key % g_cdb_numPartitions );
							if( ret == 0 )
							{
								numData_inserted++;	
								
								//Insert MD record
								ret = cachedb_ptr->InsertMDLookup( cacheKey, cacheMDValue, curr_key % g_cdb_numPartitions );
								if( ret == 0 )
								{
									numMD_inserted++;	
								}
								else
								{
									if(ret != DB_LOCK_DEADLOCK)
									{
										cout << "Error Code: " << ret << std::endl;
										continueLoop = false;
									}
								}
							}
							else
							{
								if(ret != DB_LOCK_DEADLOCK)
								{
									cout << "Error Code: " << ret << std::endl;
									continueLoop = false;
								}
							}							
						}
						else
						{
							//DELETE
							//ret = cachedb_ptr->DeleteDataRec( cacheKey, curr_key % g_cdb_numPartitions );
							//if( ret == 0 )
							//{
							//	numData_deleted++;
							//	
							//	//cout << "key: " << curr_key << endl;
							//}
							//else
							//{
							//	//continueLoop = false;
							//	/*f(ret == DB_NOTFOUND)
							//		continueLoop = false;
							//	else*/
							//		//cout << "Error Code in DataRec: " << ret << std::endl;
							//}

							//ret = cachedb_ptr->DeleteMDLookup( cacheKey, curr_key % g_cdb_numPartitions );
							//	if( ret == 0 )
							//	{
							//		numMD_deleted++;
							//		//cout << "key: " << curr_key << endl;
							//	}
							//	else
							//	{
							//		//cout << "Error Code in MD: " << ret << std::endl;
							//		//continueLoop = false;
							//	}

							//GETS
							ret = cachedb_ptr->GetDataRec( cacheKey, cacheValue, curr_key % g_cdb_numPartitions );
							if( ret == 0 )
							{
								numData_retrieved++;
								ret = cachedb_ptr->GetMDLookup( cacheKey, cacheMDValue, curr_key % g_cdb_numPartitions );
								if( ret == 0 )
								{
									numMD_retrieved++;
									//cout << "key: " << curr_key << endl;
								}
								else
								{
									if( ret == DB_LOCK_DEADLOCK )
										cout << "Error Code: " << ret << std::endl;
									//continueLoop = false;
								}
								//cout << "key: " << curr_key << endl;
							}
							else
							{
								//if(ret == DB_NOTFOUND)
								//	continueLoop = false;
								//else
								if( ret == DB_LOCK_DEADLOCK )
									cout << "Error Code: " << ret << std::endl;
							}

							
						}

						lock_cachedb.release( *FileVdt, vdtKey );

						// Verify correctness of inserted values
						//if( !g_cdb_insertStage && ret == 0 )
						//{
						//	if( cacheValue.getDataRecSize() != record_size )
						//		cout<< "Error. Retrieved record had the wrong value for Record Size\n";

						//	if( cacheMDValue.getQValue() != qval )
						//		std::cout << "Wrong Qval Found in Metadata" << std::endl;

						//	if(cacheMDValue.getDirtyBit() != dbit)
						//		std::cout << "Wrong DirtyBit Found in Metadata" << std::endl;

						//	total_record_size += record_size;
						//	//std::cout << "Records size: " << record_size;
						//}
					}

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

					//// Set the size as the value in the first 4 bytes of the data
					// *(int*)cptr = tmpPtr->size[ID][i];
					//vdtValue.set_size( tmpPtr->size[ID][i] );

					//tsm->Insert( FileVdt, vdtKey, vdtValue );
					if( lock_cachedb.acquireWait( *FileVdt, vdtKey ) == 0 )
					{
						Sleep(0);
						lock_cachedb.release( *FileVdt, vdtKey );
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
			//EnterCriticalSection(&PL);
			//Synchronize with the main thread that might be populating the Prefetch List
			//LeaveCriticalSection(&PL);

			EnterCriticalSection(cdbPL);
			//Synchronize with the main thread that might be populating the Prefetch List
			LeaveCriticalSection(cdbPL);
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

	if( g_cdb_insertStage )
	{
		cout << "Records Inserted in DataRec: " << numData_inserted << std::endl;
		cout << "Records Inserted in MD: " << numMD_inserted << std::endl;
	}
	else
	{
		cout << "Records deleted from DataRec: " << numData_deleted << std::endl;
		cout << "Records deleted from MD: " << numMD_deleted << std::endl;

		cout << "Records retrieved from DataRec: " << numData_retrieved << std::endl;
		cout << "Records retrieved from MD: " << numMD_retrieved << std::endl;
		//cout << "Total Records size in Cache: " << total_record_size << std::endl;		
	}

	cdb_cm_ptr->ReleaseContext( c_struct );
	//*/
    
    return 0; 
}
