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
#include "TCache.h"
#include "ConfigurationLoader.h"
#include "TraceLoader.h"

#include <iostream>
#include <fstream>

//#include <winsock2.h>

#include <stdio.h>

#ifdef memcacheTest
//#include "..\..\memcached-1.2.6-win32\memcached.h"
//win32\cacheAPI.h"
#include <memcached.h>
#define SERVER_NAME "127.0.0.1"
#define SERVER_PORT 11211
#define KEY "test"
#define VALUE1 "example"
#define VALUE2 "1"
#endif

using namespace std;

// Global variables used by worker thread
TCache* vcm_ptr;
TraceLoader* g_traceLoader;
//LARGE_INTEGER g_timestamp;
//int g_numPartitions;
int g_vcm_traceStage;
int	g_vcm_iteration;
//char g_headerFlag;
//ofstream* vcm_fout;

EachSecond* vcmESptr;
CRITICAL_SECTION* vcmPL;
Vdt* vcm_DZ;

int testTCMain( ConfigParametersV& cp, ConfigParametersTraceGen& cpt );
void testTCsimpleTests( TCache* vcm );
void testTCloopTests( TCache* vcm );
void testTCtraceTests( TCache* vcm );
void testTCtraceTests( TCache* vcm, ConfigParametersTraceGen& cpt );
DWORD WINAPI WorkerThreadTCTrace( LPVOID lpParam );
DWORD WINAPI WorkerThreadMemCachedExperiment( LPVOID lpParam );


int testTCMain()
{
	ConfigParametersV cp;
	ConfigurationLoader config;
	config.loadVConfigFile( "V.xml", cp );

	TCache vcm;
	vcm.Initialize( cp );

	// Test the basic functions
	//testTCsimpleTests( &vcm );

	// Test with a loop
	//testTCloopTests( &vcm );

	// Test with multiple threads and trace file
	testTCtraceTests( &vcm);

	//cm.printStats();	

	vcm.Shutdown();

	return -1;
}

int testTCMain( ConfigParametersV& cp, ConfigParametersTraceGen& cpt )
{
	//ConfigParametersV cp;
	//ConfigurationLoader config;
	//config.loadVConfigFile( "V.xml", cp );

	TCache vcm;
	vcm.Initialize( cp );

	// Test the basic functions
	//testTCsimpleTests( &vcm );

	// Test with a loop
	//testTCloopTests( &vcm );

	// Test with multiple threads and trace file
	testTCtraceTests( &vcm, cpt );

	//cm.printStats();	

	vcm.Shutdown();

	return -1;
}

void testTCsimpleTests( TCache* vcm )
{
	cout<< "-- TCache Test: Running simple tests --\n";

	string strDzName = "testVDz", strKey = "testVKey", strValue = "testVValue";
	Vdt dzName = TCUtility::convertToVdt( strDzName );
	Vdt key = TCUtility::convertToVdt( strKey );
	Vdt value = TCUtility::convertToVdt( strValue );

	if( vcm->Create( &dzName ) != 0 )
		if( false )
			cout<< "Error creating datazone\n";

	const int bufferSize = 1024;
	char dataBuffer[bufferSize];
	Vdt retrieveVal( dataBuffer, bufferSize );

	// Insert a key and check that the inserted values are correct
	if( vcm->Insert( &dzName, key, value ) != 0 )
		cout<< "Error. Could not insert\n";

	if( vcm->Get( &dzName, key, &retrieveVal ) != 0 )
		cout<< "Error. Could not get\n";

	if( retrieveVal.get_size() != value.get_size() )
		cout<< "Error. Did not get the right size\n";

	if( memcmp( retrieveVal.get_data(), value.get_data(), value.get_size() ) != 0 )
		cout<< "Error. Retrieved value was not the same as the inserted value\n";

	// Insert a key to a different datazone and check the inserted value
	int intDzName = 'A' + 4;
	Vdt dzName2( (char*)&intDzName, sizeof(intDzName) );
	int intKey = 1478;
	int intValue = 172893;

	Vdt key2( (char*)&intKey, sizeof(intKey) );
	Vdt value2( (char*)&intValue, sizeof(intValue) );

	if( vcm->Create( &dzName2 ) != 0 )
		if( false )
			cout<< "Error creating datazone\n";

	// Insert a key and check that the inserted values are correct
	if( vcm->Insert( &dzName2, key2, value2 ) != 0 )
		cout<< "Error. Could not insert\n";

	retrieveVal.set_data( dataBuffer );
	retrieveVal.set_size( bufferSize );

	if( vcm->Get( &dzName2, key2, &retrieveVal ) != 0 )
		cout<< "Error. Could not get\n";

	if( retrieveVal.get_size() != value2.get_size() )
		cout<< "Error. Did not get the right size\n";

	if( memcmp( retrieveVal.get_data(), value2.get_data(), value2.get_size() ) != 0 )
		cout<< "Error. Retrieved value was not the same as the inserted value\n";

	// Try and find the key using the wrong datazone. Should not be found
	if( vcm->Get( &dzName2, key, &retrieveVal ) == 0 )
		cout<< "Error. Found a key in the wrong datazone\n";

	if( vcm->Get( &dzName, key2, &retrieveVal ) == 0 )
		cout<< "Error. Found a key in the wrong datazone\n";

	retrieveVal.set_data( dataBuffer );
	retrieveVal.set_size( bufferSize );

	// Get an object that should be in the cache
	if( vcm->Get( &dzName, key, &retrieveVal ) != 0 )
		cout<< "Error. Could not get object in cache\n";

	vcm->DumpCache( cout );

	/*
	// Delete a key and check that its gone
	if( vcm.Delete( qKey, 0 ) != 0 )
		cout<< "Error. Could not delete\n";

	if( vcm.Get( qKey, qRetrieveVal, 0 ) == 0 )
		cout<< "Error. Deleted record was found\n";
	//*/

	cout<< "-- TCache Test: End of simple tests --\n";
}


void testTCloopTests( TCache* vcm )
{
	cout<< "-- TCache Test: Running loop tests --\n";

	int max_num_iterations = 1000;
	bool skipinsert = true;
	int num_reads = max_num_iterations;


	bool continueLoop = true;
	int ret = -1;

	int num_inserted = 0;
	int num_found = 0;
	int num_deleted = 0;

	char* dataBuf = new char[MAX_OBJECT_SIZE];

	// Initiazlie Q database
	//TCache vcm;
	//vcm.Initialize( memoryDbEnv, cp );

	// Key parameters
	string sDzNameBase = "testTCLoop", sKeyBase = "testTCLoopKey", sValueBase = "testTCLoopValue";
	string sDzName, sKey, sValue;
	Vdt dzName, key, Value;
	string sdata_retrieve;

	// Value parameters
	int record_size_in, record_size_out;

	//LARGE_INTEGER timestamp_base, timestamp_in, timestamp_out;
	//QueryPerformanceCounter( &timestamp_base );

	sDzName = sDzNameBase + TCUtility::intToString(0);
	dzName = TCUtility::convertToVdt( sDzName );

	if( !skipinsert )
	{
		vcm->Create( &dzName );
		// Insert until full
		for( int i = 0; i < max_num_iterations && continueLoop; i++ )
		{
			try
			{
				//sDzName = sDzNameBase + TCUtility::intToString(i);
				//dzName = TCUtility::convertToVdt( sDzName );
				sKey = sKeyBase + TCUtility::intToString(i);
				key = TCUtility::convertToVdt( sKey );
				sValue = sValueBase + TCUtility::intToString(i);
				//Value = TCUtility::convertToVdt( sValue );				
				
				memcpy(dataBuf, (char*)sValue.c_str(), sValue.length());
				Value.set_data( dataBuf );
				Value.set_size( MAX_OBJECT_SIZE );

				//use to create multiple data zones
				//vcm->Create( &dzName );

				ret = vcm->Insert( &dzName, key, Value );
				//if( vcm->Insert( &dzName, key, Value ) == 0 )
				if( ret == 0 )
				{
					num_inserted++;
				}
				else
				{
					cout<< "- TC is full(failed to insert more) -\n";
					continueLoop = false;
				}
			}
			catch( exception& e )
			{
				cout<< e.what();

				continueLoop = false;
			}
		}
	}
	
	cout<< "# of records inserted: " << num_inserted << endl;

	// Try and retrieve inserted records
	continueLoop = true;
	if( skipinsert )
		num_inserted = num_reads;
	for( int j = 0; j < 2; j++ )
	{
		continueLoop = true;

		for( int i = 0; i < num_inserted && continueLoop; i++ )
		{
			try
			{
				//sDzName = sDzNameBase + TCUtility::intToString(i);
				//dzName = TCUtility::convertToVdt( sDzName );
				sKey = sKeyBase + TCUtility::intToString(i);
				key = TCUtility::convertToVdt( sKey );
				sValue = sValueBase + TCUtility::intToString(i);
				//Value = TCUtility::convertToVdt( sValue );				

				Value.set_data( dataBuf );
		Value.set_size( MAX_OBJECT_SIZE );
				//Value.set_size( sValue.length() );

				if( vcm->Get( &dzName, key, &Value ) == 0 )
				{
					num_found++;

					// Check if the retrieved value was correct
					record_size_out = Value.get_size();
					//record_size_in = sValue.length();
					record_size_in = MAX_OBJECT_SIZE;
					
					if( record_size_out != record_size_in )
						cout<< "Error. Retrieved record had the wrong value for Record Size\n";
					
					sdata_retrieve.clear();
					sdata_retrieve.insert(0, (char*)Value.get_data(), sValue.length());
					if(sValue.compare(sdata_retrieve) != 0)
						std::cout << "Record's value in Data_rec don't match" << std::endl;

				}
				else
				{
					cout<< "Error. Could not retrieve inserted record (" << i << ")\n";
					continueLoop = false;
				}
			}
			catch( exception& e )
			{
				cout<< e.what();

				continueLoop = false;
			}
		}
	}
	delete [] dataBuf;
	cout<< "# of records found: " << num_found << endl;

	

	// Try and delete inserted records
	/*continueLoop = true;
	for( int i = 0; i < num_inserted && continueLoop; i++ )
	{
		try
		{
			sDzName = sDzNameBase + TCUtility::intToString(i);
			dzName = TCUtility::convertToVdt( sDzName );
			sKey = sKeyBase + TCUtility::intToString(i);
			key = TCUtility::convertToVdt( sKey );

			qKey.setValues( headerFlag, q_value*i, dzName, key );

			if( vcm.Delete( qKey, i % num_partitions ) == 0 )
			{
				num_deleted++;
			}
			else
			{
				cout<< "Error. Could not delete inserted record (" << i << ") \n";
			}
		}
		catch( exception& e )
		{
			cout<< e.what();

			continueLoop = false;
		}
	}

	cout<< "# of records deleted: " << num_deleted << endl;*/


	//vcm->Shutdown();
	vcm->DumpCache( cout );
	cout<< "-- TCache Test: End of loop tests --\n";
}

void testTCtraceTests( TCache* vcm )
{
	ConfigParametersTraceGen cpt;
	ConfigurationLoader configLoader;
	configLoader.loadTraceConfigFile( "VTestTraceGen.xml", cpt );

	testTCtraceTests( vcm, cpt );
}

void testTCtraceTests( TCache* vcm, ConfigParametersTraceGen& cpt )
{
	cout<< "-- TCache Test: Running trace tests --\n";

	

	vcm_ptr = vcm;
	int ret = -1;

	// Set up Trace Loader
	//int NumThreads = 1; 
	int NumThreads = cpt.NumThreads; 
	//int PrefetchSeconds = 20;
	int PrefetchSeconds = cpt.PrefetchSeconds;
	//int NumEltsPerThread = 2000;
	int NumEltsPerThread = cpt.NumEltsPerThread;
	//string trace_filename = "C:\\jyap\\585\\MySpace\\trace_original.txt";
	//string trace_filename = "C:\\jyap\\585\\Trace1M\\Trace1M.1MGet";
	//string trace_filename = "C:\\jyap\\585\\Trace1M\\Trace1M.10KGet";
	//string trace_filename = "C:\\jyap\\585\\Trace99\\99Objs.Save";
	//string trace_filename = "C:\\jyap\\585\\Trace99\\Trace99.10KGet";
	//string trace_filename = "C:\\jyap\\585\\Trace1M\\1MObjs.Save";
	string trace_filename = cpt.filename;
	//string trace_filename = "C:\\jyap\\585\\TFn.txt";

	
	
	
	//TraceLoader trace( trace_filename, NumThreads, PrefetchSeconds, NumEltsPerThread, WorkerThreadTCTrace, ',' );
	TraceLoader trace( trace_filename, NumThreads, PrefetchSeconds, NumEltsPerThread, WorkerThreadMemCachedExperiment, ',' );
	
	g_traceLoader = &trace;



	// These functions must be called to allow the WorkerThread to see the loaded data objects
	vcmESptr = trace.getESptr();
	vcmPL = trace.getCriticalSection();
	vcm_DZ = trace.getDZ();

	for( int i = 0; i < NUMZONES; i++ )
	{
		vcm->Create( &vcm_DZ[i] );
	}

	//vcm_fout = new ofstream( "TC_trace_out.txt", ofstream::out );
	//vcm->SetFileStream( *vcm_fout );

	g_vcm_iteration = 0;
	for( uint i = 0; i < cpt.NumIterations; i++ )
	{
		cout << "***Running trace***\n";
		g_vcm_iteration++;
		ret = trace.run();
	}

	/*
	cout<< "***Inserting records from trace\n";
	g_vcm_traceStage = 0;
	ret = trace.run();
	//*/
	
	/*
	cout<< "***Retrieving inserted records\n";
	g_vcm_traceStage = 1;
	ret = trace.run();
	//*/

	/*
	cout<< "***Deleting inserted records\n";
	g_vcm_traceStage = 2;
	ret = trace.run();
	//*/

	//*
	cout<< "Writing contents of Q_DB to file.\n";

	ofstream fout;
	fout.open( "TC_QDB_contents.csv", ios::out );	
	vcm->DumpCache( fout );
	fout.close();
	//*/

	//vcm_fout->close();
	//delete vcm_fout;

	cout<< "-- TCache Test: End of trace tests --\n";
}

DWORD WINAPI WorkerThreadMemCachedExperiment( LPVOID lpParam ) 
{
	char* dataBuffer = new char[MAX_OBJECT_SIZE];

	double cost;
	double total_time_cacheMiss = 0.0;
	struct EachSecond *tmpPtr;
	struct CacheUtilization *utilPtr;
    int     ID = 0;
    int     count = 0;
	Vdt *FileVdt;
	Vdt vdtKey, vdtValue, vdtGetValue;
	char* cptr = dataBuffer;
	int iter_cnt = 1;
	string temp_str;

	bool displayLine = false;
	bool continueLoop = true;
	int num_inserted = 0;
	int num_retrieved = 0;
	int num_deleted = 0;
	int prev_lowest = 0;
	int ret = -1;
	int sz = 0;
	
	vdtValue.set_data( dataBuffer );
	vdtGetValue.set_data( dataBuffer );
	vdtGetValue.set_size( MAX_OBJECT_SIZE );

#ifdef memcacheTest
	memcached_return rc;
	memcached_st *memc;

	
	uint64_t num;
	size_t value_length = 0;
	uint32_t flags = 0;


	memc = memcached_create(NULL);
	rc = memcached_server_add(memc, SERVER_NAME, SERVER_PORT);
	
#endif

	char* value=NULL;
	//printf("server add: %s\n", memcached_strerror(memc, rc));

	//rc = memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);


	ID = *((int*)lpParam);
	
	if (ID >= MAXTHREADS)
	{
		printf("Error, thread ID %d may not exceed MAXTHREADS.  Increase this constant and re-run.\n",ID);
		return -1;
	}
	utilPtr = & g_traceLoader->Util[ID]; 

	HANDLE SyncEvent = OpenEvent ( EVENT_ALL_ACCESS , false, (LPCWSTR) "SyncEvent" );
	if (!SyncEvent) {
		printf ("WorkerThread %d:  Failed to establish the synchronization mechanism.\n", ID);
		return -1;
	}

	tmpPtr = vcmESptr;
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

				FileVdt = &vcm_DZ[ tmpPtr->zoneid[ID][i] ];

				if (VERBOSE) 
					printf("iter=%d, cnt=%d: ",iter_cnt,i);

				int curr_key = tmpPtr->key[ID][i];
								
				vdtKey.set_data( (char*)&tmpPtr->key[ID][i] );
				vdtKey.set_size( sizeof(int) );

				// Reset vdtGetValue to the buffer
				vdtGetValue.set_data( dataBuffer );
				vdtGetValue.set_size( MAX_OBJECT_SIZE );

				int temp_int, temp_int2;
				temp_int = tmpPtr->key[ID][i];
				temp_int2 = tmpPtr->size[ID][i];

				// Initialize insert values. During Get stage, these are used to verify correctness
				int stored_value = tmpPtr->key[ID][i] + tmpPtr->size[ID][i];
				//int stored_value = tmpPtr->size[ID][i];

				if( g_vcm_traceStage == 0 )
				{
					// Store key + size into the value buffer. This is used for
					//  error checking to see if the correct value was retrieved.
					char* cptr = dataBuffer;
					*(int*) cptr = stored_value;
					vdtValue.set_size( tmpPtr->size[ID][i] );
				}
					
#ifdef memcacheTest
				char key[20];
				sprintf_s(key,20,"%d",tmpPtr->key[ID][i]);
				size_t retSize=0;
#endif

				switch(tmpPtr->cmnd[ID][i])
				{
				case Get:
					utilPtr->TotalReq ++;
					utilPtr->TotalByte += tmpPtr->size[ID][i];

					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Get Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
					//tsm->Get( FileVdt, vdtKey, &vdtGetValue );
					//(*vcm_fout)<< "WorkerThread " << ID << ": Get Key " << tmpPtr->key[ID][i] << endl;

				
#ifdef memcacheTest
					retSize=0;
					value = memcached_get(memc, key, strlen(key),&retSize, &flags, &rc);
					if(strcmp(memcached_strerror(memc, rc),"SUCCESS")==0)
					{
						ret=0;
						vdtGetValue.set_size(retSize);
						vdtGetValue.set_data(value);
						//printf("Key %d Value = %d\n",tmpPtr->key[ID][i], *(int*)value);
					}
					else
						ret=-1;

					

#else
					ret = vcm_ptr->Get( FileVdt, vdtKey, &vdtGetValue );
#endif

					if( ret == 0 )
					{
						num_retrieved++;
						utilPtr->BytesFromMem += vdtGetValue.get_size();
						utilPtr->TotalReqFromMem ++;
						// Verify correctness of inserted values
						if(  vdtGetValue.get_size() != *(int*)vdtGetValue.get_data() )
							printf("**ERROR** Retrieved value \"%d\" was incorrect, expecting %d\n", *(int*)vdtGetValue.get_data(),vdtGetValue.get_size());
						//if( *(int*)vdtGetValue.get_data() != (1024 + curr_key) )
							//printf("**ERROR** Retrieved value for key \"%d\" was incorrect, expecting %d\n", *(int*)vdtGetValue.get_data(),(1024 + curr_key));

#ifdef memcacheTest
						if(value!=NULL)
							free(value);
						
#endif
						/*if( *(int*)vdtGetValue.get_data() != stored_value )
						{
							cout<< "Error. Retrieved object, " << tmpPtr->key[ID][i] 
								<< ", had the wrong value\n";
						}*/
					}
					else
					{
						//cout<< "Error. Key " << tmpPtr->key[ID][i] << " not found in the database\n";
						utilPtr->BytesInserted += tmpPtr->size[ID][i];
						//utilPtr->BytesInserted += 1024;

						*(int*)cptr = tmpPtr->size[ID][i];
						vdtValue.set_size( tmpPtr->size[ID][i] );

#ifdef memcacheTest

						rc = memcached_set(memc, key, strlen(key), vdtValue.get_data(), vdtValue.get_size(), 0, 0);
						if(strcmp(memcached_strerror(memc, rc),"SUCCESS")==0)
						{
							ret=0;
							//printf("set '%d' to '%d'\n", tmpPtr->key[ID][i], *(int*)vdtValue.get_data() );
						}
						else
							ret=-1;	
					
#else
						//*(int*)cptr = 1024 + curr_key;
						//vdtValue.set_size( 1024 );
						cost = (curr_key % 10) * 1000;   //cost could be zero

						// Keep track of the theoretical time spent retrieving object on cache miss
						total_time_cacheMiss += cost;

						ret = vcm_ptr->Insert( FileVdt, vdtKey, vdtValue, cost );
#endif

						if( ret == 0 )
						{
							num_inserted++;								
						}
						else
						{
							//cout<< "Error. Could not insert record\n";
							//continueLoop = false;
						}
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
					*(int*)cptr = tmpPtr->size[ID][i];
					vdtValue.set_size( tmpPtr->size[ID][i] );
					
#ifdef memcacheTest

						rc = memcached_set(memc, key, strlen(key), vdtValue.get_data(), vdtValue.get_size(), 0, 0);
						if(strcmp(memcached_strerror(memc, rc),"SUCCESS")==0)
						{
							ret=0;
							//printf("set Key '%d' Value '%d'\n", tmpPtr->key[ID][i], *(int*)vdtValue.get_data() );
						}
						else
							ret=-1;	
#else					
						ret = vcm_ptr->Insert( FileVdt, vdtKey, vdtValue );
#endif					
					if( ret == 0 )
					{
						num_inserted++;								
					}
					else
					{
						cout<< "Error. Could not insert record\n";
						//continueLoop = false;
					}

					break;
				case Delete:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Delete Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
					
					ret = vcm_ptr->Delete( FileVdt, vdtKey );
					if( ret == 0 )
					{
						num_deleted++;
					}
					else
					{
						cout<< "Error. Could not delete record\n";
						//continueLoop = false;
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
			//EnterCriticalSection(&PL);
			//Synchronize with the main thread that might be populating the Prefetch List
			//LeaveCriticalSection(&PL);

			EnterCriticalSection(vcmPL);
			//Synchronize with the main thread that might be populating the Prefetch List
			LeaveCriticalSection(vcmPL);
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

	//if( g_vcm_traceStage == 0 )
	cout<< "# inserted by Thread " << ID << ": " << num_inserted << endl;
	//else if( g_vcm_traceStage == 1 )
	cout<< "# retrieved by Thread " << ID << ": " << num_retrieved << endl;
	//else
	cout<< "# deleted by Thread " << ID << ": " << num_deleted << endl;
	cout<< "Thread " << ID << ": Time for retrieving objects on cache miss: " << total_time_cacheMiss << " time units.\n";
#ifdef memcacheTest
	memcached_stat_st *stats1;
	stats1= memcached_stat(memc,NULL,&rc);
	printf("Cache hit %d \n",stats1->get_hits);
	printf("%lld %d %lld\n",stats1->evictions,stats1->curr_items,stats1->bytes);
	free(stats1);
	//memcached_free(memc);
#endif
    return 0; 
}

DWORD WINAPI WorkerThreadTCTrace( LPVOID lpParam ) 
{
	char* dataBuffer = new char[MAX_OBJECT_SIZE];

	struct EachSecond *tmpPtr;
    int     ID = 0;
    int     count = 0;
	Vdt *FileVdt;
	Vdt vdtKey, vdtValue, vdtGetValue;
	char* cptr = dataBuffer;
	int iter_cnt = 1;
	string temp_str;

	bool displayLine = false;
	bool continueLoop = true;
	int num_inserted = 0;
	int num_retrieved = 0;
	int num_deleted = 0;
	int prev_lowest = 0;
	int ret = -1;
	
	vdtValue.set_data( dataBuffer );
	vdtGetValue.set_data( dataBuffer );
	vdtGetValue.set_size( MAX_OBJECT_SIZE );
	

    ID = *((int*)lpParam); 

	HANDLE SyncEvent = OpenEvent ( EVENT_ALL_ACCESS , false, (LPCWSTR) "SyncEvent" );
	if (!SyncEvent) {
		printf ("WorkerThread %d:  Failed to establish the synchronization mechanism.\n", ID);
		return -1;
	}

	tmpPtr = vcmESptr;
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

				FileVdt = &vcm_DZ[ tmpPtr->zoneid[ID][i] ];

				if (VERBOSE) 
					printf("iter=%d, cnt=%d: ",iter_cnt,i);

				int curr_key = tmpPtr->key[ID][i];
								
				vdtKey.set_data( (char*)&tmpPtr->key[ID][i] );
				vdtKey.set_size( sizeof(int) );

				// Reset vdtGetValue to the buffer
				vdtGetValue.set_data( dataBuffer );
				vdtGetValue.set_size( MAX_OBJECT_SIZE );

				int temp_int, temp_int2;
				temp_int = tmpPtr->key[ID][i];
				temp_int2 = tmpPtr->size[ID][i];

				
				// Initialize insert values. During Get stage, these are used to verify correctness
				//int stored_value = tmpPtr->key[ID][i] + tmpPtr->size[ID][i];
				int stored_value = tmpPtr->size[ID][i];

				if( g_vcm_traceStage == 0 )
				{
					// Store key + size into the value buffer. This is used for
					//  error checking to see if the correct value was retrieved.
					char* cptr = dataBuffer;
					*(int*) cptr = stored_value;
					vdtValue.set_size( tmpPtr->size[ID][i] );
				}


				switch(tmpPtr->cmnd[ID][i])
				{
				case Get:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Get Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
					//tsm->Get( FileVdt, vdtKey, &vdtGetValue );
					//(*vcm_fout)<< "WorkerThread " << ID << ": Get Key " << tmpPtr->key[ID][i] << endl;

					//*
					ret = vcm_ptr->Get( FileVdt, vdtKey, &vdtGetValue );
					if( ret == 0 )
					{
						num_retrieved++;

						/*
						if( num_retrieved % 100000 == 0 )
						{
							std::string fname = "QDistribution";
							fname += TCUtility::intToString( num_retrieved );
							fname += "-It";
							fname += TCUtility::intToString( g_vcm_iteration );
							fname += ".csv";

							std::ofstream fout;
							fout.open( fname.c_str(), std::ofstream::out );
							vcm_ptr->DumpCache( fout );
							fout.close();
						}
						//*/


						// Verify correctness of inserted values
						if( *(int*)vdtGetValue.get_data() != stored_value )
						{
							cout<< "Error. Retrieved object, " << tmpPtr->key[ID][i] 
								<< ", had the wrong value\n";
						}
					}
					else
					{
						//cout<< "Error. Key " << tmpPtr->key[ID][i] << " not found in the database\n";

					}
					//*/

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
					//*(int*)cptr = tmpPtr->size[ID][i];
					//vdtValue.set_size( tmpPtr->size[ID][i] );
						
					//*
					ret = vcm_ptr->Insert( FileVdt, vdtKey, vdtValue );
					if( ret == 0 )
					{
						num_inserted++;	

						
					}
					else
					{
						cout<< "Error. Could not insert record\n";
						//continueLoop = false;
					}
					//*/

					break;
				case Delete:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Delete Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);

					//*
					ret = vcm_ptr->Delete( FileVdt, vdtKey );
					if( ret == 0 )
					{
						num_deleted++;
					}
					else
					{
						//cout<< "Error. Could not delete record\n";
						//continueLoop = false;
					}
					//*/

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

			EnterCriticalSection(vcmPL);
			//Synchronize with the main thread that might be populating the Prefetch List
			LeaveCriticalSection(vcmPL);
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

	//if( g_vcm_traceStage == 0 )
	cout<< "# inserted by Thread " << ID << ": " << num_inserted << endl;
	//else if( g_vcm_traceStage == 1 )
	cout<< "# retrieved by Thread " << ID << ": " << num_retrieved << endl;
	//else
	cout<< "# deleted by Thread " << ID << ": " << num_deleted << endl;

    
    return 0; 
}





