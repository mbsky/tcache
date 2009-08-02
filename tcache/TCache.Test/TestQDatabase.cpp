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
#include "QDatabase.h"
#include "ConfigurationLoader.h"
#include "TraceLoader.h"
#include "TCLock.h"

#include <iostream>
using namespace std;

DbEnv* memoryEnv;

QDatabase* qdb_ptr;
ContextManager* cm_ptr;
TCLock	lock_qdb(1000, 4);

// Global variables used by worker thread
LARGE_INTEGER g_timestamp;
int g_numPartitions;
int g_traceStage;
char g_headerFlag;

EachSecond* qdbESptr;
CRITICAL_SECTION* qdbPL;
Vdt* qdb_DZ;

void setupEnv( const ConfigParametersV& cp );
void testQDBsimpleTests( DbEnv* memoryDbEnv, const ConfigParametersV& cp, ContextManager& cm );
void testQDBloopTests( DbEnv* memoryDbEnv, const ConfigParametersV& cp, ContextManager& cm );
void testQDBtraceTests( DbEnv* memoryDbEnv, const ConfigParametersV& cp, ContextManager& cm );
DWORD WINAPI WorkerThreadQDBTrace( LPVOID lpParam );


int testQDatabaseMain()
{
	ConfigParametersV cp;
	ConfigurationLoader config;
	config.loadVConfigFile( "V_test_q.xml", cp );

	ContextManager cm(1,1,4);
	setupEnv( cp );

	// Test the basic functions
	//testQDBsimpleTests( memoryEnv, cp, cm );

	// Test with a loop
	//testQDBloopTests( memoryEnv, cp, cm );

	// Test with multiple threads and trace file
	testQDBtraceTests( memoryEnv, cp, cm );

	//cm.printStats();	

	return -1;
}

void setupEnv( const ConfigParametersV& cp )
{
	u_int32_t open_flags = 0;
	int ret;

	memoryEnv = new DbEnv( 0 );

	open_flags =
		DB_CREATE     |  /* Create the environment if it does not exist */
		DB_INIT_LOCK  |  /* Initialize the locking subsystem */
		DB_INIT_LOG   |  /* Initialize the logging subsystem */
		DB_INIT_MPOOL |  /* Initialize the memory pool (in-memory cache) */
		DB_INIT_TXN   |
		DB_THREAD	  |
		DB_PRIVATE;      /* Region files are not backed by the filesystem. 
						  * Instead, they are backed by heap memory.  */


	/* Specify in-memory logging */
	ret = memoryEnv->log_set_config( DB_LOG_IN_MEMORY, 1 );
	if (ret != 0) {
		fprintf(stderr, "Error setting log subsystem to in-memory: %s\n",
			db_strerror(ret));
	}

	ret = memoryEnv->set_lk_detect( DB_LOCK_MINWRITE );
	if (ret != 0) {
		fprintf(stderr, "Error setting deadlock flag in-memory: %s\n",
			db_strerror(ret));
	}

	/* 
	 * Specify the size of the in-memory log buffer. 
	 */
	ret = memoryEnv->set_lg_bsize( TC_LOG_BUFFER_SIZE );
	if (ret != 0) {
		fprintf(stderr, "Error increasing the log buffer size: %s\n",
			db_strerror(ret));
	}

	ret = memoryEnv->set_lk_max_objects( cp.MemBDB.MaxLockObjects );
	ret = ret + memoryEnv->set_lk_max_locks( cp.MemBDB.MaxLocks );
	ret = ret + memoryEnv->set_lk_max_lockers( cp.MemBDB.MaxLockers );
	if (ret != 0) {
		fprintf(stderr, "Error increasing the max number of locks and lock objects: %s\n",
			db_strerror(ret));
	}

	/* 
	 * Specify the size of the in-memory cache. 
	 */
	ret = memoryEnv->set_cachesize(  
		cp.MemBDB.CacheSize.GigaBytes, 
		cp.MemBDB.CacheSize.Bytes, 
		cp.MemBDB.CacheSize.NumberCaches );
	if (ret != 0) {
		fprintf(stderr, "Error increasing the cache size: %s\n",
			db_strerror(ret));
	}

	/* 
	 * Now actually open the environment. Notice that the environment home
	 * directory is NULL. This is required for an in-memory only
	 * application. 
	 */
	ret = memoryEnv->open( NULL, open_flags, 0 );
	if (ret != 0) {
		fprintf(stderr, "Error opening environment: %s\n",
			db_strerror(ret));
	}
}

void testQDBsimpleTests( DbEnv* memoryDbEnv, const ConfigParametersV& cp, ContextManager& cm )
{
	QDatabase q_db;
	q_db.Initialize( memoryDbEnv, cp );

	ContextStructure* c_struct, *c_struct2;

	cout<< "-- QDatabase Test: Running simple tests --\n";
	
	cm.GetFreeContext( c_struct );
	cm.GetFreeContext( c_struct2 );

	QDBKey qKey( c_struct );
	QDBValue qValue( c_struct ), qRetrieveVal( c_struct2 );

	string strDzName = "testQDz", strKey = "testQKey";
	Vdt dzName = TCUtility::convertToVdt( strDzName );
	Vdt key = TCUtility::convertToVdt( strKey );

	void* metadata = NULL;
	int metadata_int = 3974;
	LARGE_INTEGER timestamp;
	QueryPerformanceCounter( &timestamp );

	qKey.setValues( 'D', 0.25, dzName, key );

	if( qValue.checkMetadataSize( sizeof(metadata_int) ) == 0 )
	{
		metadata = qValue.getMetadataPtr();
		*(int*)metadata = metadata_int;
		qValue.setValues( 12347, timestamp, sizeof(metadata_int) );
	}
	else
	{
		cout<< "Error. Buffer too small for metadata\n";
		return;
	}

	// Insert a key and check that the inserted values are correct
	if( q_db.Insert( qKey, qValue, 0 ) != 0 )
		cout<< "Error. Could not insert\n";

	if( q_db.Get( qKey, qRetrieveVal, 0 ) != 0 )
		cout<< "Error. Could not get\n";

	if( qValue.getRecordSize() != qRetrieveVal.getRecordSize() )
	{
		cout<< "Error. Size does not match\n";
	}

	if( qValue.getUpdateTimestamp().QuadPart != qRetrieveVal.getUpdateTimestamp().QuadPart )
	{
		cout<< "Error. Timestamp does not match\n";
	}

	int temp = *(int*)qValue.getMetadataPtr();
	int temp2 = *(int*)qRetrieveVal.getMetadataPtr();

	if( memcmp( qValue.getMetadataPtr(), qRetrieveVal.getMetadataPtr(), sizeof(metadata) ) != 0 )
	{
		cout<< "Error. Metadata does not match\n";
	}

	// Insert a new key and get the lowest dirty record (previous key)
	strKey = "testQ2Key";
	key = TCUtility::convertToVdt( strKey );
	qKey.setValues( 'D', 0.5, dzName, key );

	if( qRetrieveVal.checkMetadataSize( sizeof(metadata_int) ) == 0 )
	{
		metadata = qRetrieveVal.getMetadataPtr();
		*(int*)metadata = metadata_int;
		qRetrieveVal.setValues( 7123, timestamp, sizeof(metadata_int) );
	}
	else
	{
		cout<< "Error. Buffer too small for metadata\n";
		return;
	}
	
	if( q_db.Insert( qKey, qRetrieveVal, 0 ) != 0 )
		cout<< "Error. Could not insert\n";

	if( q_db.GetLowestDirtyRecord( qKey, qRetrieveVal, 0 ) != 0 )
		cout<< "Error. Could not get lowest dirty record\n";

	// Check that we got the lower record
	Vdt key2 = qKey.getKey();
	strKey = "";
	strKey.insert( 0, key2.get_data(), key2.get_size() );
	if( strKey.compare( "testQKey" ) != 0 )
		cout<< "Error. Retrieved the wrong key\n";

	// Verify the values in the record
	if( qValue.getRecordSize() != qRetrieveVal.getRecordSize() )
	{
		cout<< "Error. Size does not match\n";
	}

	if( qValue.getUpdateTimestamp().QuadPart != qRetrieveVal.getUpdateTimestamp().QuadPart )
	{
		cout<< "Error. Timestamp does not match\n";
	}

	if( memcmp( qValue.getMetadataPtr(), qRetrieveVal.getMetadataPtr(), sizeof(metadata) ) != 0 )
	{
		cout<< "Error. Metadata does not match\n";
	}




	// Delete a key and check that its gone
	if( q_db.Delete( qKey, 0 ) != 0 )
		cout<< "Error. Could not delete\n";

	if( q_db.Get( qKey, qRetrieveVal, 0 ) == 0 )
		cout<< "Error. Deleted record was found\n";

	q_db.Shutdown();

	cout<< "-- QDatabase Test: End of simple tests --\n";

	cm.ReleaseContext( c_struct );
	cm.ReleaseContext( c_struct2 );
}


void testQDBloopTests( DbEnv* memoryDbEnv, const ConfigParametersV& cp, ContextManager& cm )
{
	cout<< "-- QDatabase Test: Running loop tests --\n";

	int max_num_iterations = 1000000;

	ContextStructure* c_struct;
	bool continueLoop = true;
	int ret = -1;

	int num_inserted = 0;
	int num_found = 0;
	int num_deleted = 0;

	int num_partitions = cp.MemBDB.DatabaseConfigs.Partitions;

	// Initiazlie Q database
	QDatabase q_db;
	q_db.Initialize( memoryDbEnv, cp );

	cm.GetFreeContext( c_struct );
	QDBKey qKey( c_struct );
	QDBValue qValue( c_struct );

	// Key parameters
	char headerFlag = 'C';
	double q_value = 1.0;
	string sDzNameBase = "testQLoop", sKeyBase = "testQLoopKey";
	string sDzName, sKey;
	Vdt dzName, key;


	// Value parameters
	int record_size_in, record_size_out;

	LARGE_INTEGER timestamp_base, timestamp_in, timestamp_out;
	QueryPerformanceCounter( &timestamp_base );

	void* metadata_in, *metadata_out;
	int metadata_size_in;


	// Insert until full
	for( int i = 0; i < max_num_iterations && continueLoop; i++ )
	{
		try
		{
			sDzName = sDzNameBase + TCUtility::intToString(i);
			dzName = TCUtility::convertToVdt( sDzName );
			sKey = sKeyBase + TCUtility::intToString(i);
			key = TCUtility::convertToVdt( sKey );

			qKey.setValues( headerFlag, q_value*i, dzName, key );


			record_size_in = i;
			timestamp_in.QuadPart = timestamp_base.QuadPart + i;			
			if( qValue.checkMetadataSize( sizeof(i) ) == 0 )
			{
				metadata_in = qValue.getMetadataPtr();
				*(int*)metadata_in = i;
				metadata_size_in = sizeof(i);
				qValue.setValues( record_size_in, timestamp_in, metadata_size_in );
			}
			else
			{
				cout<< "Error. Buffer is too small for metadata\n";
				return;
			}
				

			if( q_db.Insert( qKey, qValue, i % num_partitions ) == 0 )
			{
				num_inserted++;
			}
			else
			{
				cout<< "- Q DB is full(failed to insert more) -\n";
				continueLoop = false;
			}
		}
		catch( exception& e )
		{
			cout<< e.what();

			continueLoop = false;
		}
	}

	cout<< "# of records inserted: " << num_inserted << endl;

	// Try and retrieve inserted records
	continueLoop = true;
	for( int i = 0; i < num_inserted && continueLoop; i++ )
	{
		try
		{
			sDzName = sDzNameBase + TCUtility::intToString(i);
			dzName = TCUtility::convertToVdt( sDzName );
			sKey = sKeyBase + TCUtility::intToString(i);
			key = TCUtility::convertToVdt( sKey );

			record_size_in = i;
			timestamp_in.QuadPart = timestamp_base.QuadPart + i;
			metadata_in = &i;
			metadata_size_in = sizeof(i);

			qKey.setValues( headerFlag, q_value*i, dzName, key );

			if( q_db.Get( qKey, qValue, i % num_partitions ) == 0 )
			{
				num_found++;

				// Check if the retrieved value was correct
				record_size_out = qValue.getRecordSize();
				timestamp_out = qValue.getUpdateTimestamp();
				metadata_out = qValue.getMetadataPtr();

				if( record_size_out != record_size_in )
					cout<< "Error. Retrieved record had the wrong value for Record Size\n";

				if( timestamp_out.QuadPart != timestamp_in.QuadPart )
					cout<< "Error. Retrieved record had the wrong value for Timestamp\n";

				if( *(int*)metadata_out != *(int*)metadata_in )
					cout<< "Error. Retrieved record had the wrong value for Metadata\n";
			}
			else
			{
				cout<< "Error. Could not retrieve inserted record (" << i << ")\n";
			}
		}
		catch( exception& e )
		{
			cout<< e.what();

			continueLoop = false;
		}
	}

	cout<< "# of records found: " << num_found << endl;

	// Try and delete inserted records
	continueLoop = true;
	for( int i = 0; i < num_inserted && continueLoop; i++ )
	{
		try
		{
			sDzName = sDzNameBase + TCUtility::intToString(i);
			dzName = TCUtility::convertToVdt( sDzName );
			sKey = sKeyBase + TCUtility::intToString(i);
			key = TCUtility::convertToVdt( sKey );

			qKey.setValues( headerFlag, q_value*i, dzName, key );

			if( q_db.Delete( qKey, i % num_partitions ) == 0 )
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

	cout<< "# of records deleted: " << num_deleted << endl;


	q_db.Shutdown();

	cm.ReleaseContext( c_struct );

	cout<< "-- QDatabase Test: End of loop tests --\n";
}


void testQDBtraceTests( DbEnv* memoryDbEnv, const ConfigParametersV& cp, ContextManager& cm )
{
	cout<< "-- QDatabase Test: Running trace tests --\n";

	// Set up Q Database
	QDatabase q_db;
	q_db.Initialize( memoryDbEnv, cp );

	qdb_ptr = &q_db;
	cm_ptr = &cm;
	QueryPerformanceCounter( &g_timestamp );
	g_headerFlag = 'C';
	g_numPartitions = cp.MemBDB.DatabaseConfigs.Partitions;

	// Set up Trace Loader
	int NumThreads = 2; 
	int PrefetchSeconds = 10;
	int NumEltsPerThread = 50000;
	//string trace_filename = "C:\\jyap\\585\\MySpace\\trace_original.txt";
	//string trace_filename = "C:\\jyap\\585\\Trace1M\\Trace1M.1MGet";
	//string trace_filename = "C:\\jyap\\585\\Trace1M\\Trace1M.100KGet";
	string trace_filename = "C:\\jyap\\585\\Trace1M\\1MObjs.Save";
	
	
	cout<< "***Inserting records from trace\n";
	TraceLoader trace( trace_filename, NumThreads, PrefetchSeconds, NumEltsPerThread, WorkerThreadQDBTrace, ',' );

	// These functions must be called to allow the WorkerThread to see the loaded data objects
	qdbESptr = trace.getESptr();
	qdbPL = trace.getCriticalSection();
	qdb_DZ = trace.getDZ();

	g_traceStage = 0;
	int ret = trace.run();
	
	cout<< "***Retrieving inserted records\n";
	g_traceStage = 1;
	ret = trace.run();

	cout<< "***Deleting inserted records\n";
	g_traceStage = 2;
	ret = trace.run();

	lock_qdb.printStats();

	/*
	cout<< "Writing contents of Q_DB to file.\n";
	CachingAlgorithm* ca = new GreedyDualSizeAlgorithm();
	ContextStructure* c_struct;
	cm.GetFreeContext( c_struct );	

	ofstream fout;
	fout.open( "QDB_contents.txt", ios::out );	
	q_db.printContents( ca, 0, c_struct, fout );
	fout.close();
	//*/

	q_db.Shutdown();

	cout<< "-- QDatabase Test: End of trace tests --\n";
}

DWORD WINAPI WorkerThreadQDBTrace( LPVOID lpParam ) 
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

	ContextStructure* c_struct = NULL;
	cm_ptr->GetFreeContext( c_struct );

	QDBKey qKey( c_struct );
	QDBValue qValue( c_struct );

	tmpPtr = qdbESptr;
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

				FileVdt = &qdb_DZ[ tmpPtr->zoneid[ID][i] ];

				if (VERBOSE) 
					printf("iter=%d, cnt=%d: ",iter_cnt,i);

				int curr_key = tmpPtr->key[ID][i];
								
				vdtKey.set_data( (char*)&tmpPtr->key[ID][i] );
				vdtKey.set_size( sizeof(int) );

				// Reset vdtGetValue to the buffer
				vdtGetValue.set_data( dataBuffer );
				vdtGetValue.set_size( MAX_OBJECT_SIZE );

				int temp_int, temp_int2;

				char headerFlag = g_headerFlag;
				if( curr_key % 2 == 0 )
					headerFlag++;

				// Initialize the key to be inserted into the Q database
				qKey.setValues( headerFlag, (double)curr_key, *FileVdt, vdtKey );

				// Initialize insert values. During Get stage, these are used to verify correctness
				int record_size = tmpPtr->size[ID][i];

				LARGE_INTEGER timestamp;
				timestamp.QuadPart = g_timestamp.QuadPart + curr_key;

				//void* metadata = &curr_key;
				//int metadata_size = sizeof(curr_key);
				void* metadata;
				int metadata_size = 16;	// Test with 2KB records

				if( qValue.checkMetadataSize( metadata_size ) == 0 )
				{
					metadata = qValue.getMetadataPtr();
					*(int*)metadata = curr_key;
				}
				else
				{
					cout<< "Error. Buffer is too small for metadata\n";
					return -1;
				}

				if( g_traceStage == 0 )
				{
					qValue.setValues( record_size, timestamp, metadata_size );
				}


				switch(tmpPtr->cmnd[ID][i])
				{
				case Get:
					if (VERBOSE)
						printf("WorkerThread %d:  Invoke V's Get Method with Datazone element %d (file %c), Key %d, size %d.\n",ID, tmpPtr->zoneid[ID][i], FileVdt->get_data()[0], tmpPtr->key[ID][i], tmpPtr->size[ID][i]);
					//tsm->Get( FileVdt, vdtKey, &vdtGetValue );
					if( lock_qdb.acquireWait( *FileVdt, vdtKey ) == 0 )
					{
						
						if( g_traceStage == 0 )
						{
							ret = qdb_ptr->Insert( qKey, qValue, curr_key % g_numPartitions );
							if( ret == 0 )
							{
								num_inserted++;								
							}
							else
							{
								continueLoop = false;
							}
						}
						else if( g_traceStage == 1 )
						{
							ret = qdb_ptr->Get( qKey, qValue, curr_key % g_numPartitions );
							if( ret == 0 )
							{
								num_retrieved++;

								// Verify correctness of inserted values
								if( qValue.getRecordSize() != record_size )
									cout<< "Error. Retrieved record had the wrong value for Record Size\n";

								if( qValue.getUpdateTimestamp().QuadPart != timestamp.QuadPart )
									cout<< "Error. Retrieved record had the wrong value for Timestamp\n";

								if( *(int*)qValue.getMetadataPtr() != *(int*)metadata )
									cout<< "Error. Retrieved record had the wrong value for Metadata\n";
							}
							else
							{
								continueLoop = false;
							}
						}
						else if( g_traceStage == 2 )
						{
							ret = qdb_ptr->Delete( qKey, curr_key % g_numPartitions );
							if( ret == 0 )
							{
								num_deleted++;
							}
							else
							{
								continueLoop = false;
							}
						}

						lock_qdb.release( *FileVdt, vdtKey );

						/* This test is specific for the Trace1M\1MObjs.Save trace
						// Check that GetLowestDirtyRecord is thread safe
						if( qdb_ptr->GetLowestDirtyRecord( qKey, qValue, curr_key % g_numPartitions ) == 0 )
						{
							if( qKey.getHeaderFlag() != 'D' )
								cout<< "Error. Retrieved lowest-Dirty-Record was not dirty\n";

							// Check that the key ( record ID ) is the expected value
							temp_int = *(int*)qKey.getKey().get_data();

							if( temp_int != 0 )
								cout<< "Error. Did not retrieve the Lowest dirty record\n";

							temp_int = *(char*)qKey.getDzName().get_data();
							if( temp_int != 'B' )
								cout<< "Error. Did not retrieve the correct datazone for Lowest dirty record\n";

							if( qValue.getRecordSize() != 318 )
								cout<< "Error. Did not retrieve the correct record size for LowestDirtyRecord\n";
						}
						else
						{
							cout<< "Error. Could not retrieve lowest dirty record\n";
						}
						//*/
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
					//*(int*)cptr = tmpPtr->size[ID][i];
					//vdtValue.set_size( tmpPtr->size[ID][i] );

					//*
					if( lock_qdb.acquireWait( *FileVdt, vdtKey ) == 0 )
					{
						
						if( g_traceStage == 0 )
						{
							ret = qdb_ptr->Insert( qKey, qValue, curr_key % g_numPartitions );
							if( ret == 0 )
							{
								num_inserted++;								
							}
							else
							{
								continueLoop = false;
							}
						}
						else if( g_traceStage == 1 )
						{
							ret = qdb_ptr->Get( qKey, qValue, curr_key % g_numPartitions );
							if( ret == 0 )
							{
								num_retrieved++;

								// Verify correctness of inserted values
								if( qValue.getRecordSize() != record_size )
									cout<< "Error. Retrieved record had the wrong value for Record Size\n";

								if( qValue.getUpdateTimestamp().QuadPart != timestamp.QuadPart )
									cout<< "Error. Retrieved record had the wrong value for Timestamp\n";

								if( *(int*)qValue.getMetadataPtr() != *(int*)metadata )
									cout<< "Error. Retrieved record had the wrong value for Metadata\n";
							}
							else
							{
								continueLoop = false;
							}
						}
						else if( g_traceStage == 2 )
						{
							ret = qdb_ptr->Delete( qKey, curr_key % g_numPartitions );
							if( ret == 0 )
							{
								num_deleted++;
							}
							else
							{
								continueLoop = false;
							}							
						}

						lock_qdb.release( *FileVdt, vdtKey );

						/* This test is specific for the Trace1M\1MObjs.Save trace for the delete phase
						//  *NOTE: be sure to turn off the other tests (simple and loop) or they might interfere
						if( g_traceStage == 2 )
						{
							// Check that GetLowestDirtyRecord is thread safe
							if( qdb_ptr->GetLowestDirtyRecord( qKey, qValue, curr_key % g_numPartitions ) == 0 )
							{
								if( qKey.getHeaderFlag() != 'D' )
									cout<< "Error. Retrieved lowest-Dirty-Record was not dirty\n";

								// Check that the key ( record ID ) is the expected value
								temp_int = *(int*)qKey.getKey().get_data();

								if( displayLine )
								{
									cout<< "Previous Lowest: " << prev_lowest << " ; Current Lowest: " << temp_int << "\n";
									displayLine = false;
								}

								if( temp_int < prev_lowest )
									cout<< "Error. Lowest dirty record is inconsistent. Previous Lowest: " << prev_lowest 
										<< " ; Current Lowest: " << temp_int << "\n";
								else if( temp_int > prev_lowest )
								{	
									//cout<< "Thread" << ID << " Previous Lowest: " << prev_lowest << " ; Current Lowest: " << temp_int << "\n";
									prev_lowest = temp_int;
									//displayLine = true;
								}

								temp_int = *(char*)qKey.getDzName().get_data();
								if( temp_int != 'B' )
									cout<< "Error. Did not retrieve the correct datazone for Lowest dirty record\n";
							}
							else
							{
								cout<< "Error. Thread " << ID << " could not retrieve lowest dirty record\n";
							}
						}
						//*/

						

						/* This test is specific for the Trace1M\1MObjs.Save trace
						// Check that GetLowestDirtyRecord is thread safe
						if( qdb_ptr->GetLowestDirtyRecord( qKey, qValue, curr_key % g_numPartitions ) == 0 )
						{
							if( qKey.getHeaderFlag() != 'D' )
								cout<< "Error. Retrieved lowest-Dirty-Record was not dirty\n";

							// Check that the key ( record ID ) is the expected value
							temp_int = *(int*)qKey.getKey().get_data();

							if( temp_int != 0 )
								cout<< "Error. Did not retrieve the Lowest dirty record\n";

							temp_int = *(char*)qKey.getDzName().get_data();
							if( temp_int != 'B' )
								cout<< "Error. Did not retrieve the correct datazone for Lowest dirty record\n";

							if( qValue.getRecordSize() != 318 )
								cout<< "Error. Did not retrieve the correct record size for LowestDirtyRecord\n";
						}
						else
						{
							cout<< "Error. Could not retrieve lowest dirty record\n";
						}
						//*/
					}
					//*/

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

			EnterCriticalSection(qdbPL);
			//Synchronize with the main thread that might be populating the Prefetch List
			LeaveCriticalSection(qdbPL);
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

	if( g_traceStage == 0 )
		cout<< "# inserted by Thread " << ID << ": " << num_inserted << endl;
	else if( g_traceStage == 1 )
		cout<< "# retrieved by Thread " << ID << ": " << num_retrieved << endl;
	else
		cout<< "# deleted by Thread " << ID << ": " << num_deleted << endl;

	cm_ptr->ReleaseContext( c_struct );
    
    return 0; 
}

