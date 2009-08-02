#include "StdAfx.h"
#include "CacheDatabase.h"
#include "ConfigurationLoader.h"

#include <iostream>
using namespace std;

int sizeof_rec_in1 = 1*1024*1024, sizeof_rec_out1 = 1*1024*1024;

DbEnv* memEnv;

void setupMemEnv1( const ConfigParametersV& cp );
void simpleTestCacheDatabase1(CacheDatabase& cache_db, ContextManager cm);
void testCacheDbLoop1(CacheDatabase& cache_db, ContextManager cm, ConfigParametersV cp );
//void testCacheDatabaseMain();

void testCacheDatabaseMain2()
{
	ConfigParametersV cp;
	ConfigurationLoader config;
	config.loadVConfigFile( "V_test_cache.xml", cp );

	ContextManager cm(1,1,4);

	CacheDatabase cache_db;

	setupMemEnv1( cp );

	cache_db.Initialize( memEnv, cp );

	// Test the basic functions
	//simpleTestCacheDatabase1( cache_db, cm );

	//Test Filling single loop
	testCacheDbLoop1(cache_db, cm, cp);

	// Test with a trace file


	// Test with multiple threads	

	
	cache_db.Shutdown();

	//return -1;
};


void simpleTestCacheDatabase1( CacheDatabase& cache_db, ContextManager cm )
{
	/*ConfigParametersV cp;
	ConfigurationLoader config;
	config.loadVConfigFile( "V_test_cache.xml", cp );

	CacheDatabase cache_db;

	setupMemEnv( cp );

	cache_db.Initialize( memEnv, cp );*/

	ContextStructure* c_struct, *c_struct2;
	//ContextManager cm(1,1,4);
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

	//return -1;
}

void testCacheDbLoop1(CacheDatabase& cache_db, ContextManager cm, ConfigParametersV cp )
{
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
	TCQValue qval(0,0.0);
	char dirtybit = '1';
	char cleanbit = '0';
	bool quitloop = false;
	char* dataBuf = new char[sizeof_rec_in1];
	
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
		Value.set_size(sizeof_rec_in1);
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
	dataBuf = new char[sizeof_rec_out1];
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
		Value.set_size(sizeof_rec_out1);
		
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
	
}


void setupMemEnv1( const ConfigParametersV& cp )
{
	u_int32_t open_flags = 0;
	int ret;

	memEnv = new DbEnv( 0 );

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
	ret = memEnv->log_set_config( DB_LOG_IN_MEMORY, 1 );
	if (ret != 0) {
		fprintf(stderr, "Error setting log subsystem to in-memory: %s\n",
			db_strerror(ret));
	}
	/* 
	 * Specify the size of the in-memory log buffer. 
	 */
	ret = memEnv->set_lg_bsize( TC_LOG_BUFFER_SIZE );
	if (ret != 0) {
		fprintf(stderr, "Error increasing the log buffer size: %s\n",
			db_strerror(ret));
	}

	ret = memEnv->set_lk_max_objects( cp.MemBDB.MaxLockObjects );
	ret = ret + memEnv->set_lk_max_locks( cp.MemBDB.MaxLocks );
	ret = ret + memEnv->set_lk_max_lockers( cp.MemBDB.MaxLockers );
	if (ret != 0) {
		fprintf(stderr, "Error increasing the max number of locks and lock objects: %s\n",
			db_strerror(ret));
	}

	/* 
	 * Specify the size of the in-memory cache. 
	 */
	ret = memEnv->set_cachesize(  
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
	ret = memEnv->open( NULL, open_flags, 0 );
	if (ret != 0) {
		fprintf(stderr, "Error opening environment: %s\n",
			db_strerror(ret));
	}
}