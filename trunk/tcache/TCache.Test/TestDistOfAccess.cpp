#include "stdafx.h"
#include <stdlib.h>
#include "TCache.h"
#include "Vdt.h"
#include "DistOfAccess.h"
#include "ConfigParameters.h"
#include "ConfigurationLoader.h"

void testDistOfAccess();
void GenerateDatabase( TCache* tcm, const int& num_keys );
void PerformGetRequests( TCache *tcm, DistOfAccess *dist, const int& num_requests );
DWORD WINAPI	WorkerThreadDistOfAccess( LPVOID lpParam );

// Globals
Vdt	defaultDatazone;



struct ThreadParameters
{
	int ThreadID;
	int	NumElementsToProcess;
};



void testDistOfAccessMain( int argc, _TCHAR * argv[] )
{
	// Test the Zipf Distribution Generator
	//testDistOfAccess();

	// Defaults
	// =================
	// The ZipfianDistribution generator(DistOfAccess class) can only 
	//  handle up to around 5,000,000 keys due to an internal limitation from using
	//  signed 32bit integers.
	int NumKeysInDB = 100000;	

	// Any number of requests up to the max signed 32bit integer value can be specified
	int NumReqs = 1000000;

	// Threads are not currently being used
	int NumThreads = 1;

	// These values govern the variability in the distribution of keys generated
	int ZipfianSeed = 3;
	double ZipfianMean = 0.27;
	std::string Distribution = "Zipfian"; // Zipfian or Uniform
	bool generateDB = false;
	// =================

	std::string datazoneName = "TrojanDZ";
	defaultDatazone.set_data( (char*)datazoneName.c_str() );
	defaultDatazone.set_size( (int)datazoneName.length() );

	if( argc == 4 )
	{
		NumKeysInDB = _ttoi( argv[1] );
		NumReqs = _ttoi( argv[2] );
	}

	DistOfAccess	dist( NumKeysInDB, Distribution, ZipfianMean, true, false );
	
	// Load the cache manager configurations from the xml file
	ConfigParametersV	cp;
	ConfigurationLoader config;
	config.loadVConfigFile( "V.xml", cp );

	std::cout<< "Initializing Trojan Cache Manager\n";
	// Initialize the Trojan Cache Manager
	TCache	tcm;
	tcm.Initialize( cp );

	// Create the datazone in case it hasn't been created yet
	tcm.Create( &defaultDatazone );

	// If specified, generate the keys and their corresponding values and store
	//  them in the database
	if( generateDB )
		GenerateDatabase( &tcm, NumKeysInDB );


	PerformGetRequests( &tcm, &dist, NumReqs );


	std::cout<< "Test Completed. Shutting down database...\n";

	tcm.Shutdown();
	std::cout<< "Trojan Cache Manager shutdown completed\n";
}


void GenerateDatabase( TCache *tcm, const int& num_keys )
{
	Vdt vdtKey, vdtValue;	
	int object_size;
	const int MAX_SIZE = 2048;	
	char dataBuffer[MAX_SIZE];
	char* cptr;

	std::cout<< "Generating database...\n";

	for( int i = 0; i < num_keys; i++ )
	{
		// Store the index as the key
		vdtKey.set_data( (char*) &i );
		vdtKey.set_size( sizeof(i) );

		// Store some value of random size(min size of 4) for error checking.
		// It is set to contain some data based on the key
		object_size = (rand() % (MAX_SIZE - 4)) + 4;
		cptr = dataBuffer;
		*(int*)cptr = i;
		vdtValue.set_data( dataBuffer );
		vdtValue.set_size( object_size );

		// Perform the actual insert into the Trojan Cache Manager
		tcm->Insert( &defaultDatazone, vdtKey, vdtValue );	
	}

	std::cout<< "Database generation completed\n";
}

void PerformGetRequests( TCache *tcm, DistOfAccess *dist, const int& num_requests )
{
	Vdt vdtKey, vdtValue;
	double total_time_cacheMiss = 0.0;
	double runtime = 0.0;
	double cost;
	const int MAX_SIZE = 2048;	
	char dataBuffer[MAX_SIZE];
	char* cptr;
	int	key;
	int object_size;
	int cold_start_multiple = 2;
	bool doingColdStart = true;
	srand( 5 );

	std::cout<< "Retrieving objects from the Trojan Cache Manager...\n";

	LARGE_INTEGER time_begin, time_end, tick_per_second;
	
	for( int i = 0; doingColdStart || i < num_requests; i++ )
	{
		// Generate a key based on a Zipfian or Uniform distribution
		key = dist->GenerateOneItem();

		// Store the index as the key
		vdtKey.set_data( (char*) &key );
		vdtKey.set_size( sizeof(key) );

		// Retrieve the key from the database
		vdtValue.set_data( dataBuffer );
		vdtValue.set_size( MAX_SIZE );

		// Perform the actual insert into the Trojan Cache Manager
		if( tcm->Get( &defaultDatazone, vdtKey, &vdtValue ) == 0 )
		{			
			//std::cout<< "Size of key " << key << " is " << vdtValue.get_size() << " bytes.\n";
			int temp_int = *(int*)vdtValue.get_data();
			if( *(int*)vdtValue.get_data() != key )
				std::cout<< "Error. Retrieved key's value did not match the inserted data for key " << key << "\n";
		}
		else
		{
			// Could not find key in the cache. Generate a value and insert

			// Store the index as the key
			vdtKey.set_data( (char*) &key );
			vdtKey.set_size( sizeof(key) );

			// Store some value of random size(min size of 4) for error checking.
			// It is set to contain some data based on the key
			object_size = (rand() % (MAX_SIZE - 4)) + 4;
			cost = ((rand() % 30000) + 1.34);
			//object_size = 1001;
			cptr = dataBuffer;
			*(int*)cptr = key;
			vdtValue.set_data( dataBuffer );
			vdtValue.set_size( object_size );

			// Cache misses should be assigned some penalty, otherwise
			//  the results end up rewarding the system for a cache miss.
			// This simulates having to do some extra work to retrieve an object from some
			//  external storage layer to return to the user and maybe put into the cache.
			// Using Sleep(0) in a loop because even Sleep(1) makes the program
			//  take a very long time to complete large numbers of requests.
			// Sleep is disabled during the cold start to allow it to get into steady state faster
			//for( int j = 0; !doingColdStart && j < 1000; j++ )
				//Sleep(0);

			total_time_cacheMiss += cost;

			if( tcm->Insert( &defaultDatazone, vdtKey, vdtValue, cost ) != 0 )
			{
				//std::cout<< "Key " << key << " was not inserted into cache\n";

				if( doingColdStart )
				{
					std::cout<< "Cold start has ended\n";
					doingColdStart = false;
					i = 0;
					QueryPerformanceCounter( &time_begin );
				}
			}
		}		

		if( i % 20000 == 0 )
			std::cout<< "Number of requests processed: " << i << endl;

		if( i == cold_start_multiple*num_requests && doingColdStart )
		{
			std::cout<< "Cold start has ended\n";
			doingColdStart = false;
			i = 0;
			QueryPerformanceCounter( &time_begin );
		}
	}

	QueryPerformanceCounter( &time_end );
	QueryPerformanceFrequency( &tick_per_second );
	
	runtime = (time_end.QuadPart - time_begin.QuadPart) / double(tick_per_second.QuadPart);

	std::cout<< "Processed " << num_requests << " in " 
		<< runtime << " seconds.\n";

	std::cout<< "Object retrieval test completed\n";

	std::string filename = "DistOfAccessResults.csv";
	std::cout<< "Writing results to a file: " << filename << "\n";
	std::ofstream fout;
	fout.open( filename.c_str(), std::ofstream::app );

	fout<< "CacheSize(bytes),NumKeys,NumRequests,ColdStart,Runtime(s),SlidingWindow-CacheHit(%),SlidingWindow-ByteHit(%),TotalCostOfCacheMisses,TotalRuntimeIncludingMisses(s)\n";
	fout<< tcm->GetConfigurationParams().MemBDB.CacheSize.GigaBytes * 1024 * 1024 * 1024 +
		tcm->GetConfigurationParams().MemBDB.CacheSize.Bytes << ","
		<< dist->GetNumObjects() << ","
		<< num_requests << ","
		<< cold_start_multiple << ","
		<< runtime << ","
		<< tcm->GetStatisticsManager()->GetCacheHitRate() << ","
		<< tcm->GetStatisticsManager()->GetByteHitRate() << ","
		<< total_time_cacheMiss << ","
		<< runtime + total_time_cacheMiss << "\n\n";

	fout.close();
	fout.clear();

	/*
	std::cout<< "Dumping contents of QDatabase to a file... ";
	fout.open( "QDBContents.csv", std::ofstream::out );
	tcm->DumpCache( fout );
	fout.close();
	std::cout<< "done\n";
	//*/
}


void testDistOfAccess()
{
	int NumReqs = 100000;
			
	DistOfAccess dist(10, "Zipfian", 0.27, 3, false, true);
	dist.setMakeRecording(true);

	for (int i = 0; i < NumReqs; i++)
	{
		dist.GenerateOneItem();
	}

	dist.EndTimer();
	double timer = dist.nTimeTaken();
	dist.PrintAccurracy();
}


DWORD WINAPI	WorkerThreadDistOfAccess( LPVOID lpParam )
{	
	return 0;
}
