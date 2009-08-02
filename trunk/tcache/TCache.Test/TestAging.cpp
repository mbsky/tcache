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
#include <stdlib.h>
#include "TCache.h"
#include "Vdt.h"
#include "DistOfAccess.h"
#include "ConfigParameters.h"
#include "ConfigurationLoader.h"

void PerformGetRequestsAging( TCache *tcm, DistOfAccess *dist, const int& NumKeysInDB, const int& num_requests );

// Globals
Vdt	aging_defaultDatazone;


void testAgingMain( int argc, _TCHAR * argv[] )
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
	aging_defaultDatazone.set_data( (char*)datazoneName.c_str() );
	aging_defaultDatazone.set_size( (int)datazoneName.length() );

	if( argc == 3 )
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
	tcm.Create( &aging_defaultDatazone );


	PerformGetRequestsAging( &tcm, &dist, NumKeysInDB, NumReqs );


	std::cout<< "Test Completed. Shutting down database...\n";

	tcm.Shutdown();
	std::cout<< "Trojan Cache Manager shutdown completed\n";
}


void PerformGetRequestsAging( TCache *tcm, DistOfAccess *dist, const int& NumKeysInDB, const int& num_requests )
{
	Vdt vdtKey, vdtValue;
	double runtime = 0.0;
	double cost;
	const int MAX_SIZE = 2048;	
	char dataBuffer[MAX_SIZE];
	char* cptr;
	int	key;
	int object_size;
	int cold_start_multiple = 2;
	int num_popular = 100;
	bool doingColdStart = true;
	srand( 5 );

	std::cout<< "Retrieving objects from the Trojan Cache Manager...\n";

	LARGE_INTEGER time_begin, time_end, tick_per_second;

	// Insert a bunch of popular keys that will never be referenced. If aging is active they 
	//  should eventually get kicked out.
	for( int i = 1; i < num_popular; i++ )
	{
		key = NumKeysInDB + i;
		vdtKey.set_data( (char*) &key );
		vdtKey.set_size( sizeof(key) );

		cptr = dataBuffer;
		*(int*)cptr = key;
		vdtValue.set_data( dataBuffer );
		vdtValue.set_size( 4 );

		cost = 50000.0;

		if( tcm->Insert( &aging_defaultDatazone, vdtKey, vdtValue, cost ) != 0 )
		{
			std::cout<< "Error. Could not insert popular aging test object\n";
		}
	}
	
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
		if( tcm->Get( &aging_defaultDatazone, vdtKey, &vdtValue ) == 0 )
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
			object_size = (rand() % (MAX_SIZE - 100)) + 100;
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

			if( tcm->Insert( &aging_defaultDatazone, vdtKey, vdtValue, cost ) != 0 )
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

	fout<< "CacheSize(bytes),NumKeys,NumRequests,ColdStart,Runtime(s),SlidingWindow-CacheHit(%),SlidingWindow-ByteHit(%)\n";
	fout<< tcm->GetConfigurationParams().MemBDB.CacheSize.GigaBytes * 1024 * 1024 * 1024 +
		tcm->GetConfigurationParams().MemBDB.CacheSize.Bytes << ","
		<< dist->GetNumObjects() << ","
		<< num_requests << ","
		<< cold_start_multiple << ","
		<< runtime << ","
		<< tcm->GetStatisticsManager()->GetCacheHitRate() << ","
		<< tcm->GetStatisticsManager()->GetByteHitRate() << "\n";

	fout.close();
	fout.clear();

	std::cout<< "Dumping contents of QDatabase to a file... ";
	fout.open( "QDBContents.csv", std::ofstream::out );
	tcm->DumpCache( fout );
	fout.close();
	std::cout<< "done\n";
}

