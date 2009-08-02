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

// TCache.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "ConfigurationLoader.h"
#include "ContextManager.h"
#include "TCache.h"

int lockTestMain();
int ContextTestMain();
//int runLockTrace();
int runDatazoneTrace();
int testQDatabaseMain();
void testCacheDatabaseMain();
void testCacheDatabaseMain2();
void testCachingAlgorithmMain();
int testTCMain();
int testTCMain( ConfigParametersV& cp, ConfigParametersTraceGen& cpt );
void runBatchFileWLG( int argc, _TCHAR* argv[] );
void runBatchTCMain( int argc, _TCHAR* argv[] );
int runBGPrototypeTrace(int argc, _TCHAR* argv[]);

int WLGmain(int argc, _TCHAR* argv[]);
int WLGmain(int argc, _TCHAR* argv[], const ConfigParametersV& cp, const ConfigParametersWLG& cpw );

void testTCQValueMain();
void testSleepTime();
void testAging();
void testDistOfAccessMain( int argc, _TCHAR* argv[] );
void testAgingMain( int argc, _TCHAR* argv[] );
int TCacheSimpleTest();

//*
int _tmain(int argc, _TCHAR* argv[])
{
	//lockTestMain();

	//testConfigLoader();

	//testContextManager();	

	//ContextTestMain();

	//runLockTrace();
	//runDatazoneTrace();
	//runDiskTrace();
	//runStatisticsManagerTrace();
	//runBGPrototypeTrace(argc, argv);
	
	//testQDatabaseMain();	
	
	//simpleTestCacheDatabaseMain();
	//testCacheDatabaseMain();

	//testCacheDatabaseMain();
	//testCacheDatabaseMain2();

	//testCachingAlgorithmMain();


	//testTCMain();
	//runBatchFileWLG( argc, argv );
	//runBatchTCMain( argc, argv );

	//testTCQValueMain();
	//testSleepTime();
	//testAging();
	//testDistOfAccessMain( argc, argv );
	//testAgingMain( argc, argv );

	//WLGmain(argc, argv);

	TCacheSimpleTest();


	return 0;
}
//*/

void runBatchFileWLG( int argc, _TCHAR* argv[] )
{
	ConfigParametersV cp;
	ConfigParametersTraceGen cpt;
	ConfigParametersWLG cpw;
	memset( &cp, 0, sizeof(cp) );
	memset( &cpt, 0, sizeof(cpt) );
	memset( &cpw, 0, sizeof(cpw) );


	ConfigurationLoader configLoader;
	//configLoader.loadTraceConfigFile( "VTestTraceGen.xml", cpt );
	//configLoader.loadVConfigFile( "V.xml", cp );
	//configLoader.loadVConfigFile( "V_WLG.xml", cp );
	//configLoader.loadTraceConfigFile( "WLG_Params.xml", cpt );
	
	std::string cp_xml_filename = "V_WLG.xml";
	std::string cpw_xml_filename = "WLG_Params.xml";

	if( argc == 3 )
	{
		std::wstring cp_input_arg = argv[1];
		std::wstring cpw_input_arg = argv[2];

		cp_xml_filename = TCUtility::wideStringToString( cp_input_arg );
		cpw_xml_filename = TCUtility::wideStringToString( cpw_input_arg );
	}
	else
	{
		std::cout<< "Using default xml files\n";
	}

	configLoader.loadVConfigFile( cp_xml_filename.c_str(), cp );
	configLoader.loadWLGConfigFile( cpw_xml_filename.c_str(), cpw );

	/*
	if( argc == 4 )
	{
		cp.MemBDB.CacheSize.Bytes = _ttoi( argv[1] ) * 1024 * 1024;
		cp.MemBDB.DatabaseConfigs.Partitions = _ttoi( argv[2] );
		cpt.NumThreads = _ttoi( argv[3] );
	}
	//*/

	//testTCMain( cp, cpt );
	WLGmain( argc, argv, cp, cpw );
}


void runBatchTCMain( int argc, _TCHAR* argv[] )
{
	ConfigParametersV cp;
	ConfigParametersTraceGen cpt;
	ConfigParametersWLG cpw;
	memset( &cp, 0, sizeof(cp) );
	memset( &cpt, 0, sizeof(cpt) );
	memset( &cpw, 0, sizeof(cpw) );


	ConfigurationLoader configLoader;
	//configLoader.loadTraceConfigFile( "VTestTraceGen.xml", cpt );
	//configLoader.loadVConfigFile( "V.xml", cp );
	//configLoader.loadVConfigFile( "V_WLG.xml", cp );
	//configLoader.loadTraceConfigFile( "WLG_Params.xml", cpt );
	
	std::string cp_xml_filename = "V.xml";
	std::string cpt_xml_filename = "VTestTraceGen.xml";

	if( argc == 3 )
	{
		std::wstring cp_input_arg = argv[1];
		std::wstring cpt_input_arg = argv[2];

		cp_xml_filename = TCUtility::wideStringToString( cp_input_arg );
		cpt_xml_filename = TCUtility::wideStringToString( cpt_input_arg );
	}
	else
	{
		std::cout<< "Using default xml files\n";
	}

	configLoader.loadVConfigFile( cp_xml_filename.c_str(), cp );
	configLoader.loadTraceConfigFile( cpt_xml_filename.c_str(), cpt );

	/*
	if( argc == 4 )
	{
		cp.MemBDB.CacheSize.Bytes = _ttoi( argv[1] ) * 1024 * 1024;
		cp.MemBDB.DatabaseConfigs.Partitions = _ttoi( argv[2] );
		cpt.NumThreads = _ttoi( argv[3] );
	}
	//*/

	testTCMain( cp, cpt );
	//WLGmain( argc, argv, cp, cpw );
}
