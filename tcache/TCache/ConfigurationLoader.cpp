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
#include "ConfigurationLoader.h"
#include <sstream>

long long stringToLongLong( const char* num_str )
{
	std::istringstream stm;
	long long result;

	stm.str( num_str );
	stm >> result;

	return result;
}

int ConfigurationLoader::loadVConfigFile( 
	const char* filename, 
	ConfigParametersV &cp
	)
{
	bool retb = m_doc.LoadFile( filename );
	if( !retb )
	{
		//printf( "Could not load file \'%s\'. Error=\'%s\' Exiting.\n", filename, m_doc.ErrorDesc() );
		//return -1;
		printf( "Could not load file \'%s\'. Error=\'%s\'. Using default settings for memory-only configuration\n", filename, m_doc.ErrorDesc() );
	}

	TiXmlNode* root_node = 0; //m_doc.RootElement();.
	TiXmlNode* node = 0, *node2 = 0, *node3 = 0, *node4 = 0;
	TiXmlElement* element = 0;
	std::string current_element, temp_str;

	//ConfigParameters cp;
	memset( &cp, 0, sizeof(cp) );

	// Assign defaults
	cp.PrintConfiguration = true;

	cp.AgingInterval = 300000;	// 5 min
	
	cp.AsyncWrites.Enabled = false;
	cp.AsyncWrites.AsynchronousToSynchronousRatio = 0.2;
	cp.AsyncWrites.MaxSynchronousInserts = 8;

	cp.MemBDB.Enabled = true;
	cp.MemBDB.VictimizationPolicies.MaxVictimSelectionAttempts = 10;
	cp.MemBDB.VictimizationPolicies.MaxVictimSelectionByRecordsMultiple = 4;
	cp.MemBDB.VictimizationPolicies.MaxVictimSelectionSizeMultiple = 4;
	cp.MemBDB.ReplacementTechniqueID = 4;	// Interval-based Greedy Dual
	cp.MemBDB.CacheSize.Bytes = 20 * 1024 * 1024; // 20MB
	cp.MemBDB.CacheSize.NumberCaches = 1;
	cp.MemBDB.LogSize = 10 * 1024 * 1024;	// 10MB	
	cp.MemBDB.DatabaseConfigs.Partitions = 7;
	cp.MemBDB.MaxDeadlockRetries = 10;

	cp.DiskBDB.Enabled = false;
	cp.DiskBDB.CacheSize.Bytes = 32 * 1024 * 1024; // 32MB
	cp.DiskBDB.CacheSize.NumberCaches = 1;
	cp.DiskBDB.InMemoryLogging.Enabled = true;
	cp.DiskBDB.InMemoryLogging.LogSize = 10 * 1024 * 1024; // 10MB
	cp.DiskBDB.MaxDeadlockRetries = 10;


	
	if( !retb )
		return 0;

	try
	{
		current_element = "TCache";
		root_node = m_doc.FirstChild( current_element );
		if( root_node == NULL )
		{
			printf( "Root tag for XML file was missing. Using default settings for memory-only configuration\n" );
			return 0;
		}

		current_element = "PrintConfiguration";
		node = root_node->FirstChild( current_element );
		if( node != NULL )
		{
			if( strcmp(node->FirstChild()->Value(), "true") == 0 )
				cp.PrintConfiguration = true;
			else
				cp.PrintConfiguration = false;
		}


		current_element = "GrayPutzolu";
		node = root_node->FirstChild( current_element );
		if( node != NULL )
			cp.AgingInterval = atoi( node->FirstChild()->Value() );

		

		current_element = "AsyncWrites";
		node = root_node->FirstChild( current_element );

		if( node != NULL )
		{
			current_element = "Enabled";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
			{
				if( strcmp(node2->FirstChild()->Value(), "true") == 0 )
				{
					cp.AsyncWrites.Enabled = true;

					current_element = "MaxSynchronousInserts";
					node2 = node->FirstChild( current_element );
					if( node2 != NULL )
						cp.AsyncWrites.MaxSynchronousInserts = atoi( node2->FirstChild()->Value() );

					current_element = "AsynchronousToSynchronousRatio";
					node2 = node->FirstChild( current_element );
					if( node2 != NULL )
						cp.AsyncWrites.AsynchronousToSynchronousRatio = atof( node2->FirstChild()->Value() );
				}
				else
				{
					cp.AsyncWrites.Enabled = false;

					cp.AsyncWrites.AsynchronousToSynchronousRatio = 0.0;
				}
			}
		}

		current_element = "MemBDB";
		node = root_node->FirstChild( current_element );

		if( node == NULL )
		{
			cp.MemBDB.Enabled = false;
		}
		else
		{
			cp.MemBDB.Enabled = true;

			current_element = "VictimizationPolicies";
			node2 = node->FirstChild( current_element );

			if( node2 != NULL )
			{
				current_element = "MaxVictimSelectionSizeMultiple";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.MemBDB.VictimizationPolicies.MaxVictimSelectionSizeMultiple = atoi( node3->FirstChild()->Value() );

				current_element = "MaxVictimSelectionByRecordsMultiple";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.MemBDB.VictimizationPolicies.MaxVictimSelectionByRecordsMultiple = atoi( node3->FirstChild()->Value() );

				current_element = "MaxVictimSelectionAttempts";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.MemBDB.VictimizationPolicies.MaxVictimSelectionAttempts = atoi( node3->FirstChild()->Value() );
			}

			current_element = "ReplacementTechniqueID";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.MemBDB.ReplacementTechniqueID = atoi( node2->FirstChild()->Value() );

			current_element = "CorrelatedRefDelta";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.MemBDB.CorrelatedRefDelta = atoi( node2->FirstChild()->Value() );

			current_element = "CacheSize";
			node2 = node->FirstChild( current_element );

			if( node2 != NULL )				
			{
				current_element = "GigaBytes";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.MemBDB.CacheSize.GigaBytes = atoi( node3->FirstChild()->Value() );

				current_element = "Bytes";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.MemBDB.CacheSize.Bytes = atoi( node3->FirstChild()->Value() );

				current_element = "NumberCaches";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.MemBDB.CacheSize.NumberCaches = atoi( node3->FirstChild()->Value() );
			}


			current_element = "MaxLockers";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.MemBDB.MaxLockers = atoi( node2->FirstChild()->Value() );

			current_element = "MaxLocks";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.MemBDB.MaxLocks = atoi( node2->FirstChild()->Value() );

			current_element = "MaxLockObjects";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.MemBDB.MaxLockObjects = atoi( node2->FirstChild()->Value() );

			current_element = "MutexIncrement";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.MemBDB.MutexIncrement = atoi( node2->FirstChild()->Value() );

			current_element = "MaxMutexes";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.MemBDB.MaxMutexes = atoi( node2->FirstChild()->Value() );


			current_element = "LogSize";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.MemBDB.LogSize = atoi( node2->FirstChild()->Value() );


			current_element = "DatabaseConfigs";
			node2 = node->FirstChild( current_element );

			if( node2 != NULL )
			{
				current_element = "Partitions";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.MemBDB.DatabaseConfigs.Partitions = atoi( node3->FirstChild()->Value() );

				current_element = "CacheDBPageSize";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.MemBDB.DatabaseConfigs.CacheDBPageSize = atoi( node3->FirstChild()->Value() );

				current_element = "QDBPageSize";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.MemBDB.DatabaseConfigs.QDBPageSize = atoi( node3->FirstChild()->Value() );				
			}


			current_element = "MaxDeadlockRetries";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.MemBDB.MaxDeadlockRetries = atoi( node2->FirstChild()->Value() );

		}

		current_element = "DiskBDB";
		node = root_node->FirstChild( current_element );

		if( node == NULL )
		{
			cp.DiskBDB.Enabled = false;
		}
		else
		{
			cp.DiskBDB.Enabled = true;

			current_element = "CacheSize";
			node2 = node->FirstChild( current_element );

			if( node2 != NULL )				
			{
				current_element = "GigaBytes";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.DiskBDB.CacheSize.GigaBytes = atoi( node3->FirstChild()->Value() );

				current_element = "Bytes";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.DiskBDB.CacheSize.Bytes = atoi( node3->FirstChild()->Value() );

				current_element = "NumberCaches";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.DiskBDB.CacheSize.NumberCaches = atoi( node3->FirstChild()->Value() );
			}


			current_element = "HomeDirectory";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.DiskBDB.HomeDirectory = node2->FirstChild()->Value();

			current_element = "DataDirectory";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.DiskBDB.DataDirectory = node2->FirstChild()->Value();


			current_element = "MaxLockers";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.DiskBDB.MaxLockers = atoi( node2->FirstChild()->Value() );

			current_element = "MaxLocks";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.DiskBDB.MaxLocks = atoi( node2->FirstChild()->Value() );

			current_element = "MaxLockObjects";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.DiskBDB.MaxLockObjects = atoi( node2->FirstChild()->Value() );

			current_element = "MutexIncrement";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.DiskBDB.MutexIncrement = atoi( node2->FirstChild()->Value() );

			current_element = "MaxMutexes";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.DiskBDB.MaxMutexes = atoi( node2->FirstChild()->Value() );


			current_element = "InMemoryLogging";
			node2 = node->FirstChild( current_element );

			if( node2 != NULL )				
			{
				current_element = "Enabled";
				node3 = node2->FirstChild( current_element );
				if( strcmp(node3->FirstChild()->Value(), "true") == 0 )
					cp.DiskBDB.InMemoryLogging.Enabled = true;
				else
					cp.DiskBDB.InMemoryLogging.Enabled = false;

				current_element = "LogSize";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.DiskBDB.InMemoryLogging.LogSize = atoi( node3->FirstChild()->Value() );
			}
			

			current_element = "DatabaseConfigs";
			node2 = node->FirstChild( current_element );

			if( node2 != NULL )				
			{
				current_element = "Partitions";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.DiskBDB.DatabaseConfigs.Partitions = atoi( node3->FirstChild()->Value() );

				current_element = "PageSize";
				node3 = node2->FirstChild( current_element );
				if( node3 != NULL )
					cp.DiskBDB.DatabaseConfigs.PageSize = atoi( node3->FirstChild()->Value() );

				//current_element = "Type";
				//node3 = node2->FirstChild( current_element )->FirstChild();
				//if( node3 != NULL )
					//cp.DiskBDB.DatabaseConfigs.Type = node3->Value();
			}

			current_element = "MaxDeadlockRetries";
			node2 = node->FirstChild( current_element );
			if( node2 != NULL )
				cp.DiskBDB.MaxDeadlockRetries = atoi( node2->FirstChild()->Value() );
		}
	}
	catch( std::exception &e )
	{
		printf( "Error parsing config flie. %s\n", e.what() );
		printf( "Error occurred while parsing element: %s\n", current_element );
		return -1;
	}



	return 0;
}

int ConfigurationLoader::loadTraceConfigFile(
	const char *filename, 
	ConfigParametersTraceGen &cpt
	)
{
	bool retb = m_doc.LoadFile( filename );
	if( !retb )
	{
		printf( "Could not load file \'%s\'. Error=\'%s\' Exiting.\n", filename, m_doc.ErrorDesc() );
		return -1;
	}

	TiXmlNode* root_node = 0;
	TiXmlNode* node = 0;
	std::string current_element;

	memset( &cpt, 0, sizeof(cpt) );

	try
	{
		current_element = "SecondTraceGen";
		root_node = m_doc.FirstChild( current_element );

		current_element = "TraceFile";
		node = root_node->FirstChild( current_element );
		cpt.filename = node->FirstChild()->Value();

		current_element = "NumThreads";
		node = root_node->FirstChild( current_element );
		cpt.NumThreads = atoi( node->FirstChild()->Value() );

		current_element = "NumEltsPerThread";
		node = root_node->FirstChild( current_element );
		cpt.NumEltsPerThread = atoi( node->FirstChild()->Value() );

		current_element = "PrefetchSeconds";
		node = root_node->FirstChild( current_element );
		cpt.PrefetchSeconds = atoi( node->FirstChild()->Value() );


		current_element = "NumIterations";
		node = root_node->FirstChild( current_element );
		cpt.NumIterations = atoi( node->FirstChild()->Value() );

		/*
		current_element = "MaxThreads";
		node = root_node->FirstChild( current_element );
		cpt.MaxThreads = atoi( node->FirstChild()->Value() );

		current_element = "NumZones";
		node = root_node->FirstChild( current_element );
		cpt.NumZones = atoi( node->FirstChild()->Value() );

		current_element = "MaxObjectSize";
		node = root_node->FirstChild( current_element );
		cpt.MaxObjectSize = atoi( node->FirstChild()->Value() );
		//*/

	}
	catch( std::exception &e )
	{
		printf( "Error parsing config flie. %s\n", e.what() );
		printf( "Error occurred while parsing element: %s\n", current_element );
		return -1;
	}

	return 0;
}


int ConfigurationLoader::loadWLGConfigFile(
	const char *filename, 
	ConfigParametersWLG &cpw
	)
{
	bool retb = m_doc.LoadFile( filename );
	if( !retb )
	{
		printf( "Could not load file \'%s\'. Error=\'%s\' Exiting.\n", filename, m_doc.ErrorDesc() );
		return -1;
	}

	TiXmlNode* root_node = 0;
	TiXmlNode* node = 0;
	std::string current_element;

	memset( &cpw, 0, sizeof(cpw) );

	try
	{
		current_element = "WorkLoadGenerator";
		root_node = m_doc.FirstChild( current_element );

		current_element = "TraceFile";
		node = root_node->FirstChild( current_element );
		cpw.filename = node->FirstChild()->Value();

		current_element = "NumThreads";
		node = root_node->FirstChild( current_element );
		cpw.NumThreads = atoi( node->FirstChild()->Value() );

		current_element = "NumEltsPerThread";
		node = root_node->FirstChild( current_element );
		cpw.NumEltsPerThread = atoi( node->FirstChild()->Value() );

		current_element = "PrefetchSeconds";
		node = root_node->FirstChild( current_element );
		cpw.PrefetchSeconds = atoi( node->FirstChild()->Value() );


		current_element = "NumIterations";
		node = root_node->FirstChild( current_element );
		cpw.NumIterations = atoi( node->FirstChild()->Value() );

		/*
		current_element = "MaxThreads";
		node = root_node->FirstChild( current_element );
		cpt.MaxThreads = atoi( node->FirstChild()->Value() );

		current_element = "NumZones";
		node = root_node->FirstChild( current_element );
		cpt.NumZones = atoi( node->FirstChild()->Value() );

		current_element = "MaxObjectSize";
		node = root_node->FirstChild( current_element );
		cpt.MaxObjectSize = atoi( node->FirstChild()->Value() );
		//*/


		current_element = "NumPrefetchElements";
		node = root_node->FirstChild( current_element );
		cpw.NumPrefetchElements = atoi( node->FirstChild()->Value() );

		current_element = "InterarrivalTime";
		node = root_node->FirstChild( current_element );
		cpw.InterarrivalTime = atoi( node->FirstChild()->Value() );

		current_element = "MinLambda";
		node = root_node->FirstChild( current_element );
		cpw.MinLambda = atoi( node->FirstChild()->Value() );

		current_element = "MaxLambda";
		node = root_node->FirstChild( current_element );
		cpw.MaxLambda = atoi( node->FirstChild()->Value() );

		current_element = "Distribution";
		node = root_node->FirstChild( current_element );
		cpw.Distribution = node->FirstChild()->Value();



		current_element = "SimulationTime";
		node = root_node->FirstChild( current_element );
		cpw.SimulationTime = atoi( node->FirstChild()->Value() );

		current_element = "ColdStartMultiplier";
		node = root_node->FirstChild( current_element );
		cpw.ColdStartMultiplier = atof( node->FirstChild()->Value() );

		current_element = "TerminationThresholdMultiplier";
		node = root_node->FirstChild( current_element );
		cpw.TerminationThresholdMultiplier = atoi( node->FirstChild()->Value() );		


		current_element = "OutputFileName";
		node = root_node->FirstChild( current_element );
		cpw.OutputFileName = node->FirstChild()->Value();
	}
	catch( std::exception &e )
	{
		printf( "Error parsing config flie. %s\n", e.what() );
		printf( "Error occurred while parsing element: %s\n", current_element );
		return -1;
	}

	return 0;
}
