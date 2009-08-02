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

#ifndef __CONFIG_PARAMETERS_H_
#define __CONFIG_PARAMETERS_H_

#include <string>
#include "Common.h"

struct ConfigParametersV
{
	bool PrintConfiguration;
	uint AgingInterval;

	struct CP_AsyncWrites
	{
		bool Enabled;
		uint	MaxSynchronousInserts;
		double	AsynchronousToSynchronousRatio;
	}AsyncWrites;

	struct CP_MemBDB
	{
		bool Enabled;

		struct CP_VictimizationPolicies
		{
			uint MaxVictimSelectionSizeMultiple;
			uint MaxVictimSelectionByRecordsMultiple;
			uint MaxVictimSelectionAttempts;
		}VictimizationPolicies;

		int	ReplacementTechniqueID;

		uint CorrelatedRefDelta;

		struct CP_CacheSize
		{
			uint GigaBytes;
			uint Bytes;
			uint NumberCaches;
		}CacheSize;
		
		uint MaxLockers;
		uint MaxLocks;
		uint MaxLockObjects;
		uint MutexIncrement;
		uint MaxMutexes;

		uint LogSize;

		struct CP_DatabaseConfigs
		{
			uint Partitions;
			uint CacheDBPageSize;
			uint QDBPageSize;			
		}DatabaseConfigs;

		uint MaxDeadlockRetries;
	}MemBDB;

	struct CP_DiskBDB
	{
		bool Enabled;

		struct CP_CacheSize
		{
			uint GigaBytes;
			uint Bytes;
			uint NumberCaches;
		}CacheSize;

		struct CP_CacheTrickle
		{
			bool Enabled;
			uint Interval;
			double Percentage;
		}CacheTrickle;

		std::string HomeDirectory;
		std::string DataDirectory;

		uint MaxLockers;
		uint MaxLocks;
		uint MaxLockObjects;
		uint MutexIncrement;
		uint MaxMutexes;

		struct CP_InMemoryLogging
		{
			bool	Enabled;
			uint	LogSize;
		}InMemoryLogging;

		struct CP_DatabaseConfigs
		{
			uint Partitions;
			uint PageSize;
			std::string Type;
		}DatabaseConfigs;

		uint MaxDeadlockRetries;
	}DiskBDB;
};


struct ConfigParametersTraceGen
{
	std::string filename;
	uint NumThreads;
	uint NumEltsPerThread;
	uint PrefetchSeconds;
	uint MaxThreads;
	uint NumZones;
	uint MaxObjectSize;
	uint NumIterations;
};

struct ConfigParametersWLG
{
	std::string filename;
	uint NumThreads;
	uint NumEltsPerThread;
	uint PrefetchSeconds;
	uint MaxThreads;
	uint NumZones;
	uint MaxObjectSize;
	uint NumIterations;

	uint NumPrefetchElements;
	uint InterarrivalTime;
	uint MinLambda;
	uint MaxLambda;
	std::string Distribution;

	uint SimulationTime;
	double ColdStartMultiplier;
	uint TerminationThresholdMultiplier;

	std::string OutputFileName;
};

#endif //__CONFIG_PARAMETERS_H_