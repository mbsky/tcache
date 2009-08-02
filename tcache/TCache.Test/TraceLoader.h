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

#ifndef TRACE_LOADER_H__
#define TRACE_LOADER_H__

#include "windows.h"
#include "GetOneTraceElt.h"
#include "Vdt.h"

#define MAXTHREADS 10
#define FILECHUNK 1000

#define TRACE99

#ifdef TRACE99 
	#define MAX_OBJECT_SIZE 29 * 1024 * 1024	// 29 MB	
	#define SEPERATOR ','
	#define NUMZONES 27
#else
	#define MAX_OBJECT_SIZE 1 * 1024 * 1024		// 1 MB
	#define SEPERATOR ';'
	#define NUMZONES 40 
#endif 

//#define VERBOSE 1
#define VERBOSE 0
#define DISPLAY_ITERATION 1

enum cmndType {Get, Insert, Delete};

struct EachSecond {
	char Assigned[MAXTHREADS];
	char Complete[MAXTHREADS];
	int NumTraceElts[MAXTHREADS];
	int *key[MAXTHREADS];
	int *zoneid[MAXTHREADS];
	int *size[MAXTHREADS];
	cmndType *cmnd[MAXTHREADS];
	struct EachSecond *next;
};

struct CacheUtilization {
	long long TotalByte;
	long long BytesFromMem;
	long long BytesInserted;
	int TotalReq;
	int TotalReqFromMem;
};


class TraceLoader
{
private:
	int PopulateOneSecond(struct EachSecond *elt, GetOneTraceElt *scanner, int numthreads, int EltsPerThread);

	CRITICAL_SECTION PL;

	EachSecond OneSecond;
	EachSecond* ArrayOfTraces;


	int		id;

	int		NumThreads;
	int		TotalNumElts;
	int		NumEltsPerThread;
	int		PrefetchSeconds;
	char	m_separator;

	Vdt		DZ[NUMZONES];
	char	FileName[NUMZONES];

	LPTHREAD_START_ROUTINE m_workerThreadFunction;

	std::string m_filename;

public:
	TraceLoader( std::string filename, 
				 const int& iNumThreads, 
				 const int& iPrefetchSeconds,
				 const int& iNumEltsPerThread,
				 LPTHREAD_START_ROUTINE worker_thread_function, 
				 char separator = SEPERATOR );
	~TraceLoader();

	int run();
	EachSecond*			getESptr() { return ArrayOfTraces; }
	CRITICAL_SECTION*	getCriticalSection() { return &PL; }
	Vdt*				getDZ() { return DZ; }
	CacheUtilization	Util[MAXTHREADS];
	
};

#endif // TRACE_LOADER_H__
