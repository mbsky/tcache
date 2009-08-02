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
#include "TCAging.h"

volatile long TCAging::m_agingValue;

TCAging::TCAging( const int &aging_interval_milliseconds, const int& init_aging_value )
:	m_agingIntervalMSec( aging_interval_milliseconds )
{	
	m_agingValue = init_aging_value;

	// TODO: Aging is disabled for now
	// start up background thread to increment the aging counter 
	//       every 'aging_interval_seconds' seconds	
	m_threadHandle = CreateThread( NULL, 0, &TCAging::backgroundIncrementAgingThread, &m_agingIntervalMSec, 0, NULL );
}

TCAging::~TCAging()
{
	//CloseHandle( m_threadHandle );
}

DWORD WINAPI TCAging::backgroundIncrementAgingThread(LPVOID lpParam)
{
	// TODO: this thread just runs continously until the program terminates.
	//		 it sleeps most of the time, so not sure if its necessary or 
	//		  possible to gracefully exit exactly when we want it to
	while( true )
	{
		Sleep( *(int*) lpParam );
		InterlockedIncrement( &m_agingValue );
	}

	return -1;
}