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

#ifndef SLIDING_WINDOW_DATA_H__
#define SLIDING_WINDOW_DATA_H__

#include <windows.h>
#include <list>

/**
 * SlidingWindowData
 * 
 * NOT THREAD SAFE - use only with a single thread or with synchronized access using multiple threads
 */
template< class T >
class SlidingWindowData
{
private:
	std::list<T>	m_bucketList;
	LARGE_INTEGER	m_lastUpdateTimestamp;
	LARGE_INTEGER	m_updateFrequency;
	T	m_totalValue;
	int	m_numBuckets;
	int m_numSecondsPerBucket;


	void addToHeadOfList( T value ) 
	{ 
		m_bucketList.front() += value;
		m_totalValue += value;
	}	

	void slideBuckets()
	{
		if( (int)m_bucketList.size() >= m_numBuckets )
		{
			m_totalValue -= m_bucketList.back();
			m_bucketList.pop_back();			
		}

		m_bucketList.push_front( 0 );
	}

public:
	SlidingWindowData( const int& num_buckets, const int& num_seconds_per_bucket ) 
		: m_numBuckets( num_buckets ), m_numSecondsPerBucket( num_seconds_per_bucket )
	{
		m_totalValue = 0;

		QueryPerformanceCounter( &m_lastUpdateTimestamp );
		QueryPerformanceFrequency( &m_updateFrequency );
		m_updateFrequency.QuadPart *= num_seconds_per_bucket;

		m_bucketList.push_front( 0 );
	}

	~SlidingWindowData()
	{

	}

	inline T getTotalValue() { return m_totalValue; }
	inline int getTotalWindowDuration() { return m_numBuckets * m_numSecondsPerBucket; }

	void addValue( T value )
	{
		LARGE_INTEGER curr_timestamp;
		QueryPerformanceCounter( &curr_timestamp );

		while( curr_timestamp.QuadPart - m_lastUpdateTimestamp.QuadPart >= m_updateFrequency.QuadPart )
		{
			slideBuckets();	
			m_lastUpdateTimestamp.QuadPart += m_updateFrequency.QuadPart;
		}

		addToHeadOfList( value );
	}

	void reset()
	{
		m_bucketList.clear();
		m_bucketList.push_front( 0 );
		m_totalValue = 0;
	}
};


#endif // SLIDING_WINDOW_DATA_H__