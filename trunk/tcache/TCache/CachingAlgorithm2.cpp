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
#include "CachingAlgorithm.h"



LRUTimeAlgorithm::LRUTimeAlgorithm( const ConfigParametersV* cp )
	: CachingAlgorithm( cp )
{
	m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_WITHIN_DELTA_NO_UPDATE;

	m_dataSize = sizeof(md_timestamp);
}

double LRUTimeAlgorithm::calcQValue(const void* metadata, const double &L_value)
{
	LARGE_INTEGER timestamp = *(LARGE_INTEGER*) metadata;

	return double(timestamp.QuadPart);
}

int LRUTimeAlgorithm::GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size )
{
	LARGE_INTEGER timestamp = getTime();
	char* cptr = (char*)metadata; 

	*(LARGE_INTEGER*)cptr = timestamp;

	metadata_size = m_dataSize;

	return 0;
}

int LRUTimeAlgorithm::GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size )
{
	LARGE_INTEGER timestamp = getTime();
	char* cptr = (char*)metadata; 

	*(LARGE_INTEGER*)cptr = timestamp;

	metadata_size = m_dataSize;

	return 0;
}

int LRUTimeAlgorithm::UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata )
{
	return UpdateRead( metadata );
}

int LRUTimeAlgorithm::UpdateWrite( void* &metadata )
{
	return UpdateRead( metadata );
}

int LRUTimeAlgorithm::UpdateRead( void* &metadata )
{
	LARGE_INTEGER timestamp = getTime();

	char* cptr = (char*)metadata;
	*(LARGE_INTEGER*)cptr = timestamp;
	
	return 0;
}

bool LRUTimeAlgorithm::AdmitKey( const double& currQValue, const double& L_value )
{
	return true;
}

void LRUTimeAlgorithm::printMetadata( const void* metadata, std::ostream &out, const char& separator ) const
{
	LARGE_INTEGER timestamp = *(LARGE_INTEGER*) metadata;

	out<< "Timestamp = " << timestamp.QuadPart;
}

LARGE_INTEGER LRUTimeAlgorithm::getTime()
{
	LARGE_INTEGER currentTime;
	QueryPerformanceCounter( &currentTime );
	return currentTime;
}


// =============================================================================== //


LRU2TimeAlgorithm::LRU2TimeAlgorithm( const ConfigParametersV* cp )
	: CachingAlgorithm( cp )
{
	QueryPerformanceFrequency( &m_ticks_per_msec );
	m_ticks_per_msec.QuadPart /= 1000; // Convert from seconds to miliseconds

	QueryPerformanceCounter( &md_g_basetime );

	//m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_MAINTAIN_HISTORY | CA_FLAG_WITHIN_DELTA_NO_UPDATE;
	m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_WITHIN_DELTA_NO_UPDATE | CA_FLAG_LARGER_THAN_AVG_Q_NO_UPDATE;

	m_dataSize = sizeof(md_timestamp1) + sizeof(md_timestamp2);
}

double LRU2TimeAlgorithm::calcQValue(const void* metadata, const double &L_value)
{
	LARGE_INTEGER* liptr = (LARGE_INTEGER*)metadata;
	LARGE_INTEGER timestamp1 = *liptr++;
	LARGE_INTEGER timestamp2 = *liptr;

	LARGE_INTEGER diff;
	diff.QuadPart = timestamp1.QuadPart - timestamp2.QuadPart;

	if( diff.QuadPart == 0 )
		diff.QuadPart = 1;

	return double( 1.0 / diff.QuadPart );
}

int LRU2TimeAlgorithm::GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size )
{
	LARGE_INTEGER timestamp2 = md_g_basetime;
	LARGE_INTEGER timestamp1 = getTime();

	char* cptr = (char*)metadata;

	*(LARGE_INTEGER*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(LARGE_INTEGER*)cptr = timestamp2;

	metadata_size = m_dataSize;

	return 0;
}

int LRU2TimeAlgorithm::GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size )
{
	LARGE_INTEGER timestamp2 = md_g_basetime;
	LARGE_INTEGER timestamp1 = getTime();

	char* cptr = (char*)metadata;

	*(LARGE_INTEGER*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(LARGE_INTEGER*)cptr = timestamp2;

	metadata_size = m_dataSize;

	return 0;
}


int LRU2TimeAlgorithm::UpdateWrite( const CachingAlgorithmInputData& inputData,void* &metadata )
{	
	// Read and Write are treated as the same
	return UpdateRead(metadata);
}

int LRU2TimeAlgorithm::UpdateWrite( void* &metadata )
{	
	// Read and Write are treated as the same
	return UpdateRead(metadata);
}

int LRU2TimeAlgorithm::UpdateRead( void* &metadata )
{
	LARGE_INTEGER timestamp1 = *(LARGE_INTEGER*)metadata;
	LARGE_INTEGER timestamp2 = timestamp1;

	timestamp1 = getTime();

	// Copy data into buffer
	char* cptr = (char*)metadata;
	*(LARGE_INTEGER*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(LARGE_INTEGER*)cptr = timestamp2;
	
	return 0;
}

LARGE_INTEGER LRU2TimeAlgorithm::getTime()
{
	LARGE_INTEGER time;
	QueryPerformanceCounter( &time );
	
	return time;
}

bool LRU2TimeAlgorithm::AdmitKey( const double& currQValue, const double& L_value )
{
	return true;
}

void LRU2TimeAlgorithm::printMetadata( const void* metadata, std::ostream &out, const char& separator ) const
{
	LARGE_INTEGER* liptr = (LARGE_INTEGER*)metadata;
	LARGE_INTEGER timestamp1 = *liptr++;
	LARGE_INTEGER timestamp2 = *liptr;

	out<< "Timestamp1 = " << timestamp1.QuadPart << separator;
	out<< "Timestamp2 = " << timestamp2.QuadPart << separator;
	out<< "TimeDifference = " << (timestamp1.QuadPart - timestamp2.QuadPart) / double(m_ticks_per_msec.QuadPart) << "msec";
}


// =============================================================================== //



IntervalBasedGreedyDualTimeAlgorithm::IntervalBasedGreedyDualTimeAlgorithm( const ConfigParametersV* cp )
	: CachingAlgorithm( cp )
{
	QueryPerformanceCounter( &md_g_basetime );
	
	QueryPerformanceFrequency( &m_ticks_per_msec );
	m_ticks_per_msec.QuadPart /= 1000; // Convert from seconds to miliseconds

	//m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_MAINTAIN_HISTORY | CA_FLAG_WITHIN_DELTA_NO_UPDATE;
	//m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_WITHIN_DELTA_NO_UPDATE | CA_FLAG_LARGER_THAN_AVG_Q_NO_UPDATE;
	m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_WITHIN_DELTA_NO_UPDATE | CA_FLAG_LARGER_THAN_AVG_Q_NO_UPDATE;
	//m_flags = CA_FLAG_WITHIN_DELTA_NO_UPDATE;

	m_dataSize = sizeof(md_timestamp1) + sizeof(md_timestamp2) + sizeof(md_references) + 
				 sizeof(md_record_size) + sizeof(md_cost);
}

double IntervalBasedGreedyDualTimeAlgorithm::calcQValue(const void* metadata, const double &L_value)
{
	char* cptr = (char*)metadata;
	LARGE_INTEGER timestamp1 = *(LARGE_INTEGER*)cptr;
	cptr += sizeof( timestamp1 );
	LARGE_INTEGER timestamp2 = *(LARGE_INTEGER*)cptr;
	cptr += sizeof( timestamp2 );

	int references = *(int*)cptr;
	cptr += sizeof( references );

	int record_size = *(int*)cptr;
	cptr += sizeof( record_size );

	double cost = *(double*)cptr;

	// Avoid division by zero
	if( record_size == 0 )
		record_size = 1;

	LARGE_INTEGER diff;
	diff.QuadPart = timestamp1.QuadPart - timestamp2.QuadPart;
	if( diff.QuadPart == 0 )
		diff.QuadPart = 1;

	LARGE_INTEGER ticksPerSecond;
	QueryPerformanceFrequency( &ticksPerSecond );
	ticksPerSecond.QuadPart /= 1000000;	// Convert to ticks per microsecond

	diff.QuadPart = diff.QuadPart / ticksPerSecond.QuadPart;

	double denom = double( diff.QuadPart ) * record_size;

	return double( (references * cost / denom ) + L_value );
}

int IntervalBasedGreedyDualTimeAlgorithm::GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size )
{
	LARGE_INTEGER timestamp2 = md_g_basetime;
	LARGE_INTEGER timestamp1 = getTime();

	int references = 1;
	int record_size = dzName.get_size() + Key.get_size() + value->get_size();

	char* cptr = (char*)metadata;

	*(LARGE_INTEGER*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(LARGE_INTEGER*)cptr = timestamp2;
	cptr += sizeof(timestamp2);
	*(int*)cptr = references;
	cptr += sizeof(references);
	*(int*)cptr = record_size;	
	cptr += sizeof(record_size);
	*(double*)cptr = 1.0;	// Cost = 1.0

	metadata_size = m_dataSize;

	return 0;
}


int IntervalBasedGreedyDualTimeAlgorithm::GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size )
{
	LARGE_INTEGER timestamp2 = md_g_basetime;
	LARGE_INTEGER timestamp1 = getTime();

	int references = 1;
	int record_size = inputData.dzName->get_size() + inputData.Key->get_size() + inputData.value->get_size();

	char* cptr = (char*)metadata;

	*(LARGE_INTEGER*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(LARGE_INTEGER*)cptr = timestamp2;
	cptr += sizeof(timestamp2);
	*(int*)cptr = references;
	cptr += sizeof(references);
	*(int*)cptr = record_size;	
	cptr += sizeof(record_size);
	*(double*)cptr = inputData.cost;

	metadata_size = m_dataSize;

	return 0;
}

int IntervalBasedGreedyDualTimeAlgorithm::UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata )
{
	char* cptr = (char*) metadata;
	LARGE_INTEGER timestamp1 = *(LARGE_INTEGER*)cptr;
	cptr += sizeof( timestamp1 );
	LARGE_INTEGER timestamp2 = timestamp1;
	cptr += sizeof( timestamp2 );	// Skip over old timestamp. don't need it

	timestamp1 = getTime();

	int references = *(int*)cptr;
	cptr += sizeof( references );
	int record_size = inputData.dzName->get_size() + inputData.Key->get_size() + inputData.value->get_size();

	// Copy data into buffer
	cptr = (char*)metadata;
	*(LARGE_INTEGER*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(LARGE_INTEGER*)cptr = timestamp2;
	cptr += sizeof(timestamp2);
	*(int*)cptr = references + 1;
	cptr += sizeof(references);
	*(int*)cptr = record_size;

	// Cost doesn't change
	
	return 0;
}


int IntervalBasedGreedyDualTimeAlgorithm::UpdateWrite( void* &metadata )
{
	return UpdateRead(metadata);
}

int IntervalBasedGreedyDualTimeAlgorithm::UpdateRead( void* &metadata )
{
	char* cptr = (char*) metadata;
	LARGE_INTEGER timestamp1 = *(LARGE_INTEGER*)cptr;
	cptr += sizeof( timestamp1 );
	LARGE_INTEGER timestamp2 = timestamp1;
	cptr += sizeof( timestamp2 );	// Skip over old timestamp. don't need it

	timestamp1 = getTime();

	int references = *(int*)cptr;
	cptr += sizeof( references );
	int record_size = *(int*)cptr;

	// Copy data into buffer
	cptr = (char*)metadata;
	*(LARGE_INTEGER*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(LARGE_INTEGER*)cptr = timestamp2;
	cptr += sizeof(timestamp2);
	*(int*)cptr = references + 1;
	cptr += sizeof(references);
	*(int*)cptr = record_size;

	// Cost doesn't change
	
	return 0;
}

LARGE_INTEGER IntervalBasedGreedyDualTimeAlgorithm::getTime()
{
	LARGE_INTEGER time;
	QueryPerformanceCounter( &time );

	return time;
}

bool IntervalBasedGreedyDualTimeAlgorithm::AdmitKey( const double& currQValue, const double& L_value )
{
	if( (m_flags & CA_FLAG_ADMISSION_CONTROL) != 0 )
		if( currQValue - L_value < L_value )
			return false;

	return true;
}

void IntervalBasedGreedyDualTimeAlgorithm::printMetadata( const void* metadata, std::ostream &out, const char& separator ) const
{
	char* cptr = (char*)metadata;
	LARGE_INTEGER timestamp1 = *(LARGE_INTEGER*)cptr;
	cptr += sizeof( timestamp1 );
	LARGE_INTEGER timestamp2 = *(LARGE_INTEGER*)cptr;
	cptr += sizeof( timestamp2 );

	int references = *(int*)cptr;
	cptr += sizeof( references );
	int record_size = *(int*)cptr;
	cptr += sizeof( record_size );
	double cost = *(double*)cptr;

	out<< "Timestamp1 = " << timestamp1.QuadPart << separator;
	out<< "Timestamp2 = " << timestamp2.QuadPart << separator;
	out<< "TimeDifference = " << (timestamp1.QuadPart - timestamp2.QuadPart) / double(m_ticks_per_msec.QuadPart) << "msec" << separator;
	out<< "References = " << references << separator;
	out<< "Record Size = " << record_size << separator;
	out<< "Cost = " << cost; 
}


// =============================================================================== //


LRU_SKTimeAlgorithm::LRU_SKTimeAlgorithm( const ConfigParametersV* cp )
	: CachingAlgorithm( cp )
{
	QueryPerformanceCounter( &md_g_basetime );

	QueryPerformanceFrequency( &m_ticks_per_msec );
	m_ticks_per_msec.QuadPart /= 1000; // Convert from seconds to miliseconds

	//m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_MAINTAIN_HISTORY | CA_FLAG_WITHIN_DELTA_NO_UPDATE;
	//m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_WITHIN_DELTA_NO_UPDATE | CA_FLAG_LARGER_THAN_AVG_Q_NO_UPDATE;
	m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_WITHIN_DELTA_NO_UPDATE | CA_FLAG_LARGER_THAN_AVG_Q_NO_UPDATE;
	//m_flags = CA_FLAG_WITHIN_DELTA_NO_UPDATE;

	m_dataSize =	sizeof(md_timestamp1) + 
					sizeof(md_timestamp2) + 
					sizeof(md_record_size) + 
					sizeof(md_cost);
}

double LRU_SKTimeAlgorithm::calcQValue(const void* metadata, const double &L_value)
{
	char* cptr = (char*)metadata;
	LARGE_INTEGER timestamp1 = *(LARGE_INTEGER*)cptr;
	cptr += sizeof( timestamp1 );
	LARGE_INTEGER timestamp2 = *(LARGE_INTEGER*)cptr;
	cptr += sizeof( timestamp2 );

	int record_size = *(int*)cptr;
	cptr += sizeof( record_size );

	double cost = *(double*)cptr;

	// Avoid division by zero
	if( record_size == 0 )
		record_size = 1;

	LARGE_INTEGER diff;
	diff.QuadPart = timestamp1.QuadPart - timestamp2.QuadPart;
	if( diff.QuadPart == 0 )
		diff.QuadPart = 1;

	double denom = double( diff.QuadPart ) * record_size;

	return ( cost / denom ) + L_value;
}

int LRU_SKTimeAlgorithm::GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size )
{
	LARGE_INTEGER timestamp2 = md_g_basetime;
	LARGE_INTEGER timestamp1 = getTime();

	int record_size = dzName.get_size() + Key.get_size() + value->get_size();

	char* cptr = (char*)metadata;

	*(LARGE_INTEGER*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(LARGE_INTEGER*)cptr = timestamp2;
	cptr += sizeof(timestamp2);
	*(int*)cptr = record_size;
	cptr += sizeof(record_size);
	*(double*)cptr = 1.0;		// Default cost is 1.0
	

	metadata_size = m_dataSize;

	return 0;
}

int LRU_SKTimeAlgorithm::GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size )
{
	LARGE_INTEGER timestamp2 = md_g_basetime;
	LARGE_INTEGER timestamp1 = getTime();

	int record_size = inputData.dzName->get_size() + inputData.Key->get_size() + inputData.value->get_size();

	char* cptr = (char*)metadata;

	*(LARGE_INTEGER*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(LARGE_INTEGER*)cptr = timestamp2;
	cptr += sizeof(timestamp2);
	*(int*)cptr = record_size;
	cptr += sizeof(record_size);
	*(double*)cptr = inputData.cost;

	metadata_size = m_dataSize;

	return 0;
}

int LRU_SKTimeAlgorithm::UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata )
{
	char* cptr = (char*) metadata;
	LARGE_INTEGER timestamp1 = *(LARGE_INTEGER*)cptr;
	cptr += sizeof( timestamp1 );
	LARGE_INTEGER timestamp2 = timestamp1;
	cptr += sizeof( timestamp2 );	// Skip over old timestamp. don't need it

	timestamp1 = getTime();

	int record_size = inputData.dzName->get_size() + inputData.Key->get_size() + inputData.value->get_size();

	// Copy data into buffer
	cptr = (char*)metadata;
	*(LARGE_INTEGER*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(LARGE_INTEGER*)cptr = timestamp2;
	cptr += sizeof(timestamp2);
	*(int*)cptr = record_size;
	
	return 0;
}

int LRU_SKTimeAlgorithm::UpdateWrite( void* &metadata )
{
	return UpdateRead(metadata);
}

int LRU_SKTimeAlgorithm::UpdateRead( void* &metadata )
{
	char* cptr = (char*) metadata;
	LARGE_INTEGER timestamp1 = *(LARGE_INTEGER*)cptr;
	cptr += sizeof( timestamp1 );
	LARGE_INTEGER timestamp2 = timestamp1;
	cptr += sizeof( timestamp2 );	// Skip over old timestamp. don't need it

	timestamp1 = getTime();

	int record_size = *(int*)cptr;

	// Copy data into buffer
	cptr = (char*)metadata;
	*(LARGE_INTEGER*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(LARGE_INTEGER*)cptr = timestamp2;
	cptr += sizeof(timestamp2);
	*(int*)cptr = record_size;
	
	return 0;
}

LARGE_INTEGER LRU_SKTimeAlgorithm::getTime()
{
	LARGE_INTEGER time;
	QueryPerformanceCounter( &time );
	
	return time;
}

bool LRU_SKTimeAlgorithm::AdmitKey( const double& currQValue, const double& L_value )
{
	if( (m_flags & CA_FLAG_ADMISSION_CONTROL) != 0 )
		if( currQValue - L_value < L_value )
			return false;

	return true;
}

void LRU_SKTimeAlgorithm::printMetadata( const void* metadata, std::ostream &out, const char& separator ) const
{
	char* cptr = (char*)metadata;
	LARGE_INTEGER timestamp1 = *(LARGE_INTEGER*)cptr;
	cptr += sizeof( timestamp1 );
	LARGE_INTEGER timestamp2 = *(LARGE_INTEGER*)cptr;
	cptr += sizeof( timestamp2 );

	int record_size = *(int*)cptr;
	cptr += sizeof(record_size);

	double cost = *(double*)cptr;

	out<< "Timestamp1 = " << timestamp1.QuadPart << separator;
	out<< "Timestamp2 = " << timestamp2.QuadPart << separator;
	out<< "TimeDifference = " << (timestamp1.QuadPart - timestamp2.QuadPart) / double(m_ticks_per_msec.QuadPart) << "msec" << separator;
	out<< "Record Size = " << record_size << separator;
	out<< "Cost = " << cost;
}


// =============================================================================== //