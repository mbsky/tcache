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

#include "StdAfx.h"
#include "CachingAlgorithm.h"
#include <iostream>


CachingAlgorithm::CachingAlgorithm( const ConfigParametersV* cp )
{
	//m_sizeofDataBuffer = TC_SM_BUFFER_SIZE;
	//memset( m_dataBuffer, 0, m_sizeofDataBuffer );

	m_dataSize = 0;
	m_flags = 0;
}

bool CachingAlgorithm::NoNeedToUpdateMetadata(	
	const double& oldQ, 
	const double& newQ, 
	const double& avgQ,
	const LARGE_INTEGER& oldTimestamp,
	const LARGE_INTEGER& currentTimestamp,
	const LARGE_INTEGER& updateDelta )
{
	//*
	// Check timestamp and don't update if last update was within a certain delta					
	if( (m_flags & CA_FLAG_WITHIN_DELTA_NO_UPDATE) != 0 )
	{
		if( currentTimestamp.QuadPart - oldTimestamp.QuadPart < updateDelta.QuadPart )
		{			
			return true;
		}
	}

	// Don't update metadata if the q value is the same ( assuming metadata didn't change )
	if( (m_flags & CA_FLAG_SAME_Q_NO_UPDATE) != 0 )
	{
		if( newQ == oldQ )
		{
			return true;
		}
	}

	// Don't update if the q value was already popular and still is ( > avg )
	if( (m_flags & CA_FLAG_LARGER_THAN_AVG_Q_NO_UPDATE) != 0 )
	{
		if( oldQ > avgQ && newQ > avgQ )
		{
			return true;
		}
	}
	//*/

	return false;
}

GreedyDualSizeAlgorithm::GreedyDualSizeAlgorithm( const ConfigParametersV* cp )
	: CachingAlgorithm( cp )
{
	m_dataSize = sizeof( md_record_size ) + sizeof( md_cost );
	//md_cost = 1.0;

	//m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_SAME_Q_NO_UPDATE | CA_FLAG_WITHIN_DELTA_NO_UPDATE;
	m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_WITHIN_DELTA_NO_UPDATE | CA_FLAG_LARGER_THAN_AVG_Q_NO_UPDATE;
}


double GreedyDualSizeAlgorithm::calcQValue(const void* metadata, const double &L_value)
{
	char* cptr = (char*)metadata;
	int record_size = *(int*) cptr;
	cptr += sizeof( record_size );	
	double cost = *(double*) cptr;

	if( record_size == 0 )
		record_size = 1;

	return cost / record_size + L_value;
}

int GreedyDualSizeAlgorithm::GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size )
{

	int record_size = dzName.get_size() + Key.get_size() + value->get_size();
	char* cptr = (char*)metadata;

	*(int*)cptr = record_size;
	cptr += sizeof( record_size );
	*(double*)cptr = 1.0; // Cost = 1.0

	metadata_size = m_dataSize;

	return 0;
}

int GreedyDualSizeAlgorithm::GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size )
{
	int record_size = inputData.dzName->get_size() + inputData.Key->get_size() + inputData.value->get_size();
	char* cptr = (char*)metadata;

	*(int*)cptr = record_size;
	cptr += sizeof( record_size );
	*(double*)cptr = inputData.cost;

	metadata_size = m_dataSize;

	
	return 0;
}

int GreedyDualSizeAlgorithm::UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata )
{
	int record_size = inputData.dzName->get_size() + inputData.Key->get_size() + inputData.value->get_size();
	char* cptr = (char*)metadata;

	*(int*)cptr = record_size;

	// Assuming cost doesn't change
	//cptr += sizeof( record_size );
	//*(double*)cptr = inputData.cost;
	
	return 0;
}

int GreedyDualSizeAlgorithm::UpdateWrite( void* &metadata )
{
	return UpdateRead( metadata );
}

int GreedyDualSizeAlgorithm::UpdateRead( void* &metadata )
{
	//int record_size = *(int*) metadata;
	//char* cptr = (char*)metadata; 

	//*(int*)cptr = record_size;
	
	return 0;
}

bool GreedyDualSizeAlgorithm::AdmitKey( const double& currQValue, const double& L_value )
{	
	if( (m_flags & CA_FLAG_ADMISSION_CONTROL) != 0 )
		if( currQValue - L_value < L_value )
			return false;

	return true;
}

void GreedyDualSizeAlgorithm::printMetadata( const void* metadata, std::ostream &out, const char& separator ) const
{
	char* cptr = (char*)metadata;
	int record_size = *(int*) cptr;	
	cptr += sizeof(record_size);
	double cost = *(double*) cptr;

	out<< "Record size = " << record_size << separator;
	out<< "Cost = " << cost;
}

LRUAlgorithm::LRUAlgorithm( const ConfigParametersV* cp )
	: CachingAlgorithm( cp )
{
	md_g_counter = 0;

	m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_WITHIN_DELTA_NO_UPDATE;
}

double LRUAlgorithm::calcQValue(const void* metadata, const double &L_value)
{
	int timestamp = *(int*) metadata;

	return double(timestamp);
}

int LRUAlgorithm::GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size )
{
	int timestamp = md_g_counter;
	InterlockedIncrement( &md_g_counter );

	char* cptr = (char*)metadata; 

	*(int*)cptr = timestamp;

	metadata_size = sizeof(timestamp);

	return 0;
}

int LRUAlgorithm::GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size )
{
	int timestamp = md_g_counter;
	InterlockedIncrement( &md_g_counter );

	char* cptr = (char*)metadata; 

	*(int*)cptr = timestamp;

	metadata_size = sizeof(timestamp);

	return 0;
}

int LRUAlgorithm::UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata )
{
	return UpdateRead( metadata );
}

int LRUAlgorithm::UpdateWrite( void* &metadata )
{
	return UpdateRead( metadata );
}

int LRUAlgorithm::UpdateRead( void* &metadata )
{
	int timestamp = md_g_counter;
	InterlockedIncrement( &md_g_counter );

	char* cptr = (char*)metadata;
	*(int*)cptr = timestamp;
	
	return 0;
}

bool LRUAlgorithm::AdmitKey( const double& currQValue, const double& L_value )
{

	return true;
}

void LRUAlgorithm::printMetadata( const void* metadata, std::ostream &out, const char& separator ) const
{
	int timestamp = *(int*) metadata;

	out<< "Timestamp = " << timestamp;
}

LRU2Algorithm::LRU2Algorithm( const ConfigParametersV* cp )
	: CachingAlgorithm( cp )
{
	// Start the counter at an offset so the early values don't get skewed
	md_g_counter = 5001;

	//m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_MAINTAIN_HISTORY | CA_FLAG_WITHIN_DELTA_NO_UPDATE;
	//m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_WITHIN_DELTA_NO_UPDATE;
	m_flags = CA_FLAG_WITHIN_DELTA_NO_UPDATE | CA_FLAG_LARGER_THAN_AVG_Q_NO_UPDATE;
}

double LRU2Algorithm::calcQValue(const void* metadata, const double &L_value)
{
	int* iptr = (int*)metadata;
	int timestamp1 = *iptr++;
	int timestamp2 = *iptr;

	int time_diff = timestamp1 - timestamp2;
	if( time_diff == 0 )
		time_diff = 1;

	return double( 1.0 / time_diff );
}

int LRU2Algorithm::GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size )
{
	int timestamp2 = 0;
	int timestamp1 = md_g_counter;
	incrementCounter();
	char* cptr = (char*)metadata;

	*(int*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(int*)cptr = timestamp2;

	metadata_size = sizeof(timestamp1) + sizeof(timestamp2);

	return 0;
}

int LRU2Algorithm::GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size )
{
	int timestamp2 = 0;
	int timestamp1 = md_g_counter;
	incrementCounter();
	char* cptr = (char*)metadata;

	*(int*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(int*)cptr = timestamp2;

	metadata_size = sizeof(timestamp1) + sizeof(timestamp2);

	return 0;
}

int LRU2Algorithm::UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata )
{
	// Read and Write are treated as the same
	return UpdateRead(metadata);
}

int LRU2Algorithm::UpdateWrite( void* &metadata )
{	
	// Read and Write are treated as the same
	return UpdateRead(metadata);
}

int LRU2Algorithm::UpdateRead( void* &metadata )
{
	int timestamp1 = *(int*)metadata;
	int timestamp2 = timestamp1;

	timestamp1 = md_g_counter;
	incrementCounter();

	// Copy data into buffer
	char* cptr = (char*)metadata;
	*(int*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(int*)cptr = timestamp2;
	
	return 0;
}

void LRU2Algorithm::incrementCounter()
{
	InterlockedIncrement( &md_g_counter );
}

bool LRU2Algorithm::AdmitKey( const double& currQValue, const double& L_value )
{
	if( (m_flags & CA_FLAG_ADMISSION_CONTROL) != 0 )
		if( currQValue < L_value )
			return false;

	return true;
}

void LRU2Algorithm::printMetadata( const void* metadata, std::ostream &out, const char& separator ) const
{
	int* iptr = (int*)metadata;
	int timestamp1 = *iptr++;
	int timestamp2 = *iptr;

	out<< "Timestamp1 = " << timestamp1 << separator;
	out<< "Timestamp2 = " << timestamp2;
}



IntervalBasedGreedyDualAlgorithm::IntervalBasedGreedyDualAlgorithm( const ConfigParametersV* cp )
	: CachingAlgorithm( cp )
{
	// Start the counter at an offset so the early values don't get skewed
	md_g_counter = 5001;

	m_flags = 0;
	m_flags |= CA_FLAG_ADMISSION_CONTROL;
	m_flags |= CA_FLAG_WITHIN_DELTA_NO_UPDATE;
	//m_flags |= CA_FLAG_MAINTAIN_HISTORY;
	m_flags |= CA_FLAG_LARGER_THAN_AVG_Q_NO_UPDATE;

	//m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_MAINTAIN_HISTORY | CA_FLAG_WITHIN_DELTA_NO_UPDATE;
	//m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_WITHIN_DELTA_NO_UPDATE | CA_FLAG_LARGER_THAN_AVG_Q_NO_UPDATE;
	//m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_WITHIN_DELTA_NO_UPDATE;
	//m_flags = CA_FLAG_WITHIN_DELTA_NO_UPDATE;

	m_dataSize =	sizeof(md_timestamp1) + 
					sizeof(md_timestamp2) + 
					sizeof(md_references) + 
					sizeof(md_record_size) +
					sizeof(md_cost);
}

double IntervalBasedGreedyDualAlgorithm::calcQValue(const void* metadata, const double &L_value)
{
	int* iptr = (int*)metadata;
	int timestamp1 = *iptr++;
	int timestamp2 = *iptr++;
	int references = *iptr++;
	int record_size = *iptr++;
	double cost = *(double*)iptr;

	// Avoid division by zero
	if( record_size == 0 )
		record_size = 1;

	int diff = timestamp1 - timestamp2;
	if( diff == 0 )
		diff = 1;

	// Scale it so that the smaller Q values don't get lost due to the fiveMinuteRule
	double denom = double( diff ) * record_size;

	return (references * cost / denom ) + L_value;
}

int IntervalBasedGreedyDualAlgorithm::GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size )
{
	int timestamp2 = 0;
	int timestamp1 = md_g_counter;
	incrementCounter();
	int references = 1;
	int record_size = dzName.get_size() + Key.get_size() + value->get_size();

	char* cptr = (char*)metadata;

	*(int*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(int*)cptr = timestamp2;
	cptr += sizeof(timestamp2);
	*(int*)cptr = references;
	cptr += sizeof(references);
	*(int*)cptr = record_size;
	cptr += sizeof(record_size);
	*(double*)cptr = 1.0;			// Cost is 1.0 by default

	metadata_size = m_dataSize;

	return 0;
}

int IntervalBasedGreedyDualAlgorithm::GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size )
{
	int timestamp2 = 0;
	int timestamp1 = md_g_counter;
	incrementCounter();
	int references = 1;
	int record_size = inputData.dzName->get_size() + inputData.Key->get_size() + inputData.value->get_size();

	char* cptr = (char*)metadata;

	*(int*)cptr = timestamp1;
	cptr += sizeof(timestamp1);

	*(int*)cptr = timestamp2;
	cptr += sizeof(timestamp2);

	*(int*)cptr = references;
	cptr += sizeof(references);

	*(int*)cptr = record_size;	
	cptr += sizeof(record_size);

	*(double*)cptr = inputData.cost;

	metadata_size = m_dataSize;

	return 0;
}

int IntervalBasedGreedyDualAlgorithm::UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata )
{
	int* iptr = (int*) metadata;
	int timestamp1 = *iptr++;
	int timestamp2 = timestamp1;
	iptr++;		// Skip over old timestamp. don't need it

	timestamp1 = md_g_counter;
	incrementCounter();

	int references = *iptr++;

	int record_size = inputData.dzName->get_size() + inputData.Key->get_size() + inputData.value->get_size();

	char* cptr = (char*)metadata;

	*(int*)cptr = timestamp1;
	cptr += sizeof(timestamp1);

	*(int*)cptr = timestamp2;
	cptr += sizeof(timestamp2);

	*(int*)cptr = references + 1;
	cptr += sizeof(references);

	*(int*)cptr = record_size;	
	cptr += sizeof(record_size);

	return 0;
}


int IntervalBasedGreedyDualAlgorithm::UpdateWrite( void* &metadata )
{
	return UpdateRead(metadata);
}

int IntervalBasedGreedyDualAlgorithm::UpdateRead( void* &metadata )
{
	int* iptr = (int*) metadata;
	int timestamp1 = *iptr++;
	int timestamp2 = timestamp1;
	iptr++;		// Skip over old timestamp. don't need it

	timestamp1 = md_g_counter;
	incrementCounter();

	int references = *iptr++;
	int record_size = *iptr;

	// Copy data into buffer
	char* cptr = (char*)metadata;
	*(int*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(int*)cptr = timestamp2;
	cptr += sizeof(timestamp2);
	*(int*)cptr = references + 1;

	// Cost and record size do not change, so no need to update
	
	return 0;
}

void IntervalBasedGreedyDualAlgorithm::incrementCounter()
{
	InterlockedIncrement( &md_g_counter );
}

bool IntervalBasedGreedyDualAlgorithm::AdmitKey( const double& currQValue, const double& L_value )
{
	if( (m_flags & CA_FLAG_ADMISSION_CONTROL) != 0 )
		if( currQValue - L_value < L_value )
			return false;

	return true;
}

void IntervalBasedGreedyDualAlgorithm::printMetadata( const void* metadata, std::ostream &out, const char& separator ) const
{
	int* iptr = (int*)metadata;
	int timestamp1 = *iptr++;
	int timestamp2 = *iptr++;
	int references = *iptr++;
	int record_size = *iptr++;
	double cost = *(double*)iptr;

	out<< "Timestamp1 = " << timestamp1 << separator;
	out<< "Timestamp2 = " << timestamp2 << separator;
	out<< "References = " << references << separator;
	out<< "Record Size = " << record_size << separator;
	out<< "Cost = " << cost;
}



LRU_SKAlgorithm::LRU_SKAlgorithm( const ConfigParametersV* cp )
	: CachingAlgorithm( cp )
{
	// Start the counter at an offset so the early values don't get skewed
	md_g_counter = 5001;

	//m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_MAINTAIN_HISTORY | CA_FLAG_WITHIN_DELTA_NO_UPDATE;
	m_flags = CA_FLAG_ADMISSION_CONTROL | CA_FLAG_WITHIN_DELTA_NO_UPDATE | CA_FLAG_LARGER_THAN_AVG_Q_NO_UPDATE;

	//if( cp->MemBDB.DeadlockDetection.Enabled )
	//	m_flags |= CA_FLAG_LARGER_THAN_AVG_Q_NO_UPDATE;
	
	m_dataSize =	sizeof(md_timestamp1) + 
					sizeof(md_timestamp2) +
					sizeof(md_record_size) + 
					sizeof(md_cost);
}

double LRU_SKAlgorithm::calcQValue(const void* metadata, const double &L_value)
{
	int* iptr = (int*)metadata;
	int timestamp1 = *iptr++;
	int timestamp2 = *iptr++;
	int record_size = *iptr++;
	double cost = *(double*)iptr;

	if( record_size == 0 )
		record_size = 1;

	int diff = timestamp1 - timestamp2;
	if( diff == 0 )
		diff = 1;

	double denom = double( diff ) * record_size;

	return ( cost / denom ) + L_value;
}

int LRU_SKAlgorithm::GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size )
{
	int timestamp2 = 0;
	int timestamp1 = md_g_counter;
	incrementCounter();
	int record_size = dzName.get_size() + Key.get_size() + value->get_size();

	char* cptr = (char*)metadata;

	*(int*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(int*)cptr = timestamp2;
	cptr += sizeof(timestamp2);
	*(int*)cptr = record_size;	
	cptr += sizeof(record_size);
	*(double*)cptr = 1.0;		// Default Cost is 1.0

	metadata_size = m_dataSize;

	return 0;
}

int LRU_SKAlgorithm::GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size )
{
	int timestamp2 = 0;
	int timestamp1 = md_g_counter;
	incrementCounter();
	int record_size = inputData.dzName->get_size() + inputData.Key->get_size() + inputData.value->get_size();

	char* cptr = (char*)metadata;

	*(int*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(int*)cptr = timestamp2;
	cptr += sizeof(timestamp2);
	*(int*)cptr = record_size;	
	cptr += sizeof(record_size);
	*(double*)cptr = inputData.cost;

	metadata_size = m_dataSize;

	return 0;
}

int LRU_SKAlgorithm::UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata )
{
	int* iptr = (int*) metadata;
	int timestamp1 = *iptr++;
	int timestamp2 = timestamp1;
	iptr++;			// Skip over old timestamp. don't need it

	timestamp1 = md_g_counter;
	incrementCounter();

	int record_size = inputData.dzName->get_size() + inputData.Key->get_size() + inputData.value->get_size();

	// Copy data into buffer
	char* cptr = (char*)metadata;
	*(int*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(int*)cptr = timestamp2;
	cptr += sizeof(timestamp2);
	*(int*)cptr = record_size;
	
	return 0;
}


int LRU_SKAlgorithm::UpdateWrite( void* &metadata )
{
	return UpdateRead(metadata);
}

int LRU_SKAlgorithm::UpdateRead( void* &metadata )
{
	int* iptr = (int*) metadata;
	int timestamp1 = *iptr++;
	int timestamp2 = timestamp1;
	iptr++;			// Skip over old timestamp. don't need it

	timestamp1 = md_g_counter;
	incrementCounter();

	int record_size = *iptr;

	// Copy data into buffer
	char* cptr = (char*)metadata;
	*(int*)cptr = timestamp1;
	cptr += sizeof(timestamp1);
	*(int*)cptr = timestamp2;
	cptr += sizeof(timestamp2);
	*(int*)cptr = record_size;
	
	return 0;
}

void LRU_SKAlgorithm::incrementCounter()
{
	InterlockedIncrement( &md_g_counter );
}

bool LRU_SKAlgorithm::AdmitKey( const double& currQValue, const double& L_value )
{
	if( (m_flags & CA_FLAG_ADMISSION_CONTROL) != 0 )
		if( currQValue - L_value < L_value )
			return false;

	return true;
}

void LRU_SKAlgorithm::printMetadata( const void* metadata, std::ostream &out, const char& separator ) const
{
	int* iptr = (int*)metadata;
	int timestamp1 = *iptr++;
	int timestamp2 = *iptr++;
	int record_size = *iptr++;
	double cost = *(double*)iptr;

	out<< "Timestamp1 = " << timestamp1 << separator;
	out<< "Timestamp2 = " << timestamp2 << separator;
	out<< "Record Size = " << record_size << separator;
	out<< "Cost = " << cost;
}