#include "stdafx.h"
#include "CachingAlgorithm.h"
#include <iostream>
using namespace std;


void testCachingAlgorithmGreedyDualSize();

void testCachingAlgorithmMain()
{
	testCachingAlgorithmGreedyDualSize();

}

void testCachingAlgorithmGreedyDualSize()
{
	cout<< "-- Caching Algorithm Test: Greedy Dual Size --\n";

	CachingAlgorithm *ca = new GreedyDualSizeAlgorithm();

	string sKey1 = "CachingAlgorithm#TestKey";
	Vdt catKey1 = TCUtility::convertToVdt( sKey1 );

	int dataBufferSize1 = 1024 * 1024;
	char* dataBuffer1 = new char[dataBufferSize1];
	int valueSize1 = 900;
	Dbt Value1( dataBuffer1, valueSize1 );
	Value1.set_ulen( dataBufferSize1 );
	Value1.set_flags( DB_DBT_USERMEM );

	int mdBufferSize1 = 128;
	char* mdBuffer1 = new char[mdBufferSize1];
	void* metadata1 = mdBuffer1;
	int metadata_size1 = 0;

	// Generate the first metadata record
	//ca->GenerateMetadata( catKey1, &Value1, metadata1, metadata_size1 );

	if( ca->GetDataSize() != metadata_size1 )
		cout<< "Error. Metadata size does not match\n";

	double q_val1 = ca->calcQValue( metadata1, 0.0 );

	string sKey2 = "CachingAlgorithm#TestKey";
	Vdt catKey2 = TCUtility::convertToVdt( sKey2 );
	int dataBufferSize2 = 1024 * 1024;
	char* dataBuffer2 = new char[dataBufferSize2];
	int valueSize2 = 900;
	Dbt Value2( dataBuffer2, valueSize2 );
	Value2.set_ulen( dataBufferSize2 );
	Value2.set_flags( DB_DBT_USERMEM );

	int mdBufferSize2 = 128;
	char* mdBuffer2 = new char[mdBufferSize2];
	void* metadata2 = mdBuffer2;
	int metadata_size2 = 0;

	// Generate the second metadata record
	//ca->GenerateMetadata( catKey2, &Value2, metadata2, metadata_size2 );

	if( ca->GetDataSize() != metadata_size2 )
		cout<< "Error. Metadata size does not match\n";

	double q_val2 = ca->calcQValue( metadata2, 0.0 );

	// Since the metadata was generated with the exact same starting values,
	//  the q value should be the same

	if( q_val1 != q_val2 )
		cout<< "Error. Q values do not match\n";

	// Generate a different metadata record, this time q values should be different
	// Generating a smaller object which should have a larger q value
	Value2.set_size( 300 );

	//ca->GenerateMetadata( catKey2, &Value2, metadata2, metadata_size2 );

	q_val2 = ca->calcQValue( metadata2, 0.0 );

	if( q_val1 >= q_val2 )
		cout<< "Error. Q value 1 should be less than Q value 2\n";

	// Test the Admission control
	double L_val = 15.0;
	double shouldAdmit = 35.0;
	double shouldnotAdmit = 5.0;

	if( ca->AdmitKey( shouldAdmit, L_val ) == false )
		cout<< "Error. Key should have been admitted. It's Q value was high enough\n";

	if( ca->AdmitKey( shouldnotAdmit, L_val ) == true )
		cout<< "Error. Key should not have been admitted. It's Q value was too low\n";

	cout<< "-- Caching Algorithm Test: end of Greedy Dual Size test --\n";
}


