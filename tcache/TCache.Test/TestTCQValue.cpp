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
#include <iostream>
#include "TCQValue.h"
#include "TCAging.h"

using namespace std;


void testTCQValueMain()
{
	TCQValue val1(1,0.03);
	TCQValue val2(0,0.0);
	TCQValue val3(5,0.1239615);
	TCQValue val4(5,0.000000000000000000000000000001234567);
	TCQValue val5(182698,0.000000000000000000000000000000000000000000000000000000000000001234567891234);

	cout<< "Sizeof int and double: " << sizeof(int) + sizeof(double) << " bytes\n";
	cout<< "Sizeof a TCQValue: " << sizeof(val1) << " bytes\n";
	cout<< "Val1: " << val1 << endl
		<< "Val2: " << val2 << endl
		<< "Val3: " << val3 << endl
		<< "Val4: " << val4 << endl
		<< "Val5: " << val5 << endl;

	if( val1 == val2 )
		cout<< "Error. 2 different values should not be equal\n";

	cout<< "Val3+Val1: " << val3 + val1 << endl;

	cout<< "Val3-Val1: " << val3 - val1 << endl;

	if( val1 > val3 || val4 > val3 || val2 >= val1 || val4 >= val3 || val3 <= val4 || val3 < val4 )
		cout<< "Error\n";

	if( val1 != val1 || val3 == val4 || val5 < val2 )
		cout<< "Error\n";
}

void testSleepTime()
{
	LARGE_INTEGER begin_time, end_time, total_begin, ticks_per_second;
	QueryPerformanceCounter( &total_begin );
	QueryPerformanceFrequency( &ticks_per_second );

	for( int i = 0; i < 4; i++ )
	{
		QueryPerformanceCounter( &begin_time );
		Sleep( 30000 );
		QueryPerformanceCounter( &end_time );

		cout<< i << " Slept for " << (end_time.QuadPart - begin_time.QuadPart) / double(ticks_per_second.QuadPart)
			<< " seconds this iteration\n";
	}

	cout<< "Slept for a total of " << (end_time.QuadPart - total_begin.QuadPart) / double(ticks_per_second.QuadPart)
		<< " seconds\n";
}

void testAging()
{
	LARGE_INTEGER begin_time, end_time, total_begin, ticks_per_second;
	QueryPerformanceCounter( &total_begin );
	QueryPerformanceFrequency( &ticks_per_second );

	TCAging age( 120, 0 );

	for( int i = 0; i < 20; i++ )
	{
		QueryPerformanceCounter( &begin_time );
		Sleep( 30000 );
		QueryPerformanceCounter( &end_time );

		cout<< i << " Slept for " << (end_time.QuadPart - begin_time.QuadPart) / double(ticks_per_second.QuadPart)
			<< " seconds this iteration\n";
		cout<< "The value of age is: " << age.getAgingValue() << endl;
	}
}