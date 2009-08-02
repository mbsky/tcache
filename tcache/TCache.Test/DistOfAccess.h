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

#ifndef DISTOFACCESS_H__
#define DISTOFACCESS_H__

#include <string>
#include <cstdlib>
#include <ctime>
#include <cmath>
#include <iostream>
#include <vector>
#include "LibRandom.h"
using namespace std;

/// <summary>
/// Summary description for DistOfAccess.
/// </summary>
class DistOfAccess
{
private:
	Random m_randNumGenerator;
	string m_CurrentDist;
	double m_ZipfianMean;
	int m_numberOfItems;
	vector<double> m_DistLevels;
	vector<double> m_DistValues;
	int* m_SV;
	int m_SV_Length;
	bool m_MakeRec;
	clock_t m_BeginTime;
	clock_t m_EndTime;
	clock_t m_span;
	double m_nTime;
	bool m_bBinarySearch; //  0 for linear, 1 for binary

	int LinearSearch(int nNum);	
	int BinarySearch(int nNum, int nStart, int nEnd);

	int getRandomNum( int max );

public:
	double nTimeTaken()	{ return m_nTime; }
	void StartTimer() {	m_BeginTime = clock(); }

	void EndTimer();	
	bool MakeRecording() { return m_MakeRec; }
	void setMakeRecording( bool value ) { m_MakeRec = value; }

	void InitZipfian(int numOfItems, double ZipfianMean, bool bBeginTimer);

	DistOfAccess(int numOfItems, string distname, double ZipfianMean, bool bBinary, bool bBeginTimer);
	DistOfAccess(int numOfItems, string distname, double ZipfianMean, int randomSeed, bool bBinary, bool bBeginTimer);
	
	int GenerateOneItem();
	int GetNumObjects() { return m_numberOfItems; }

	//index starts from one
	double GetFrequency(int index);

	void PrintAccurracy ();

};


#endif // DISTOFACCESS_H__