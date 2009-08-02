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
#include "DistOfAccess.h"


int DistOfAccess::LinearSearch(int nNum)
{
	int randMovie = 0;
	for (int i=0; i<=m_numberOfItems; i++)
	{
		if ((double)m_DistLevels[i] > nNum)
		{
			randMovie = i;
			break;
		}
	}
	return randMovie;
}


int DistOfAccess::BinarySearch(int nNum, int nStart, int nEnd)
{
	int nIndex = (nEnd-nStart)/2;
	nIndex+=nStart;
	if((double)m_DistLevels[nIndex] <= nNum && (double)m_DistLevels[nIndex+1] > nNum)
		return nIndex+1;
	else if((double)m_DistLevels[nIndex] >= nNum && (double)m_DistLevels[nIndex+1] >= nNum)
		return BinarySearch(nNum, nStart, nIndex);
	else if((double)m_DistLevels[nIndex] <= nNum && (double)m_DistLevels[nIndex+1] <= nNum)
		return BinarySearch(nNum, nIndex+1, nEnd);
	else
		return nEnd;
}



void DistOfAccess::EndTimer()
{
	m_EndTime = clock();
	m_span = m_EndTime - m_BeginTime;
	m_nTime = double(m_span / CLOCKS_PER_SEC);
}




void DistOfAccess::InitZipfian(int numOfItems, double ZipfianMean, bool bBeginTimer)
{
	m_numberOfItems = numOfItems;
	m_ZipfianMean = ZipfianMean;
	if(bBeginTimer)
		StartTimer();

	m_SV_Length = numOfItems+1;
	m_SV = new int[m_SV_Length];

	for (int i = 0 ; i < numOfItems+1 ; i++) 
		m_SV[i] = 0;

	m_DistLevels.push_back(0.0);
	m_DistValues.push_back(0.0);
	for (int i=1; i<=m_numberOfItems; i++)
	{
		if ( m_CurrentDist.compare("Zipfian") == 0 )
			m_DistValues.push_back(100 * pow(i, -(1-ZipfianMean))/pow(m_numberOfItems, -(1-ZipfianMean)));
		else 
			m_DistValues.push_back(10.0);

		m_DistLevels.push_back((double)m_DistLevels[i-1] + (double)m_DistValues[i]);
	}
}

DistOfAccess::DistOfAccess(int numOfItems, string distname, double ZipfianMean, bool bBinary, bool bBeginTimer)
{
	if ( distname.compare("U") == 0 || 
		distname.compare("u") == 0 || 
		distname.compare("Uniform") == 0 || 
		distname.compare("Unif") == 0 || 
		distname.compare("uniform") == 0 || 
		distname.compare("UNIFORM") == 0 || 
		distname.compare("UNIF") == 0 )
		m_CurrentDist="Uniform";
	else 
		m_CurrentDist = "Zipfian";
	m_bBinarySearch = bBinary;
	InitZipfian(numOfItems, ZipfianMean, bBeginTimer);

}

DistOfAccess::DistOfAccess(int numOfItems, string distname, double ZipfianMean, int randomSeed, bool bBinary, bool bBeginTimer)
{
	cout<< "distname = " << distname << endl;
	if ( distname.compare("U") == 0 || 
		distname.compare("u") == 0 || 
		distname.compare("Uniform") == 0 || 
		distname.compare("Unif") == 0 || 
		distname.compare("uniform") == 0 || 
		distname.compare("UNIFORM") == 0 || 
		distname.compare("UNIF") == 0 )
		m_CurrentDist="Uniform";
	else 
		m_CurrentDist = "Zipfian";
	m_bBinarySearch = bBinary;

	//this.rClip = new Random(randomSeed);
	//srand( randomSeed );
	m_randNumGenerator.setRandomSeed( randomSeed );

	InitZipfian(numOfItems, ZipfianMean, bBeginTimer);
}

int DistOfAccess::getRandomNum( int max )
{
	//double r = ( (double)rand() / ((double)(RAND_MAX)+1) );
	//return int(r * max);

	return m_randNumGenerator.uniform( (unsigned int)max );
}

int DistOfAccess::GenerateOneItem()
{
	int randMovie = 0;
	double max_dbl = m_DistLevels[m_numberOfItems];
	int temp = (int) max_dbl;
	int max = (int)(double)m_DistLevels[m_numberOfItems];
	//int movieIndex = rClip.Next(max);
	int movieIndex = getRandomNum( max );

	if(!m_bBinarySearch)
		randMovie = LinearSearch(movieIndex);
	else
		randMovie = BinarySearch(movieIndex, 0 , m_numberOfItems);

	if (m_MakeRec)
	{
		// Console.WriteLine("item is "+randMovie);
		if (randMovie >= 0 && randMovie <= m_SV_Length)
			m_SV[randMovie] += 1;
		else 
			cout<< "Error in DistOfAccess.cs, indexing item " 
			<< randMovie 
			<< " which is out of range.\n";
	}
	return randMovie;
}

//index starts from one
double DistOfAccess::GetFrequency(int index)
{
	if (index < 1 || index > m_numberOfItems) 
		return -1;
	return (double)m_DistValues[index] / (double)m_DistLevels[m_numberOfItems];
}

void DistOfAccess::PrintAccurracy ()
{
	if (m_MakeRec)
	{
		cout<< "Item \t Obs Freq \t Exp Freq \t Freq Err\n";
		int TotalSamples = 0;
		for (int i = 0; i < m_numberOfItems+1 ; i++)
			TotalSamples += m_SV[i];
		if (TotalSamples > 0)
		{
			double ObsFreq = 0.0;
			for (int i = 1; i < m_numberOfItems+1 ; i++)
			{
				ObsFreq = (double) m_SV[i] / TotalSamples;
				// Console.WriteLine("Elt "+ i + ", number of occ is "+SV[i]+", freq is "+ObsFreq);
				cout<< "" << i << " \t " << ObsFreq << " \t " << GetFrequency(i) 
					<< " \t " << (double) 100 * (GetFrequency(i) - ObsFreq ) / GetFrequency(i) << endl;
			}
		} 
		else 
			cout<< "Error, total samples is " << TotalSamples << endl;
	} 
	else 
	{
		cout<< "Error, MakeRecording was not enabled.\n";
		cout<< "Enable MakeRecording must be enabled to gather statistics.\n";
		cout<< "Usage:  DistOfAccess.MakeRecording = true\n";
	}
}