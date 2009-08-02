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
#include "PoissonD.h"
#include <math.h>
#include<stdlib.h>
#include <stdio.h>
#include <cstdlib>



PoissonD::PoissonD(int lambda_i, long timeUnit_i)
{
	errorThreshold = 10; //Acceptable error thresholds are 5, 10, and 15%
	randomNumberGenSeed = 2008; 
	arrRate = 100;
	lambda =0;
	timeUnit = 0;    
	differedReq = 0;
	differedTime = 0;
	differedArrival = 0;
	srand(PoissonD::randomNumberGenSeed);
	  timeUnit = timeUnit_i * 1000; //shifting time unit to the granularity of the microsecond
	  lambda = lambda_i;
	  arrRate = ((3600 * 1000) / timeUnit_i) * lambda_i;  //how many request should be issued in an hour

	//check the errors
	switch (PoissonD::errorThreshold)
   	{
  		 case 5: // 5% error
       		 if ((lambda * 3600 * 1000) / timeUnit >= 360000000)
			 {
				 printf("\nPoisson Exception: The request generation rate is too high. The percentage error is more than {%d}%",PoissonD::errorThreshold);
       		 }
			 break; 

    	 case 10: //10% error
            if ((lambda * 3600 * 1000) / timeUnit >= 720000000)
            {
           		printf("\nPoisson Exception: The request generation rate is too high. The percentage error is more than {%d}%",PoissonD::errorThreshold);
        	}
            break; 

    	case 15: //15% error
        	if ((lambda * 3600 * 1000) / timeUnit >= 1080000000)
        	{
            	printf("\nPoisson Exception: The request generation rate is too high. The percentage error is more than {%d}%",PoissonD::errorThreshold);
        	}
        	break;
     }

	  
}

double PoissonD::ln (double x)
{
	return (log10(x)/log10(exp(1.0)));
}

int PoissonD::getNextArrivalTime()
{
	double u = rand()%RAND_MAX;    
	u = u /RAND_MAX;               //u is a random variable between 0 and 1
	double AR = (double) arrRate;  
	AR /= 60;
	AR /= 60;
	AR /= 1000;
	AR /= 1000;   //bringing bach to the granularity of millisecond
	double y = (-1 * (1/AR))* (ln(1-u));
	y = y/1000;
	return (int) y;
}

PoissonD::~PoissonD(void)
{
}
