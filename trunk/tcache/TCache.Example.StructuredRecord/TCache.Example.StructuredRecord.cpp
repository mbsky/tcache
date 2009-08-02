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

// TCache.Example.StructuredRecord.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"

#include "stdafx.h"
#include <stdio.h>
#include "TCache.h"
#include "ConfigurationLoader.h"
#include "Common.h"
#include <db_cxx.h>


// Define the name and path of the configuration file 
#define CONFIG_FILE "V.xml"

const int size=200;

// Define the number and names of the datazones to be created
#define NUM_DATAZONE 2 
char create_datazone[][size]={"S_Superhero","S_Homosapien"};

// Define the seperator for the semi-strcutred record test
#define SEPERATOR ","

// Should the output be verbose?
#define VERBOSE true
//#define VERBOSE false

struct superhero_structure {
	int num_enemies; //Number of Enemies
	int num_movies; //Number of movies made(as per imdb search with superhero name)
	int year_of_origin; //Year of origin
};

void draw_line() {
	printf("\n");
	for(int i=0;i<80;i++)
		printf("-");
	printf("\n\n");
}


// This test demonstrates the opaque nature of the VDT datastructure. It 
// a)Inserts pairs of key and structured record for the value into the datazones
//	  The structured record consists of Number of Enemies,Number of movies made(as per imdb search with superhero name),Year of origin
// b)Retrieves the key and structured record pair from the datazones

int structuredRecordTest(TCache *tc)
{
	
	if(VERBOSE)
		printf("*~*-~- Performing the structued record test -~-*~*\n\n\n");

	const int num_structure_fields=3;
	const int num_objects=5;

	const int datasize=sizeof(superhero_structure);
	// Format: 
	// "key", = "Superhero name"	
	char record[num_objects][size] ={
		"Superman",
		"Batman",
		"Wonderwoman",
		"Green Lantern",
		"Flash"
	};

	// Foramt:
	// Number of Enemies,Number of movies made(as per imdb search with superhero name),Year of origin
	int superhero_structure_data[num_objects][num_structure_fields]={
		35,	4,	1938,
		34,	9,	1939,
		29,	5,	1941,
		6,	2,	1940,
		9,	1,	1940
	};


	// Vdt is the data structure to store opaque objects
	Vdt vdt_key,vdt_get_value,vdt_insert_value;

	// Initialize the datazone vdt 
	Vdt vdt_datazone= TCUtility::cstringToVdt(create_datazone[0]);

	superhero_structure superhero;

	char *get_buffer= new char[datasize];
	
	int ret=0,i;

	if(VERBOSE)
	{
		printf("Checking the datazones for existing records\n");

		// Check for existing records in the persistent storage
		for(i=0;i<num_objects;i++)
		{
			// Initialize the VDT
			vdt_key= TCUtility::cstringToVdt(record[i]);

			vdt_get_value.set_data(get_buffer);
			vdt_get_value.set_size(datasize);

			//Perform the get
			ret=tc->Get(&vdt_datazone,vdt_key,&vdt_get_value);
			if(ret!=0) {
				printf("The persistent store is empty\n");
				break;
			}

			if(i==0)
				printf("Found\n");
			
			//Type case the retrieved value into superhero structure
			superhero_structure get_superhero= *(superhero_structure *) get_buffer;
			printf("%s %d %d %d \n",vdt_key.get_data(),get_superhero.num_enemies,get_superhero.num_movies,get_superhero.year_of_origin );

		}

	}

	if(VERBOSE)
		printf("\nInserting the records into the datazones\n\n");
	
	//Insert the records merging with the seperator 
	for(i=0;i<num_objects;i++)
	{
		// Initialize the VDT
		vdt_key= TCUtility::cstringToVdt(record[i]);
		
		//initialize the structure as per the data 
		superhero.num_enemies=superhero_structure_data[i][0];
		superhero.num_movies=superhero_structure_data[i][1];
		superhero.year_of_origin=superhero_structure_data[i][2];

		//Since the vdt is opaque, we can directly initialize it with the structure 
		//typecasting it to char * and specifying the size. 
		vdt_insert_value.set_data((char *)&superhero);
		vdt_insert_value.set_size(sizeof(superhero));
		
		//Perform the insert
		ret=tc->Insert(&vdt_datazone,vdt_key,vdt_insert_value);
		if(ret!=0) {
			printf("Failed to insert record %s\n",db_strerror(ret));
			return -1;
		}
	}

	if(VERBOSE)
		printf("Retrieving the records from the datazones\n");

	// Get the key value pair to verify whether it is inserted 
	for(i=0;i<num_objects;i++)
	{
		// Initialize the VDT
		vdt_key= TCUtility::cstringToVdt(record[i]);

		vdt_get_value.set_data(get_buffer);
		vdt_get_value.set_size(datasize);

		//Perform the get
		ret=tc->Get(&vdt_datazone,vdt_key,&vdt_get_value);
		if(ret!=0) {
			printf("Failed to get record %d %s\n",i,db_strerror(ret));
			return -1;
		}

		//Type case the retrieved value into superhero structure
		superhero_structure get_superhero= *(superhero_structure *) get_buffer;

		// Check if the retrieved value is what we stored
		if(get_superhero.num_enemies != superhero_structure_data[i][0])
		{
			printf("The Retrieved number of enemies does not match the inserted value \n");
			return -1;
		}

		if(get_superhero.num_movies != superhero_structure_data[i][1])
		{
			printf("The Retrieved number of movies does not match the inserted value \n");
			return -1;
		}

		if(get_superhero.year_of_origin != superhero_structure_data[i][2])
		{
			printf("The Retrieved year of origin does not match the inserted value \n");
			return -1;
		}

		if(VERBOSE)
			printf("%s %d %d %d \n",vdt_key.get_data(),get_superhero.num_enemies,get_superhero.num_movies,get_superhero.year_of_origin );

	}

	if(VERBOSE)
			printf("\n");
	delete [] get_buffer;
	return 0;
}


int _tmain(int argc, _TCHAR* argv[])
{
	int ret=0,i;

	// Create a TCache Object
	TCache tc;
	
	// Load the configuration parameters from the XML file. 
	ConfigParametersV cp;
	ConfigurationLoader config;
	config.loadVConfigFile(CONFIG_FILE , cp );

	// Initialize the instance of the trojan cache with configuration parameters
	ret=tc.Initialize(cp) ;
	if(ret!=0) {
		printf("Failed to initialize the system. Aborting the test %s\n",db_strerror(ret));
		return -1;
	}

		// Vdt is the data structure to store opaque objects
	Vdt vdt_datazone;
		
	// Create the datazones
	for(i=0;i<NUM_DATAZONE;i++)
	{
		// Initialize the VDT
		vdt_datazone= TCUtility::cstringToVdt(create_datazone[i]);

		// Create the datazone
		ret=tc.Create(&vdt_datazone);
		if(ret!=0 &&ret!=-1) {
			printf("Failed to create datazone %s\n",db_strerror(ret));
			return -1;
		}
	}

	if (VERBOSE)
		draw_line();

	ret= structuredRecordTest(&tc);
	if(ret==0) 
		printf("Structured Record example completed succesfully\n");
	else 
		printf("Failed the Structured Record example \n");

	if (VERBOSE)
		draw_line();

	system("pause");
	// Shutdown the system
	tc.Shutdown();
	return 0;
	
}

