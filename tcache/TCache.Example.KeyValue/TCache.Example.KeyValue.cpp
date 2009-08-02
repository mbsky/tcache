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

// TCache.Example.KeyValue.cpp : Defines the entry point for the console application.
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
char create_datazone[][size]={"Superhero","Homosapien"};

// Define the seperator for the semi-strcutred record test
#define SEPERATOR ","

// Should the output be verbose?
#define VERBOSE true
//#define VERBOSE false

void draw_line() {
	printf("\n");
	for(int i=0;i<80;i++)
		printf("-");
	printf("\n\n");
}

// This test 
// a)Inserts key value pairs into the datazones
// b)Retrieves the key value pairs from the datazones
// c)Delete the key value pairs from the datazones
// d)Retrieves the key value pair from the datazones which should fail.  
int keyValueTest(TCache *tc)
{
	if(VERBOSE)
		printf("*~*-~- Performing the key value test -~-*~*\n\n\n");

	const int key_index=0;
	const int value_index=1;
	const int datazone_index=2;
	
	const int num_fields=3;
	const int num_objects=15;

	// Format: 
	// "key", "value" ,"datazone" 
	// i.e The first field is the key, the second one is the value and the third one is the datazone
	char record[num_objects][num_fields][size] ={
		"Superman"		,"It's a bird, It's a plane, It's Superman"						,"Superhero",
		"Spiderman"	,"With great power comes great responsibilty"					,"Superhero",
		"The Thing"	,"It's clobberin' time!"											,"Superhero",
		"Batman"		,"Criminals are a superstitious and cowardly lot"				,"Superhero",
		"Wonderwoman"	,"I am Diana, princess of the Amazons"							,"Superhero",
		"Mr Incredible","The world always manages to get back in jeopardy again"		,"Superhero",
		// Same key value pair in a different datazone, the datazone acts like a namespace
		"Mr Incredible","The world always manages to get back in jeopardy again"		,"Homosapien",
		"Edward Blake" ,"Mother forgive me"												,"Superhero",
		// Same key value pair in a different datazone, the datazone acts like a namespace
		"Edward Blake" ,"Mother forgive me"												,"Homosapien",

		"Prof Shahram","Knows all"														,"Homosapien",
		"Igor"			,"Mentors all"														,"Homosapien",
		"Elham"			,"Evaluates all"													,"Homosapien",
		"Jason"			,"Gets all"														,"Homosapien",
		"Jorge"			,"Terminates all"													,"Homosapien",
		"Showvick"		,"Saves all"														,"Homosapien"
	};
	
	// Vdt is the data structure to store opaque objects
	Vdt vdt_datazone[num_objects],vdt_key[num_objects],vdt_value[num_objects];
	char *get_buffer= new char[size];
	Vdt vdt_get_value(get_buffer,size);

	int ret=0,i;
	
	//Create the vdts
	for(i=0;i<num_objects;i++)
	{
		//Convert the cstring to VDT
		vdt_key[i]= TCUtility::cstringToVdt(record[i][key_index]);
		vdt_value[i]= TCUtility::cstringToVdt(record[i][value_index]);
		vdt_datazone[i]= TCUtility::cstringToVdt(record[i][datazone_index]);
	}

	if(VERBOSE)
		printf("Inserting the key value pairs into the datazones\n\n");

	// Insert the key value pair to the datazones
	for(i=0;i<num_objects;i++)
	{
		//Perform the insert
		ret=tc->Insert(&vdt_datazone[i],vdt_key[i],vdt_value[i]);
		if(ret!=0) {
			printf("Failed to insert record %s\n",db_strerror(ret));
			return -1;
		}
	}

	if(VERBOSE)
		printf("Retrieving the key value pairs from the datazones\n");
	// Get the key value pair to verify whether it is inserted 
	for(i=0;i<num_objects;i++)
	{
		vdt_get_value.set_data(get_buffer);
		vdt_get_value.set_size(size);
		//Perform the get
		ret=tc->Get(&vdt_datazone[i],vdt_key[i],&vdt_get_value);
		if(ret!=0) {
			printf("Failed to get record %s\n",db_strerror(ret));
			return -1;
		}

		// Check if the retrieved value is what we stored
		if(strncmp(get_buffer,record[i][value_index],strlen(record[i][value_index]))!=0)
		{
			printf("The Retrieved value does not match the inserted value \n");
			return -1;
		}
		if(VERBOSE)
			printf("%s %s \n",vdt_key[i].get_data(),get_buffer);

	}

	if(VERBOSE)
		printf("\nDeleting the key value pair from the datazone\n\n");


	// Delete the records 
	for(i=0;i<num_objects;i++)
	{
		//Perform the delete
		ret= tc->Delete(&vdt_datazone[i],vdt_key[i]);
		if(ret!=0) {
			printf("Failed to delete record %s\n",db_strerror(ret));
			return -1;
		}
	}

	// Get the records to verify if it is deleted
	for(i=0;i<num_objects;i++)
	{
		vdt_get_value.set_data(get_buffer);
		vdt_get_value.set_size(size);
		//Perform the get
		ret=tc->Get(&vdt_datazone[i],vdt_key[i],&vdt_get_value);
		if(ret==0) {
			printf("Failed to delete record it still exist in the system\n");
			return -1;
		}
	}
	
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

	ret= keyValueTest(&tc);
	if(ret==0) 
		printf("Key Value example completed successfully\n");
	else 
		printf("Failed the KeyValue example \n");

	if (VERBOSE)
		draw_line();

	system("pause");
	// Shutdown the system
	tc.Shutdown();
	return 0;
}
