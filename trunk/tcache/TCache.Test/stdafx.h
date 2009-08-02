// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//

#pragma once

#define DB_USE_DLL

#include "targetver.h"
#include "dll_header.h"

#ifdef DLL_EXPORT
#define DLLDIR __declspec(dllexport)
#else
#define DLLDIR __declspec(dllimport)
#endif

//#define WIN32_LEAN_AND_MEAN		// Exclude rarely-used stuff from Windows headers
#include <stdio.h>
#include <tchar.h>



// TODO: reference additional headers your program requires here
#include <windows.h>


