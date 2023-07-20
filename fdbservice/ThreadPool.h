/****************************** Module Header ******************************\
* Module Name:  ThreadPool.h
* Project:      CppWindowsService
* Copyright (c) Microsoft Corporation.
*
* The class was designed by Kenny Kerr. It provides the ability to queue
* simple member functions of a class to the Windows thread pool.
*
* Using the thread pool is simple and feels natural in C++.
*
* class FDBService
* {
* public:
*
*     void AsyncRun()
*     {
*         CThreadPool::QueueUserWorkItem(&Service::Run, this);
*     }
*
*     void Run()
*     {
*         // Some lengthy operation
*     }
* };
*
* Kenny Kerr spends most of his time designing and building distributed
* applications for the Microsoft Windows platform. He also has a particular
* passion for C++ and security programming. Reach Kenny at
* http://weblogs.asp.net/kennykerr/ or visit his Web site:
* http://www.kennyandkarin.com/Kenny/.
*
* This source is subject to the Microsoft Public License.
* See http://www.microsoft.com/en-us/openness/resources/licenses.aspx#MPL.
* All other rights reserved.
*
* THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
* EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
\***************************************************************************/

#pragma once

#include <memory>

class CThreadPool {
public:
	template <typename T>
	static void QueueUserWorkItem(void (T::*function)(void), T* object, ULONG flags = WT_EXECUTELONGFUNCTION) {
		typedef std::pair<void (T::*)(), T*> CallbackType;
		auto p = std::make_unique<CallbackType>(function, object);

		if (::QueueUserWorkItem(ThreadProc<T>, p.get(), flags)) {
			// The ThreadProc now has the responsibility of deleting the pair.
			p.release();
		} else {
			throw GetLastError();
		}
	}

private:
	template <typename T>
	static DWORD WINAPI ThreadProc(PVOID context) {
		typedef std::pair<void (T::*)(), T*> CallbackType;

		std::unique_ptr<CallbackType> p(static_cast<CallbackType*>(context));

		(p->second->*p->first)();
		return 0;
	}
};
