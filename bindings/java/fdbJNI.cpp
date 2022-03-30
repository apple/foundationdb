/*
 * fdbJNI.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <jni.h>
#include <string.h>
#include <functional>

#include "com_apple_foundationdb_FDB.h"
#include "com_apple_foundationdb_FDBDatabase.h"
#include "com_apple_foundationdb_FDBTransaction.h"
#include "com_apple_foundationdb_FutureInt64.h"
#include "com_apple_foundationdb_FutureKey.h"
#include "com_apple_foundationdb_FutureKeyArray.h"
#include "com_apple_foundationdb_FutureResult.h"
#include "com_apple_foundationdb_FutureResults.h"
#include "com_apple_foundationdb_FutureStrings.h"
#include "com_apple_foundationdb_NativeFuture.h"

#define FDB_API_VERSION 710

#include <foundationdb/fdb_c.h>

#define JNI_NULL nullptr

#if defined(__GNUG__)
#undef JNIEXPORT
#define JNIEXPORT __attribute__((visibility("default")))
#endif

static JavaVM* g_jvm = nullptr;
static thread_local JNIEnv* g_thread_jenv =
    nullptr; // Defined for the network thread once it is running, and for any thread that has called registerCallback
static thread_local jmethodID g_IFutureCallback_call_methodID = JNI_NULL;
static thread_local bool is_external = false;
static jclass range_result_summary_class;
static jclass range_result_class;
static jclass mapped_range_result_class;
static jclass mapped_key_value_class;
static jclass string_class;
static jclass key_array_result_class;
static jmethodID key_array_result_init;
static jmethodID range_result_init;
static jmethodID mapped_range_result_init;
static jmethodID mapped_key_value_from_bytes;
static jmethodID range_result_summary_init;

void detachIfExternalThread(void* ignore) {
	if (is_external && g_thread_jenv != nullptr) {
		g_thread_jenv = nullptr;
		g_IFutureCallback_call_methodID = JNI_NULL;
		g_jvm->DetachCurrentThread();
	}
}

void throwOutOfMem(JNIEnv* jenv) {
	const char* className = "java/lang/OutOfMemoryError";
	jclass illegalArgClass = jenv->FindClass(className);

	if (jenv->ExceptionOccurred())
		return;

	if (jenv->ThrowNew(illegalArgClass, nullptr) != 0) {
		if (!jenv->ExceptionOccurred()) {
			jenv->FatalError("Could not throw OutOfMemoryError");
		} else {
			// This means that an exception is pending. We do not know what it is, but are sure
			//  that control flow will include throwing that exception into the calling Java code.
		}
	}
}

static jthrowable getThrowable(JNIEnv* jenv, fdb_error_t e, const char* msg = nullptr) {
	jclass excepClass = jenv->FindClass("com/apple/foundationdb/FDBException");
	if (jenv->ExceptionOccurred())
		return JNI_NULL;

	jmethodID excepCtor = jenv->GetMethodID(excepClass, "<init>", "(Ljava/lang/String;I)V");
	if (jenv->ExceptionOccurred())
		return JNI_NULL;

	const char* fdb_message = msg ? msg : fdb_get_error(e);
	jstring m = jenv->NewStringUTF(fdb_message);
	if (jenv->ExceptionOccurred())
		return JNI_NULL;

	jthrowable t = (jthrowable)jenv->NewObject(excepClass, excepCtor, m, e);
	if (jenv->ExceptionOccurred())
		return JNI_NULL;

	return t;
}

void throwNamedException(JNIEnv* jenv, const char* class_full_name, const char* message) {
	jclass exceptionClass = jenv->FindClass(class_full_name);
	if (jenv->ExceptionOccurred())
		return;

	if (jenv->ThrowNew(exceptionClass, message) != 0) {
		if (jenv->ExceptionOccurred())
			return;
		jenv->FatalError("FDB: Error throwing exception");
	}
}

void throwRuntimeEx(JNIEnv* jenv, const char* message) {
	throwNamedException(jenv, "java/lang/RuntimeException", message);
}

void throwParamNotNull(JNIEnv* jenv) {
	throwNamedException(jenv, "java/lang/IllegalArgumentException", "Argument cannot be null");
}

#ifdef __cplusplus
extern "C" {
#endif

// If the methods are not found, exceptions are thrown and this will return false.
//  Returns TRUE on success, false otherwise.
static bool findCallbackMethods(JNIEnv* jenv) {
	jclass cls = jenv->FindClass("java/lang/Runnable");
	if (jenv->ExceptionOccurred())
		return false;

	g_IFutureCallback_call_methodID = jenv->GetMethodID(cls, "run", "()V");
	if (jenv->ExceptionOccurred())
		return false;

	return true;
}

static void callCallback(FDBFuture* f, void* data) {
	if (g_thread_jenv == nullptr) {
		// We are on an external thread and must attach to the JVM.
		// The shutdown hook will later detach this thread.
		is_external = true;
		if (g_jvm != nullptr && g_jvm->AttachCurrentThreadAsDaemon((void**)&g_thread_jenv, nullptr) == JNI_OK) {
			if (!findCallbackMethods(g_thread_jenv)) {
				g_thread_jenv->FatalError("FDB: Could not find callback method.\n");
			}
		} else {
			// Can't call FatalError, because we don't have a pointer to the jenv...
			// There will be a segmentation fault from the attempt to call the callback method.
			fprintf(stderr, "FDB: Could not attach external client thread to the JVM as daemon.\n");
		}
	}

	jobject callback = (jobject)data;
	g_thread_jenv->CallVoidMethod(callback, g_IFutureCallback_call_methodID);
	g_thread_jenv->DeleteGlobalRef(callback);
}

// Attempts to throw 't', attempts to shut down the JVM if this fails.
void safeThrow(JNIEnv* jenv, jthrowable t) {
	if (jenv->Throw(t) != 0) {
		jenv->FatalError("FDB: Unable to throw exception");
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_NativeFuture_Future_1registerCallback(JNIEnv* jenv,
                                                                                         jobject cls,
                                                                                         jlong future,
                                                                                         jobject callback) {
	// SOMEDAY: Do this on module load instead. Can we cache method ids across threads?
	if (!g_IFutureCallback_call_methodID) {
		if (!findCallbackMethods(jenv)) {
			return;
		}
	}

	if (!future || !callback) {
		throwParamNotNull(jenv);
		return;
	}
	FDBFuture* f = (FDBFuture*)future;

	// This is documented as not throwing, but simply returning null on OOM.
	//  As belt and suspenders, we will check for pending exceptions and then,
	//  if there are none and the result is null, we'll throw our own OOM.
	callback = jenv->NewGlobalRef(callback);
	if (!callback) {
		if (!jenv->ExceptionOccurred())
			throwOutOfMem(jenv);
		return;
	}

	// Here we cache a thread-local reference to jenv
	g_thread_jenv = jenv;
	fdb_error_t err = fdb_future_set_callback(f, &callCallback, callback);
	if (err) {
		jenv->DeleteGlobalRef(callback);
		safeThrow(jenv, getThrowable(jenv, err));
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_NativeFuture_Future_1blockUntilReady(JNIEnv* jenv,
                                                                                        jobject,
                                                                                        jlong future) {
	if (!future) {
		throwParamNotNull(jenv);
		return;
	}
	FDBFuture* sav = (FDBFuture*)future;

	fdb_error_t err = fdb_future_block_until_ready(sav);
	if (err)
		safeThrow(jenv, getThrowable(jenv, err));
}

JNIEXPORT jthrowable JNICALL Java_com_apple_foundationdb_NativeFuture_Future_1getError(JNIEnv* jenv,
                                                                                       jobject,
                                                                                       jlong future) {
	if (!future) {
		throwParamNotNull(jenv);
		return JNI_NULL;
	}
	FDBFuture* sav = (FDBFuture*)future;
	fdb_error_t err = fdb_future_get_error(sav);
	if (err)
		return getThrowable(jenv, err);
	else
		return JNI_NULL;
}

JNIEXPORT jboolean JNICALL Java_com_apple_foundationdb_NativeFuture_Future_1isReady(JNIEnv* jenv,
                                                                                    jobject,
                                                                                    jlong future) {
	if (!future) {
		throwParamNotNull(jenv);
		return JNI_FALSE;
	}
	FDBFuture* var = (FDBFuture*)future;
	return (jboolean)fdb_future_is_ready(var);
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_NativeFuture_Future_1dispose(JNIEnv* jenv, jobject, jlong future) {
	if (!future) {
		throwParamNotNull(jenv);
		return;
	}
	FDBFuture* var = (FDBFuture*)future;
	fdb_future_destroy(var);
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_NativeFuture_Future_1cancel(JNIEnv* jenv, jobject, jlong future) {
	if (!future) {
		throwParamNotNull(jenv);
		return;
	}
	FDBFuture* var = (FDBFuture*)future;
	fdb_future_cancel(var);
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_NativeFuture_Future_1releaseMemory(JNIEnv* jenv,
                                                                                      jobject,
                                                                                      jlong future) {
	if (!future) {
		throwParamNotNull(jenv);
		return;
	}
	FDBFuture* var = (FDBFuture*)future;
	fdb_future_release_memory(var);
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FutureInt64_FutureInt64_1get(JNIEnv* jenv, jobject, jlong future) {
	if (!future) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBFuture* f = (FDBFuture*)future;

	int64_t value = 0;
	fdb_error_t err = fdb_future_get_int64(f, &value);
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
		return 0;
	}

	return (jlong)value;
}

JNIEXPORT jobjectArray JNICALL Java_com_apple_foundationdb_FutureStrings_FutureStrings_1get(JNIEnv* jenv,
                                                                                            jobject,
                                                                                            jlong future) {
	if (!future) {
		throwParamNotNull(jenv);
		return JNI_NULL;
	}
	FDBFuture* f = (FDBFuture*)future;

	const char** strings;
	int count;
	fdb_error_t err = fdb_future_get_string_array(f, &strings, &count);
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
		return JNI_NULL;
	}

	if (jenv->ExceptionOccurred())
		return JNI_NULL;
	jobjectArray arr = jenv->NewObjectArray(count, string_class, JNI_NULL);
	if (!arr) {
		if (!jenv->ExceptionOccurred())
			throwOutOfMem(jenv);
		return JNI_NULL;
	}

	for (int i = 0; i < count; i++) {
		jstring str = jenv->NewStringUTF(strings[i]);
		if (!str) {
			if (!jenv->ExceptionOccurred())
				throwOutOfMem(jenv);
			return JNI_NULL;
		}

		jenv->SetObjectArrayElement(arr, i, str);
		if (jenv->ExceptionOccurred())
			return JNI_NULL;
	}

	return arr;
}

JNIEXPORT jobject JNICALL Java_com_apple_foundationdb_FutureKeyArray_FutureKeyArray_1get(JNIEnv* jenv,
                                                                                         jobject,
                                                                                         jlong future) {
	if (!future) {
		throwParamNotNull(jenv);
		return JNI_NULL;
	}

	FDBFuture* f = (FDBFuture*)future;

	const FDBKey* ks;
	int count;
	fdb_error_t err = fdb_future_get_key_array(f, &ks, &count);
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
		return JNI_NULL;
	}

	int totalKeySize = 0;
	for (int i = 0; i < count; i++) {
		totalKeySize += ks[i].key_length;
	}

	jbyteArray keyArray = jenv->NewByteArray(totalKeySize);
	if (!keyArray) {
		if (!jenv->ExceptionOccurred())
			throwOutOfMem(jenv);
		return JNI_NULL;
	}
	uint8_t* keys_barr = (uint8_t*)jenv->GetByteArrayElements(keyArray, JNI_NULL);
	if (!keys_barr) {
		throwRuntimeEx(jenv, "Error getting handle to native resources");
		return JNI_NULL;
	}

	jintArray lengthArray = jenv->NewIntArray(count);
	if (!lengthArray) {
		if (!jenv->ExceptionOccurred())
			throwOutOfMem(jenv);

		jenv->ReleaseByteArrayElements(keyArray, (jbyte*)keys_barr, 0);
		return JNI_NULL;
	}

	jint* length_barr = jenv->GetIntArrayElements(lengthArray, JNI_NULL);
	if (!length_barr) {
		if (!jenv->ExceptionOccurred())
			throwOutOfMem(jenv);

		jenv->ReleaseByteArrayElements(keyArray, (jbyte*)keys_barr, 0);
		return JNI_NULL;
	}

	int offset = 0;
	for (int i = 0; i < count; i++) {
		memcpy(keys_barr + offset, ks[i].key, ks[i].key_length);
		length_barr[i] = ks[i].key_length;
		offset += ks[i].key_length;
	}

	jenv->ReleaseByteArrayElements(keyArray, (jbyte*)keys_barr, 0);
	jenv->ReleaseIntArrayElements(lengthArray, length_barr, 0);

	jobject result = jenv->NewObject(key_array_result_class, key_array_result_init, keyArray, lengthArray);
	if (jenv->ExceptionOccurred())
		return JNI_NULL;

	return result;
}

// SOMEDAY: explore doing this more efficiently with Direct ByteBuffers
JNIEXPORT jobject JNICALL Java_com_apple_foundationdb_FutureResults_FutureResults_1get(JNIEnv* jenv,
                                                                                       jobject,
                                                                                       jlong future) {
	if (!future) {
		throwParamNotNull(jenv);
		return JNI_NULL;
	}

	FDBFuture* f = (FDBFuture*)future;

	const FDBKeyValue* kvs;
	int count;
	fdb_bool_t more;
	fdb_error_t err = fdb_future_get_keyvalue_array(f, &kvs, &count, &more);
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
		return JNI_NULL;
	}

	int totalKeyValueSize = 0;
	for (int i = 0; i < count; i++) {
		totalKeyValueSize += kvs[i].key_length + kvs[i].value_length;
	}

	jbyteArray keyValueArray = jenv->NewByteArray(totalKeyValueSize);
	if (!keyValueArray) {
		if (!jenv->ExceptionOccurred())
			throwOutOfMem(jenv);
		return JNI_NULL;
	}
	uint8_t* keyvalues_barr = (uint8_t*)jenv->GetByteArrayElements(keyValueArray, JNI_NULL);
	if (!keyvalues_barr) {
		throwRuntimeEx(jenv, "Error getting handle to native resources");
		return JNI_NULL;
	}

	jintArray lengthArray = jenv->NewIntArray(count * 2);
	if (!lengthArray) {
		if (!jenv->ExceptionOccurred())
			throwOutOfMem(jenv);

		jenv->ReleaseByteArrayElements(keyValueArray, (jbyte*)keyvalues_barr, 0);
		return JNI_NULL;
	}

	jint* length_barr = jenv->GetIntArrayElements(lengthArray, JNI_NULL);
	if (!length_barr) {
		if (!jenv->ExceptionOccurred())
			throwOutOfMem(jenv);

		jenv->ReleaseByteArrayElements(keyValueArray, (jbyte*)keyvalues_barr, 0);
		return JNI_NULL;
	}

	int offset = 0;
	for (int i = 0; i < count; i++) {
		memcpy(keyvalues_barr + offset, kvs[i].key, kvs[i].key_length);
		length_barr[i * 2] = kvs[i].key_length;
		offset += kvs[i].key_length;

		memcpy(keyvalues_barr + offset, kvs[i].value, kvs[i].value_length);
		length_barr[(i * 2) + 1] = kvs[i].value_length;
		offset += kvs[i].value_length;
	}

	jenv->ReleaseByteArrayElements(keyValueArray, (jbyte*)keyvalues_barr, 0);
	jenv->ReleaseIntArrayElements(lengthArray, length_barr, 0);

	jobject result = jenv->NewObject(range_result_class, range_result_init, keyValueArray, lengthArray, (jboolean)more);
	if (jenv->ExceptionOccurred())
		return JNI_NULL;

	return result;
}

class ExecuteOnLeave {
	std::function<void()> func;

public:
	explicit ExecuteOnLeave(std::function<void()> func) : func(func) {}
	~ExecuteOnLeave() { func(); }
};

void cpBytesAndLengthInner(uint8_t*& pByte, jint*& pLength, const uint8_t* data, const int& length) {
	*pLength = length;
	pLength++;

	memcpy(pByte, data, length);
	pByte += length;
}

void cpBytesAndLength(uint8_t*& pByte, jint*& pLength, const FDBKey& key) {
	cpBytesAndLengthInner(pByte, pLength, key.key, key.key_length);
}

JNIEXPORT jobject JNICALL Java_com_apple_foundationdb_FutureMappedResults_FutureMappedResults_1get(JNIEnv* jenv,
                                                                                                   jobject,
                                                                                                   jlong future) {
	if (!future) {
		throwParamNotNull(jenv);
		return JNI_NULL;
	}

	FDBFuture* f = (FDBFuture*)future;

	const FDBMappedKeyValue* kvms;
	int count;
	fdb_bool_t more;
	fdb_error_t err = fdb_future_get_mappedkeyvalue_array(f, &kvms, &count, &more);
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
		return JNI_NULL;
	}

	jobjectArray mrr_values = jenv->NewObjectArray(count, mapped_key_value_class, NULL);
	if (!mrr_values) {
		if (!jenv->ExceptionOccurred())
			throwOutOfMem(jenv);
		return JNI_NULL;
	}

	for (int i = 0; i < count; i++) {
		FDBMappedKeyValue kvm = kvms[i];
		int kvm_count = kvm.getRange.m_size;

		const int totalLengths = 4 + kvm_count * 2;

		int totalBytes = kvm.key.key_length + kvm.value.key_length + kvm.getRange.begin.key.key_length +
		                 kvm.getRange.end.key.key_length;
		for (int i = 0; i < kvm_count; i++) {
			auto kv = kvm.getRange.data[i];
			totalBytes += kv.key_length + kv.value_length;
		}

		jbyteArray bytesArray = jenv->NewByteArray(totalBytes);
		if (!bytesArray) {
			if (!jenv->ExceptionOccurred())
				throwOutOfMem(jenv);
			return JNI_NULL;
		}

		jintArray lengthArray = jenv->NewIntArray(totalLengths);
		if (!lengthArray) {
			if (!jenv->ExceptionOccurred())
				throwOutOfMem(jenv);
			return JNI_NULL;
		}

		uint8_t* bytes_barr = (uint8_t*)jenv->GetByteArrayElements(bytesArray, JNI_NULL);
		if (!bytes_barr) {
			throwRuntimeEx(jenv, "Error getting handle to native resources");
			return JNI_NULL;
		}
		{
			ExecuteOnLeave e([&]() { jenv->ReleaseByteArrayElements(bytesArray, (jbyte*)bytes_barr, 0); });

			jint* length_barr = jenv->GetIntArrayElements(lengthArray, JNI_NULL);
			if (!length_barr) {
				if (!jenv->ExceptionOccurred())
					throwOutOfMem(jenv);
				return JNI_NULL;
			}
			{
				ExecuteOnLeave e([&]() { jenv->ReleaseIntArrayElements(lengthArray, length_barr, 0); });

				uint8_t* pByte = bytes_barr;
				jint* pLength = length_barr;

				cpBytesAndLength(pByte, pLength, kvm.key);
				cpBytesAndLength(pByte, pLength, kvm.value);
				cpBytesAndLength(pByte, pLength, kvm.getRange.begin.key);
				cpBytesAndLength(pByte, pLength, kvm.getRange.end.key);
				for (int kvm_i = 0; kvm_i < kvm_count; kvm_i++) {
					auto kv = kvm.getRange.data[kvm_i];
					cpBytesAndLengthInner(pByte, pLength, kv.key, kv.key_length);
					cpBytesAndLengthInner(pByte, pLength, kv.value, kv.value_length);
				}
			}
		}
		// After native arrays are released
		jobject mkv = jenv->CallStaticObjectMethod(
		    mapped_key_value_class, mapped_key_value_from_bytes, (jbyteArray)bytesArray, (jintArray)lengthArray);
		if (jenv->ExceptionOccurred())
			return JNI_NULL;
		jenv->SetObjectArrayElement(mrr_values, i, mkv);
		if (jenv->ExceptionOccurred())
			return JNI_NULL;
	}

	jobject mrr = jenv->NewObject(mapped_range_result_class, mapped_range_result_init, mrr_values, (jboolean)more);
	if (jenv->ExceptionOccurred())
		return JNI_NULL;

	return mrr;
}

// SOMEDAY: explore doing this more efficiently with Direct ByteBuffers
JNIEXPORT jbyteArray JNICALL Java_com_apple_foundationdb_FutureResult_FutureResult_1get(JNIEnv* jenv,
                                                                                        jobject,
                                                                                        jlong future) {
	if (!future) {
		throwParamNotNull(jenv);
		return JNI_NULL;
	}
	FDBFuture* f = (FDBFuture*)future;

	fdb_bool_t present;
	const uint8_t* value;
	int length;
	fdb_error_t err = fdb_future_get_value(f, &present, &value, &length);
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
		return JNI_NULL;
	}

	if (!present)
		return JNI_NULL;

	jbyteArray result = jenv->NewByteArray(length);
	if (!result) {
		if (!jenv->ExceptionOccurred())
			throwOutOfMem(jenv);
		return JNI_NULL;
	}

	jenv->SetByteArrayRegion(result, 0, length, (const jbyte*)value);
	return result;
}

JNIEXPORT jbyteArray JNICALL Java_com_apple_foundationdb_FutureKey_FutureKey_1get(JNIEnv* jenv, jobject, jlong future) {
	if (!future) {
		throwParamNotNull(jenv);
		return JNI_NULL;
	}
	FDBFuture* f = (FDBFuture*)future;

	const uint8_t* value;
	int length;
	fdb_error_t err = fdb_future_get_key(f, &value, &length);
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
		return JNI_NULL;
	}

	jbyteArray result = jenv->NewByteArray(length);
	if (!result) {
		if (!jenv->ExceptionOccurred())
			throwOutOfMem(jenv);
		return JNI_NULL;
	}

	jenv->SetByteArrayRegion(result, 0, length, (const jbyte*)value);
	return result;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBDatabase_Database_1createTransaction(JNIEnv* jenv,
                                                                                            jobject,
                                                                                            jlong dbPtr) {
	if (!dbPtr) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBDatabase* database = (FDBDatabase*)dbPtr;
	FDBTransaction* tr;
	fdb_error_t err = fdb_database_create_transaction(database, &tr);
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
		return 0;
	}
	return (jlong)tr;
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBDatabase_Database_1dispose(JNIEnv* jenv, jobject, jlong dPtr) {
	if (!dPtr) {
		throwParamNotNull(jenv);
		return;
	}
	fdb_database_destroy((FDBDatabase*)dPtr);
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBDatabase_Database_1setOption(JNIEnv* jenv,
                                                                                   jobject,
                                                                                   jlong dPtr,
                                                                                   jint code,
                                                                                   jbyteArray value) {
	if (!dPtr) {
		throwParamNotNull(jenv);
		return;
	}
	FDBDatabase* c = (FDBDatabase*)dPtr;
	uint8_t* barr = nullptr;
	int size = 0;

	if (value != JNI_NULL) {
		barr = (uint8_t*)jenv->GetByteArrayElements(value, JNI_NULL);
		if (!barr) {
			throwRuntimeEx(jenv, "Error getting handle to native resources");
			return;
		}
		size = jenv->GetArrayLength(value);
	}
	fdb_error_t err = fdb_database_set_option(c, (FDBDatabaseOption)code, barr, size);
	if (value != JNI_NULL)
		jenv->ReleaseByteArrayElements(value, (jbyte*)barr, JNI_ABORT);
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
	}
}

// Get network thread busyness (updated every 1s)
// A value of 0 indicates that the client is more or less idle
// A value of 1 (or more) indicates that the client is saturated
JNIEXPORT jdouble JNICALL Java_com_apple_foundationdb_FDBDatabase_Database_1getMainThreadBusyness(JNIEnv* jenv,
                                                                                                  jobject,
                                                                                                  jlong dbPtr) {
	if (!dbPtr) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBDatabase* database = (FDBDatabase*)dbPtr;
	return (jdouble)fdb_database_get_main_thread_busyness(database);
}

JNIEXPORT jboolean JNICALL Java_com_apple_foundationdb_FDB_Error_1predicate(JNIEnv* jenv,
                                                                            jobject,
                                                                            jint predicate,
                                                                            jint code) {
	return (jboolean)fdb_error_predicate(predicate, code);
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDB_Database_1create(JNIEnv* jenv,
                                                                         jobject,
                                                                         jstring clusterFileName) {
	const char* fileName = nullptr;
	if (clusterFileName != JNI_NULL) {
		fileName = jenv->GetStringUTFChars(clusterFileName, JNI_NULL);
		if (jenv->ExceptionOccurred()) {
			return 0;
		}
	}

	FDBDatabase* db;
	fdb_error_t err = fdb_create_database(fileName, &db);

	if (clusterFileName != JNI_NULL) {
		jenv->ReleaseStringUTFChars(clusterFileName, fileName);
	}

	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
		return 0;
	}

	return (jlong)db;
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1setVersion(JNIEnv* jenv,
                                                                                          jobject,
                                                                                          jlong tPtr,
                                                                                          jlong version) {
	if (!tPtr) {
		throwParamNotNull(jenv);
		return;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;
	fdb_transaction_set_read_version(tr, version);
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1getReadVersion(JNIEnv* jenv,
                                                                                               jobject,
                                                                                               jlong tPtr) {
	if (!tPtr) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;
	FDBFuture* f = fdb_transaction_get_read_version(tr);
	return (jlong)f;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1get(JNIEnv* jenv,
                                                                                    jobject,
                                                                                    jlong tPtr,
                                                                                    jbyteArray keyBytes,
                                                                                    jboolean snapshot) {
	if (!tPtr || !keyBytes) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;

	uint8_t* barr = (uint8_t*)jenv->GetByteArrayElements(keyBytes, JNI_NULL);
	if (!barr) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return 0;
	}

	FDBFuture* f = fdb_transaction_get(tr, barr, jenv->GetArrayLength(keyBytes), (fdb_bool_t)snapshot);
	jenv->ReleaseByteArrayElements(keyBytes, (jbyte*)barr, JNI_ABORT);
	return (jlong)f;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1getKey(JNIEnv* jenv,
                                                                                       jobject,
                                                                                       jlong tPtr,
                                                                                       jbyteArray keyBytes,
                                                                                       jboolean orEqual,
                                                                                       jint offset,
                                                                                       jboolean snapshot) {
	if (!tPtr || !keyBytes) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;

	uint8_t* barr = (uint8_t*)jenv->GetByteArrayElements(keyBytes, JNI_NULL);
	if (!barr) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return 0;
	}

	FDBFuture* f =
	    fdb_transaction_get_key(tr, barr, jenv->GetArrayLength(keyBytes), orEqual, offset, (fdb_bool_t)snapshot);
	jenv->ReleaseByteArrayElements(keyBytes, (jbyte*)barr, JNI_ABORT);
	return (jlong)f;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1getRange(JNIEnv* jenv,
                                                                                         jobject,
                                                                                         jlong tPtr,
                                                                                         jbyteArray keyBeginBytes,
                                                                                         jboolean orEqualBegin,
                                                                                         jint offsetBegin,
                                                                                         jbyteArray keyEndBytes,
                                                                                         jboolean orEqualEnd,
                                                                                         jint offsetEnd,
                                                                                         jint rowLimit,
                                                                                         jint targetBytes,
                                                                                         jint streamingMode,
                                                                                         jint iteration,
                                                                                         jboolean snapshot,
                                                                                         jboolean reverse) {
	if (!tPtr || !keyBeginBytes || !keyEndBytes) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;

	uint8_t* barrBegin = (uint8_t*)jenv->GetByteArrayElements(keyBeginBytes, JNI_NULL);
	if (!barrBegin) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return 0;
	}

	uint8_t* barrEnd = (uint8_t*)jenv->GetByteArrayElements(keyEndBytes, JNI_NULL);
	if (!barrEnd) {
		jenv->ReleaseByteArrayElements(keyBeginBytes, (jbyte*)barrBegin, JNI_ABORT);
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return 0;
	}

	FDBFuture* f = fdb_transaction_get_range(tr,
	                                         barrBegin,
	                                         jenv->GetArrayLength(keyBeginBytes),
	                                         orEqualBegin,
	                                         offsetBegin,
	                                         barrEnd,
	                                         jenv->GetArrayLength(keyEndBytes),
	                                         orEqualEnd,
	                                         offsetEnd,
	                                         rowLimit,
	                                         targetBytes,
	                                         (FDBStreamingMode)streamingMode,
	                                         iteration,
	                                         snapshot,
	                                         reverse);
	jenv->ReleaseByteArrayElements(keyBeginBytes, (jbyte*)barrBegin, JNI_ABORT);
	jenv->ReleaseByteArrayElements(keyEndBytes, (jbyte*)barrEnd, JNI_ABORT);
	return (jlong)f;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1getMappedRange(JNIEnv* jenv,
                                                                                               jobject,
                                                                                               jlong tPtr,
                                                                                               jbyteArray keyBeginBytes,
                                                                                               jboolean orEqualBegin,
                                                                                               jint offsetBegin,
                                                                                               jbyteArray keyEndBytes,
                                                                                               jboolean orEqualEnd,
                                                                                               jint offsetEnd,
                                                                                               jbyteArray mapperBytes,
                                                                                               jint rowLimit,
                                                                                               jint targetBytes,
                                                                                               jint streamingMode,
                                                                                               jint iteration,
                                                                                               jboolean snapshot,
                                                                                               jboolean reverse) {
	if (!tPtr || !keyBeginBytes || !keyEndBytes || !mapperBytes) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;

	uint8_t* barrBegin = (uint8_t*)jenv->GetByteArrayElements(keyBeginBytes, JNI_NULL);
	if (!barrBegin) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return 0;
	}

	uint8_t* barrEnd = (uint8_t*)jenv->GetByteArrayElements(keyEndBytes, JNI_NULL);
	if (!barrEnd) {
		jenv->ReleaseByteArrayElements(keyBeginBytes, (jbyte*)barrBegin, JNI_ABORT);
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return 0;
	}

	uint8_t* barrMapper = (uint8_t*)jenv->GetByteArrayElements(mapperBytes, JNI_NULL);
	if (!barrMapper) {
		jenv->ReleaseByteArrayElements(keyBeginBytes, (jbyte*)barrBegin, JNI_ABORT);
		jenv->ReleaseByteArrayElements(keyEndBytes, (jbyte*)barrEnd, JNI_ABORT);
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return 0;
	}

	FDBFuture* f = fdb_transaction_get_mapped_range(tr,
	                                                barrBegin,
	                                                jenv->GetArrayLength(keyBeginBytes),
	                                                orEqualBegin,
	                                                offsetBegin,
	                                                barrEnd,
	                                                jenv->GetArrayLength(keyEndBytes),
	                                                orEqualEnd,
	                                                offsetEnd,
	                                                barrMapper,
	                                                jenv->GetArrayLength(mapperBytes),
	                                                rowLimit,
	                                                targetBytes,
	                                                (FDBStreamingMode)streamingMode,
	                                                iteration,
	                                                snapshot,
	                                                reverse);
	jenv->ReleaseByteArrayElements(keyBeginBytes, (jbyte*)barrBegin, JNI_ABORT);
	jenv->ReleaseByteArrayElements(keyEndBytes, (jbyte*)barrEnd, JNI_ABORT);
	jenv->ReleaseByteArrayElements(mapperBytes, (jbyte*)barrMapper, JNI_ABORT);
	return (jlong)f;
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FutureResults_FutureResults_1getDirect(JNIEnv* jenv,
                                                                                          jobject,
                                                                                          jlong future,
                                                                                          jobject jbuffer,
                                                                                          jint bufferCapacity) {
	if (!future) {
		throwParamNotNull(jenv);
		return;
	}

	uint8_t* buffer = (uint8_t*)jenv->GetDirectBufferAddress(jbuffer);
	if (!buffer) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return;
	}

	FDBFuture* f = (FDBFuture*)future;
	const FDBKeyValue* kvs;
	int count;
	fdb_bool_t more;
	fdb_error_t err = fdb_future_get_keyvalue_array(f, &kvs, &count, &more);
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
		return;
	}

	// Capacity for Metadata+Keys+Values
	//  => sizeof(jint) for total key/value pairs
	//  => sizeof(jint) to store more flag
	//  => sizeof(jint) to store key length per KV pair
	//  => sizeof(jint) to store value length per KV pair
	int totalCapacityNeeded = 2 * sizeof(jint);
	for (int i = 0; i < count; i++) {
		totalCapacityNeeded += kvs[i].key_length + kvs[i].value_length + 2 * sizeof(jint);
		if (bufferCapacity < totalCapacityNeeded) {
			count = i; /* Only fit first `i` K/V pairs */
			more = true;
			break;
		}
	}

	int offset = 0;

	// First copy RangeResultSummary, i.e. [keyCount, more]
	memcpy(buffer + offset, &count, sizeof(jint));
	offset += sizeof(jint);

	memcpy(buffer + offset, &more, sizeof(jint));
	offset += sizeof(jint);

	for (int i = 0; i < count; i++) {
		memcpy(buffer + offset, &kvs[i].key_length, sizeof(jint));
		memcpy(buffer + offset + sizeof(jint), &kvs[i].value_length, sizeof(jint));
		offset += 2 * sizeof(jint);

		memcpy(buffer + offset, kvs[i].key, kvs[i].key_length);
		offset += kvs[i].key_length;

		memcpy(buffer + offset, kvs[i].value, kvs[i].value_length);
		offset += kvs[i].value_length;
	}
}

void memcpyStringInner(uint8_t* buffer, int& offset, const uint8_t* data, const int& length) {
	memcpy(buffer + offset, &length, sizeof(jint));
	offset += sizeof(jint);
	memcpy(buffer + offset, data, length);
	offset += length;
}

void memcpyString(uint8_t* buffer, int& offset, const FDBKey& key) {
	memcpyStringInner(buffer, offset, key.key, key.key_length);
}

JNIEXPORT void JNICALL
Java_com_apple_foundationdb_FutureMappedResults_FutureMappedResults_1getDirect(JNIEnv* jenv,
                                                                               jobject,
                                                                               jlong future,
                                                                               jobject jbuffer,
                                                                               jint bufferCapacity) {

	if (!future) {
		throwParamNotNull(jenv);
		return;
	}

	uint8_t* buffer = (uint8_t*)jenv->GetDirectBufferAddress(jbuffer);
	if (!buffer) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return;
	}

	FDBFuture* f = (FDBFuture*)future;
	const FDBMappedKeyValue* kvms;
	int count;
	fdb_bool_t more;
	fdb_error_t err = fdb_future_get_mappedkeyvalue_array(f, &kvms, &count, &more);
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
		return;
	}

	int totalCapacityNeeded = 2 * sizeof(jint);
	for (int i = 0; i < count; i++) {
		const FDBMappedKeyValue& kvm = kvms[i];
		totalCapacityNeeded += kvm.key.key_length + kvm.value.key_length + kvm.getRange.begin.key.key_length +
		                       kvm.getRange.end.key.key_length +
		                       5 * sizeof(jint); // Besides the 4 lengths above, also one for kvm_count.
		int kvm_count = kvm.getRange.m_size;
		for (int i = 0; i < kvm_count; i++) {
			auto kv = kvm.getRange.data[i];
			totalCapacityNeeded += kv.key_length + kv.value_length + 2 * sizeof(jint);
		}
		if (bufferCapacity < totalCapacityNeeded) {
			count = i; /* Only fit first `i` K/V pairs */
			more = true;
			break;
		}
	}

	int offset = 0;

	// First copy RangeResultSummary, i.e. [keyCount, more]
	memcpy(buffer + offset, &count, sizeof(jint));
	offset += sizeof(jint);

	memcpy(buffer + offset, &more, sizeof(jint));
	offset += sizeof(jint);

	for (int i = 0; i < count; i++) {
		const FDBMappedKeyValue& kvm = kvms[i];
		memcpyString(buffer, offset, kvm.key);
		memcpyString(buffer, offset, kvm.value);
		memcpyString(buffer, offset, kvm.getRange.begin.key);
		memcpyString(buffer, offset, kvm.getRange.end.key);

		int kvm_count = kvm.getRange.m_size;
		memcpy(buffer + offset, &kvm_count, sizeof(jint));
		offset += sizeof(jint);

		for (int i = 0; i < kvm_count; i++) {
			auto kv = kvm.getRange.data[i];
			memcpyStringInner(buffer, offset, kv.key, kv.key_length);
			memcpyStringInner(buffer, offset, kv.value, kv.value_length);
		}
	}
}

JNIEXPORT jlong JNICALL
Java_com_apple_foundationdb_FDBTransaction_Transaction_1getEstimatedRangeSizeBytes(JNIEnv* jenv,
                                                                                   jobject,
                                                                                   jlong tPtr,
                                                                                   jbyteArray beginKeyBytes,
                                                                                   jbyteArray endKeyBytes) {
	if (!tPtr || !beginKeyBytes || !endKeyBytes) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;

	uint8_t* startKey = (uint8_t*)jenv->GetByteArrayElements(beginKeyBytes, JNI_NULL);
	if (!startKey) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return 0;
	}

	uint8_t* endKey = (uint8_t*)jenv->GetByteArrayElements(endKeyBytes, JNI_NULL);
	if (!endKey) {
		jenv->ReleaseByteArrayElements(beginKeyBytes, (jbyte*)startKey, JNI_ABORT);
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return 0;
	}

	FDBFuture* f = fdb_transaction_get_estimated_range_size_bytes(
	    tr, startKey, jenv->GetArrayLength(beginKeyBytes), endKey, jenv->GetArrayLength(endKeyBytes));
	jenv->ReleaseByteArrayElements(beginKeyBytes, (jbyte*)startKey, JNI_ABORT);
	jenv->ReleaseByteArrayElements(endKeyBytes, (jbyte*)endKey, JNI_ABORT);
	return (jlong)f;
}

JNIEXPORT jlong JNICALL
Java_com_apple_foundationdb_FDBTransaction_Transaction_1getRangeSplitPoints(JNIEnv* jenv,
                                                                            jobject,
                                                                            jlong tPtr,
                                                                            jbyteArray beginKeyBytes,
                                                                            jbyteArray endKeyBytes,
                                                                            jlong chunkSize) {
	if (!tPtr || !beginKeyBytes || !endKeyBytes) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;

	uint8_t* startKey = (uint8_t*)jenv->GetByteArrayElements(beginKeyBytes, JNI_NULL);
	if (!startKey) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return 0;
	}

	uint8_t* endKey = (uint8_t*)jenv->GetByteArrayElements(endKeyBytes, JNI_NULL);
	if (!endKey) {
		jenv->ReleaseByteArrayElements(beginKeyBytes, (jbyte*)startKey, JNI_ABORT);
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return 0;
	}

	FDBFuture* f = fdb_transaction_get_range_split_points(
	    tr, startKey, jenv->GetArrayLength(beginKeyBytes), endKey, jenv->GetArrayLength(endKeyBytes), chunkSize);
	jenv->ReleaseByteArrayElements(beginKeyBytes, (jbyte*)startKey, JNI_ABORT);
	jenv->ReleaseByteArrayElements(endKeyBytes, (jbyte*)endKey, JNI_ABORT);
	return (jlong)f;
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1set(JNIEnv* jenv,
                                                                                   jobject,
                                                                                   jlong tPtr,
                                                                                   jbyteArray keyBytes,
                                                                                   jbyteArray valueBytes) {
	if (!tPtr || !keyBytes || !valueBytes) {
		throwParamNotNull(jenv);
		return;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;

	uint8_t* barrKey = (uint8_t*)jenv->GetByteArrayElements(keyBytes, JNI_NULL);
	if (!barrKey) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return;
	}

	uint8_t* barrValue = (uint8_t*)jenv->GetByteArrayElements(valueBytes, JNI_NULL);
	if (!barrValue) {
		jenv->ReleaseByteArrayElements(keyBytes, (jbyte*)barrKey, JNI_ABORT);
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return;
	}

	fdb_transaction_set(tr, barrKey, jenv->GetArrayLength(keyBytes), barrValue, jenv->GetArrayLength(valueBytes));
	jenv->ReleaseByteArrayElements(keyBytes, (jbyte*)barrKey, JNI_ABORT);
	jenv->ReleaseByteArrayElements(valueBytes, (jbyte*)barrValue, JNI_ABORT);
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1clear__J_3B(JNIEnv* jenv,
                                                                                           jobject,
                                                                                           jlong tPtr,
                                                                                           jbyteArray keyBytes) {
	if (!tPtr || !keyBytes) {
		throwParamNotNull(jenv);
		return;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;

	uint8_t* barr = (uint8_t*)jenv->GetByteArrayElements(keyBytes, JNI_NULL);
	if (!barr) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return;
	}

	fdb_transaction_clear(tr, barr, jenv->GetArrayLength(keyBytes));
	jenv->ReleaseByteArrayElements(keyBytes, (jbyte*)barr, JNI_ABORT);
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1clear__J_3B_3B(JNIEnv* jenv,
                                                                                              jobject,
                                                                                              jlong tPtr,
                                                                                              jbyteArray keyBeginBytes,
                                                                                              jbyteArray keyEndBytes) {
	if (!tPtr || !keyBeginBytes || !keyEndBytes) {
		throwParamNotNull(jenv);
		return;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;

	uint8_t* barrKeyBegin = (uint8_t*)jenv->GetByteArrayElements(keyBeginBytes, JNI_NULL);
	if (!barrKeyBegin) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return;
	}

	uint8_t* barrKeyEnd = (uint8_t*)jenv->GetByteArrayElements(keyEndBytes, JNI_NULL);
	if (!barrKeyEnd) {
		jenv->ReleaseByteArrayElements(keyBeginBytes, (jbyte*)barrKeyBegin, JNI_ABORT);
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return;
	}

	fdb_transaction_clear_range(
	    tr, barrKeyBegin, jenv->GetArrayLength(keyBeginBytes), barrKeyEnd, jenv->GetArrayLength(keyEndBytes));
	jenv->ReleaseByteArrayElements(keyBeginBytes, (jbyte*)barrKeyBegin, JNI_ABORT);
	jenv->ReleaseByteArrayElements(keyEndBytes, (jbyte*)barrKeyEnd, JNI_ABORT);
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1mutate(JNIEnv* jenv,
                                                                                      jobject,
                                                                                      jlong tPtr,
                                                                                      jint code,
                                                                                      jbyteArray key,
                                                                                      jbyteArray value) {
	if (!tPtr || !key || !value) {
		throwParamNotNull(jenv);
		return;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;

	uint8_t* barrKey = (uint8_t*)jenv->GetByteArrayElements(key, JNI_NULL);
	if (!barrKey) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return;
	}

	uint8_t* barrValue = (uint8_t*)jenv->GetByteArrayElements(value, JNI_NULL);
	if (!barrValue) {
		jenv->ReleaseByteArrayElements(key, (jbyte*)barrKey, JNI_ABORT);
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return;
	}

	fdb_transaction_atomic_op(
	    tr, barrKey, jenv->GetArrayLength(key), barrValue, jenv->GetArrayLength(value), (FDBMutationType)code);

	jenv->ReleaseByteArrayElements(key, (jbyte*)barrKey, JNI_ABORT);
	jenv->ReleaseByteArrayElements(value, (jbyte*)barrValue, JNI_ABORT);
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1commit(JNIEnv* jenv,
                                                                                       jobject,
                                                                                       jlong tPtr) {
	if (!tPtr) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;
	FDBFuture* f = fdb_transaction_commit(tr);
	return (jlong)f;
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1setOption(JNIEnv* jenv,
                                                                                         jobject,
                                                                                         jlong tPtr,
                                                                                         jint code,
                                                                                         jbyteArray value) {
	if (!tPtr) {
		throwParamNotNull(jenv);
		return;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;
	uint8_t* barr = nullptr;
	int size = 0;

	if (value != JNI_NULL) {
		barr = (uint8_t*)jenv->GetByteArrayElements(value, JNI_NULL);
		if (!barr) {
			if (!jenv->ExceptionOccurred())
				throwRuntimeEx(jenv, "Error getting handle to native resources");
			return;
		}
		size = jenv->GetArrayLength(value);
	}
	fdb_error_t err = fdb_transaction_set_option(tr, (FDBTransactionOption)code, barr, size);
	if (value != JNI_NULL)
		jenv->ReleaseByteArrayElements(value, (jbyte*)barr, JNI_ABORT);
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
	}
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1getCommittedVersion(JNIEnv* jenv,
                                                                                                    jobject,
                                                                                                    jlong tPtr) {
	if (!tPtr) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;
	int64_t version;
	fdb_error_t err = fdb_transaction_get_committed_version(tr, &version);
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
		return 0;
	}
	return (jlong)version;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1getApproximateSize(JNIEnv* jenv,
                                                                                                   jobject,
                                                                                                   jlong tPtr) {
	if (!tPtr) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBFuture* f = fdb_transaction_get_approximate_size((FDBTransaction*)tPtr);
	return (jlong)f;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1getVersionstamp(JNIEnv* jenv,
                                                                                                jobject,
                                                                                                jlong tPtr) {
	if (!tPtr) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;
	FDBFuture* f = fdb_transaction_get_versionstamp(tr);
	return (jlong)f;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1getKeyLocations(JNIEnv* jenv,
                                                                                                jobject,
                                                                                                jlong tPtr,
                                                                                                jbyteArray key) {
	if (!tPtr || !key) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;

	uint8_t* barr = (uint8_t*)jenv->GetByteArrayElements(key, JNI_NULL);
	if (!barr) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return 0;
	}
	int size = jenv->GetArrayLength(key);

	FDBFuture* f = fdb_transaction_get_addresses_for_key(tr, barr, size);

	jenv->ReleaseByteArrayElements(key, (jbyte*)barr, JNI_ABORT);
	return (jlong)f;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1onError(JNIEnv* jenv,
                                                                                        jobject,
                                                                                        jlong tPtr,
                                                                                        jint errorCode) {
	if (!tPtr) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;
	FDBFuture* f = fdb_transaction_on_error(tr, (fdb_error_t)errorCode);
	return (jlong)f;
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1dispose(JNIEnv* jenv,
                                                                                       jobject,
                                                                                       jlong tPtr) {
	if (!tPtr) {
		throwParamNotNull(jenv);
		return;
	}
	fdb_transaction_destroy((FDBTransaction*)tPtr);
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1reset(JNIEnv* jenv,
                                                                                     jobject,
                                                                                     jlong tPtr) {
	if (!tPtr) {
		throwParamNotNull(jenv);
		return;
	}
	fdb_transaction_reset((FDBTransaction*)tPtr);
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1watch(JNIEnv* jenv,
                                                                                      jobject,
                                                                                      jlong tPtr,
                                                                                      jbyteArray key) {
	if (!tPtr || !key) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;

	uint8_t* barr = (uint8_t*)jenv->GetByteArrayElements(key, JNI_NULL);
	if (!barr) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return 0;
	}
	int size = jenv->GetArrayLength(key);
	FDBFuture* f = fdb_transaction_watch(tr, barr, size);

	jenv->ReleaseByteArrayElements(key, (jbyte*)barr, JNI_ABORT);
	return (jlong)f;
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1cancel(JNIEnv* jenv,
                                                                                      jobject,
                                                                                      jlong tPtr) {
	if (!tPtr) {
		throwParamNotNull(jenv);
		return;
	}
	fdb_transaction_cancel((FDBTransaction*)tPtr);
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1addConflictRange(JNIEnv* jenv,
                                                                                                jobject,
                                                                                                jlong tPtr,
                                                                                                jbyteArray keyBegin,
                                                                                                jbyteArray keyEnd,
                                                                                                jint conflictType) {
	if (!tPtr || !keyBegin || !keyEnd) {
		throwParamNotNull(jenv);
		return;
	}
	FDBTransaction* tr = (FDBTransaction*)tPtr;

	uint8_t* begin_barr = (uint8_t*)jenv->GetByteArrayElements(keyBegin, JNI_NULL);
	if (!begin_barr) {
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return;
	}
	int begin_size = jenv->GetArrayLength(keyBegin);

	uint8_t* end_barr = (uint8_t*)jenv->GetByteArrayElements(keyEnd, JNI_NULL);
	if (!end_barr) {
		jenv->ReleaseByteArrayElements(keyBegin, (jbyte*)begin_barr, JNI_ABORT);
		if (!jenv->ExceptionOccurred())
			throwRuntimeEx(jenv, "Error getting handle to native resources");
		return;
	}
	int end_size = jenv->GetArrayLength(keyEnd);

	fdb_error_t err = fdb_transaction_add_conflict_range(
	    tr, begin_barr, begin_size, end_barr, end_size, (FDBConflictRangeType)conflictType);

	jenv->ReleaseByteArrayElements(keyBegin, (jbyte*)begin_barr, JNI_ABORT);
	jenv->ReleaseByteArrayElements(keyEnd, (jbyte*)end_barr, JNI_ABORT);

	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDB_Select_1API_1version(JNIEnv* jenv, jclass, jint version) {
	fdb_error_t err = fdb_select_api_version((int)version);
	if (err) {
		if (err == 2203) {
			int maxSupportedVersion = fdb_get_max_api_version();

			char errorStr[1024];
			if (FDB_API_VERSION > maxSupportedVersion) {
				snprintf(errorStr,
				         sizeof(errorStr),
				         "This version of the FoundationDB Java binding is not supported by the installed "
				         "FoundationDB C library. The binding requires a library that supports API version "
				         "%d, but the installed library supports a maximum version of %d.",
				         FDB_API_VERSION,
				         maxSupportedVersion);
			} else {
				snprintf(errorStr,
				         sizeof(errorStr),
				         "API version %d is not supported by the installed FoundationDB C library.",
				         version);
			}

			safeThrow(jenv, getThrowable(jenv, err, errorStr));
		} else {
			safeThrow(jenv, getThrowable(jenv, err));
		}
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDB_Network_1setOption(JNIEnv* jenv,
                                                                          jobject,
                                                                          jint code,
                                                                          jbyteArray value) {
	uint8_t* barr = nullptr;
	int size = 0;
	if (value != JNI_NULL) {
		barr = (uint8_t*)jenv->GetByteArrayElements(value, JNI_NULL);
		if (!barr) {
			if (!jenv->ExceptionOccurred())
				throwRuntimeEx(jenv, "Error getting handle to native resources");
			return;
		}
		size = jenv->GetArrayLength(value);
	}
	fdb_error_t err = fdb_network_set_option((FDBNetworkOption)code, barr, size);
	if (value != JNI_NULL)
		jenv->ReleaseByteArrayElements(value, (jbyte*)barr, JNI_ABORT);
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDB_Network_1setup(JNIEnv* jenv, jobject) {
	fdb_error_t err = fdb_setup_network();
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDB_Network_1run(JNIEnv* jenv, jobject) {
	// initialize things for the callbacks on the network thread
	g_thread_jenv = jenv;
	if (!g_IFutureCallback_call_methodID) {
		if (!findCallbackMethods(jenv))
			return;
	}

	fdb_error_t hookErr = fdb_add_network_thread_completion_hook(&detachIfExternalThread, nullptr);
	if (hookErr) {
		safeThrow(jenv, getThrowable(jenv, hookErr));
	}

	fdb_error_t err = fdb_run_network();
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDB_Network_1stop(JNIEnv* jenv, jobject) {
	fdb_error_t err = fdb_stop_network();
	if (err) {
		safeThrow(jenv, getThrowable(jenv, err));
	}
}

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
	JNIEnv* env;
	g_jvm = vm;
	if (vm->GetEnv((void**)&env, JNI_VERSION_1_6) != JNI_OK) {
		return JNI_ERR;
	} else {
		jclass local_range_result_class = env->FindClass("com/apple/foundationdb/RangeResult");
		range_result_init = env->GetMethodID(local_range_result_class, "<init>", "([B[IZ)V");
		range_result_class = (jclass)(env)->NewGlobalRef(local_range_result_class);

		jclass local_mapped_range_result_class = env->FindClass("com/apple/foundationdb/MappedRangeResult");
		mapped_range_result_init =
		    env->GetMethodID(local_mapped_range_result_class, "<init>", "([Lcom/apple/foundationdb/MappedKeyValue;Z)V");
		mapped_range_result_class = (jclass)(env)->NewGlobalRef(local_mapped_range_result_class);

		jclass local_mapped_key_value_class = env->FindClass("com/apple/foundationdb/MappedKeyValue");
		mapped_key_value_from_bytes = env->GetStaticMethodID(
		    local_mapped_key_value_class, "fromBytes", "([B[I)Lcom/apple/foundationdb/MappedKeyValue;");
		mapped_key_value_class = (jclass)(env)->NewGlobalRef(local_mapped_key_value_class);

		jclass local_key_array_result_class = env->FindClass("com/apple/foundationdb/KeyArrayResult");
		key_array_result_init = env->GetMethodID(local_key_array_result_class, "<init>", "([B[I)V");
		key_array_result_class = (jclass)(env)->NewGlobalRef(local_key_array_result_class);

		jclass local_range_result_summary_class = env->FindClass("com/apple/foundationdb/RangeResultSummary");
		range_result_summary_init = env->GetMethodID(local_range_result_summary_class, "<init>", "([BIZ)V");
		range_result_summary_class = (jclass)(env)->NewGlobalRef(local_range_result_summary_class);

		jclass local_string_class = env->FindClass("java/lang/String");
		string_class = (jclass)(env)->NewGlobalRef(local_string_class);

		return JNI_VERSION_1_6;
	}
}

// Is automatically called once the Classloader is destroyed
void JNI_OnUnload(JavaVM* vm, void* reserved) {
	JNIEnv* env;
	if (vm->GetEnv((void**)&env, JNI_VERSION_1_6) != JNI_OK) {
		return;
	} else {
		// delete global references so the GC can collect them
		if (range_result_summary_class != JNI_NULL) {
			env->DeleteGlobalRef(range_result_summary_class);
		}
		if (range_result_class != JNI_NULL) {
			env->DeleteGlobalRef(range_result_class);
		}
		if (mapped_range_result_class != JNI_NULL) {
			env->DeleteGlobalRef(mapped_range_result_class);
		}
		if (mapped_key_value_class != JNI_NULL) {
			env->DeleteGlobalRef(mapped_key_value_class);
		}
		if (string_class != JNI_NULL) {
			env->DeleteGlobalRef(string_class);
		}
	}
}

#ifdef __cplusplus
}
#endif
