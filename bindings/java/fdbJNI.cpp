/*
 * fdbJNI.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#define FDB_API_VERSION 520

#include <foundationdb/fdb_c.h>

#define JNI_NULL 0

#if defined(__GNUG__)
#define thread_local __thread
// TODO: figure out why the default definition suppresses visibility
#undef JNIEXPORT
#define JNIEXPORT __attribute__ ((visibility ("default")))
#elif defined(_MSC_VER)
#define thread_local __declspec(thread)
#else
#error Missing thread local storage
#endif

static JavaVM* g_jvm = 0; 
static thread_local JNIEnv* g_thread_jenv = 0;  // Defined for the network thread once it is running, and for any thread that has called registerCallback
static thread_local jmethodID g_IFutureCallback_call_methodID = 0;
static thread_local bool is_external = false;

void detachIfExternalThread(void *ignore) {
	if(is_external && g_thread_jenv != 0) {
		g_thread_jenv = 0;
		g_IFutureCallback_call_methodID = 0;
		g_jvm->DetachCurrentThread();
	}
}

void throwOutOfMem(JNIEnv *jenv) {
	const char *className = "java/lang/OutOfMemoryError";
	jclass illegalArgClass = jenv->FindClass( className );

	if(jenv->ExceptionOccurred())
		return;

	if( jenv->ThrowNew( illegalArgClass, NULL ) != 0 ) {
		if( !jenv->ExceptionOccurred() ) {
			jenv->FatalError("Could not throw OutOfMemoryError");
		} else {
			// This means that an exception is pending. We do not know what it is, but are sure
			//  that control flow will include throwing that exception into the calling Java code.
		}
	}
}

static jthrowable getThrowable(JNIEnv *jenv, fdb_error_t e, const char* msg = NULL) {
	jclass excepClass = jenv->FindClass("com/apple/foundationdb/FDBException");
	if(jenv->ExceptionOccurred())
		return JNI_NULL;

	jmethodID excepCtor = jenv->GetMethodID(excepClass, "<init>", "(Ljava/lang/String;I)V");
	if(jenv->ExceptionOccurred())
		return JNI_NULL;

	const char *fdb_message = msg ? msg : fdb_get_error(e);
	jstring m = jenv->NewStringUTF(fdb_message);
	if(jenv->ExceptionOccurred())
		return JNI_NULL;

	jthrowable t = (jthrowable)jenv->NewObject(excepClass, excepCtor, m, e);
	if(jenv->ExceptionOccurred())
		return JNI_NULL;

	return t;
}

void throwNamedException(JNIEnv *jenv, const char *class_full_name, const char* message ) {
	jclass exceptionClass = jenv->FindClass( class_full_name );
	if(jenv->ExceptionOccurred())
		return;

	if( jenv->ThrowNew( exceptionClass, message ) != 0 ) {
		if(jenv->ExceptionOccurred())
			return;
		jenv->FatalError("FDB: Error throwing exception");
	}
}

void throwRuntimeEx(JNIEnv *jenv, const char* message) {
	throwNamedException( jenv, "java/lang/RuntimeException", message );
}

void throwParamNotNull(JNIEnv *jenv) {
	throwNamedException( jenv, "java/lang/IllegalArgumentException", "Argument cannot be null" );
}

#ifdef __cplusplus
extern "C" {
#endif

// If the methods are not found, exceptions are thrown and this will return false.
//  Returns TRUE on success, false otherwise.
static bool findCallbackMethods(JNIEnv *jenv) {
	jclass cls = jenv->FindClass("java/lang/Runnable");
	if(jenv->ExceptionOccurred())
		return false;

	g_IFutureCallback_call_methodID = jenv->GetMethodID(cls, "run", "()V");
	if(jenv->ExceptionOccurred())
		return false;

	return true;
}

static void callCallback( FDBFuture* f, void* data ) {
	if (g_thread_jenv == 0) {
		// We are on an external thread and must attach to the JVM.
		// The shutdown hook will later detach this thread.
		is_external = true;
		if( g_jvm != 0 && g_jvm->AttachCurrentThreadAsDaemon((void **) &g_thread_jenv, JNI_NULL) == JNI_OK ) {
			if( !findCallbackMethods( g_thread_jenv ) ) {
				g_thread_jenv->FatalError("FDB: Could not find callback method.\n");
			}
		} else {
			// Can't call FatalError, because we don't have a pointer to the jenv...
			// There will be a segmentation fault from the attempt to call the callback method.
			fprintf(stderr, "FDB: Could not attach external client thread to the JVM as daemon.\n");
		}
	}

	jobject callback = (jobject)data;
	g_thread_jenv->CallVoidMethod( callback, g_IFutureCallback_call_methodID );
	g_thread_jenv->DeleteGlobalRef(callback);
}

// Attempts to throw 't', attempts to shut down the JVM if this fails.
void safeThrow( JNIEnv *jenv, jthrowable t ) {
	if( jenv->Throw( t ) != 0 ) {
		jenv->FatalError("FDB: Unable to throw exception");
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_NativeFuture_Future_1registerCallback(JNIEnv *jenv, jobject cls, jlong future, jobject callback) {
	// SOMEDAY: Do this on module load instead. Can we cache method ids across threads?
	if( !g_IFutureCallback_call_methodID ) {
		if( !findCallbackMethods( jenv ) ) {
			return;
		}
	}

	if( !future || !callback ) {
		throwParamNotNull(jenv);
		return;
	}
	FDBFuture *f = (FDBFuture *)future;

	// This is documented as not throwing, but simply returning NULL on OMM.
	//  As belt and suspenders, we will check for pending exceptions and then,
	//  if there are none and the result is NULL, we'll throw our own OMM.
	callback = jenv->NewGlobalRef( callback );
	if( !callback ) {
		if( !jenv->ExceptionOccurred() )
			throwOutOfMem(jenv);
		return;
	}

	// Here we cache a thread-local reference to jenv
	g_thread_jenv = jenv;
	fdb_error_t err = fdb_future_set_callback( f, &callCallback, callback );
	if( err ) {
		jenv->DeleteGlobalRef( callback );
		safeThrow( jenv, getThrowable( jenv, err ) );
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_NativeFuture_Future_1blockUntilReady(JNIEnv *jenv, jobject, jlong future) {
	if( !future ) {
		throwParamNotNull(jenv);
		return;
	}
	FDBFuture *sav = (FDBFuture *)future;

	fdb_error_t err = fdb_future_block_until_ready( sav );
	if( err )
		safeThrow( jenv, getThrowable( jenv, err ) );
}

JNIEXPORT jthrowable JNICALL Java_com_apple_foundationdb_NativeFuture_Future_1getError(JNIEnv *jenv, jobject, jlong future) {
	if( !future ) {
		throwParamNotNull(jenv);
		return JNI_NULL;
	}
	FDBFuture *sav = (FDBFuture *)future;
	return getThrowable( jenv, fdb_future_get_error( sav ) );
}

JNIEXPORT jboolean JNICALL Java_com_apple_foundationdb_NativeFuture_Future_1isReady(JNIEnv *jenv, jobject, jlong future) {
	if( !future ) {
		throwParamNotNull(jenv);
		return JNI_FALSE;
	}
	FDBFuture *var = (FDBFuture *)future;
	return (jboolean)fdb_future_is_ready(var);
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_NativeFuture_Future_1dispose(JNIEnv *jenv, jobject, jlong future) {
	if( !future ) {
		throwParamNotNull(jenv);
		return;
	}
	FDBFuture *var = (FDBFuture *)future;
	fdb_future_destroy(var);
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_NativeFuture_Future_1cancel(JNIEnv *jenv, jobject, jlong future) {
	if( !future ) {
		throwParamNotNull(jenv);
		return;
	}
	FDBFuture *var = (FDBFuture *)future;
	fdb_future_cancel(var);
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_NativeFuture_Future_1releaseMemory(JNIEnv *jenv, jobject, jlong future) {
	if( !future ) {
		throwParamNotNull(jenv);
		return;
	}
	FDBFuture *var = (FDBFuture *)future;
	fdb_future_release_memory(var);
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FutureVersion_FutureVersion_1get(JNIEnv *jenv, jobject, jlong future) {
	if( !future ) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBFuture *f = (FDBFuture *)future;

	int64_t version = 0;
	fdb_error_t err = fdb_future_get_version(f, &version);
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
		return 0;
	}

	return (jlong)version;
}

JNIEXPORT jobject JNICALL Java_com_apple_foundationdb_FutureStrings_FutureStrings_1get(JNIEnv *jenv, jobject, jlong future) {
	if( !future ) {
		throwParamNotNull(jenv);
		return JNI_NULL;
	}
	FDBFuture *f = (FDBFuture *)future;

	const char **strings;
	int count;
	fdb_error_t err = fdb_future_get_string_array( f, &strings, &count );
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
		return JNI_NULL;
	}

	jclass str_clazz = jenv->FindClass("java/lang/String");
	if( jenv->ExceptionOccurred() )
		return JNI_NULL;
	jobjectArray arr = jenv->NewObjectArray(count, str_clazz, NULL);
	if( !arr ) {
		if( !jenv->ExceptionOccurred() )
			throwOutOfMem(jenv);
		return JNI_NULL;
	}

	for(int i = 0; i < count; i++) {
		jstring str = jenv->NewStringUTF( strings[i] );
		if( !str ) {
			if( !jenv->ExceptionOccurred() )
				throwOutOfMem(jenv);
			return JNI_NULL;
		}

		jenv->SetObjectArrayElement( arr, i, str );
		if( jenv->ExceptionOccurred() )
			return JNI_NULL;
	}

	return arr;
}

JNIEXPORT jobject JNICALL Java_com_apple_foundationdb_FutureResults_FutureResults_1getSummary(JNIEnv *jenv, jobject, jlong future) {
	if( !future ) {
		throwParamNotNull(jenv);
		return JNI_NULL;
	}

	jclass resultCls = jenv->FindClass("com/apple/foundationdb/RangeResultSummary");
	if( jenv->ExceptionOccurred() )
		return JNI_NULL;
	jmethodID resultCtorId = jenv->GetMethodID(resultCls, "<init>", "([BIZ)V");
	if( jenv->ExceptionOccurred() )
		return JNI_NULL;
 
	FDBFuture *f = (FDBFuture *)future;

	const FDBKeyValue *kvs;
	int count;
	fdb_bool_t more;
	fdb_error_t err = fdb_future_get_keyvalue_array( f, &kvs, &count, &more );
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
		return JNI_NULL;
	}

	jbyteArray lastKey = NULL;
	if(count) {
		lastKey = jenv->NewByteArray(kvs[count - 1].key_length);
		if( !lastKey ) {
			if( !jenv->ExceptionOccurred() )
				throwOutOfMem(jenv);
			return JNI_NULL;
		}

		jenv->SetByteArrayRegion(lastKey, 0, kvs[count - 1].key_length, (jbyte *)kvs[count - 1].key);
	}

	jobject result = jenv->NewObject(resultCls, resultCtorId, lastKey, count, (jboolean)more);
	if( jenv->ExceptionOccurred() )
		return JNI_NULL;

	return result;
}

// SOMEDAY: explore doing this more efficiently with Direct ByteBuffers
JNIEXPORT jobject JNICALL Java_com_apple_foundationdb_FutureResults_FutureResults_1get(JNIEnv *jenv, jobject, jlong future) {
	if( !future ) {
		throwParamNotNull(jenv);
		return JNI_NULL;
	}

	jclass resultCls = jenv->FindClass("com/apple/foundationdb/RangeResult");
	jmethodID resultCtorId = jenv->GetMethodID(resultCls, "<init>", "([B[IZ)V");
 
	FDBFuture *f = (FDBFuture *)future;

	const FDBKeyValue *kvs;
	int count;
	fdb_bool_t more;
	fdb_error_t err = fdb_future_get_keyvalue_array( f, &kvs, &count, &more );
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
		return JNI_NULL;
	}

	int totalKeyValueSize = 0;
	for(int i = 0; i < count; i++) {
		totalKeyValueSize += kvs[i].key_length + kvs[i].value_length;
	}

	jbyteArray keyValueArray = jenv->NewByteArray(totalKeyValueSize);
	if( !keyValueArray ) {
		if( !jenv->ExceptionOccurred() )
			throwOutOfMem(jenv);
		return JNI_NULL;
	}
	uint8_t *keyvalues_barr = (uint8_t *)jenv->GetByteArrayElements(keyValueArray, NULL); 
	if (!keyvalues_barr) {
		throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return JNI_NULL;
	}

	jintArray lengthArray = jenv->NewIntArray(count * 2);
	if( !lengthArray ) {
		if( !jenv->ExceptionOccurred() )
			throwOutOfMem(jenv);

		jenv->ReleaseByteArrayElements(keyValueArray, (jbyte *)keyvalues_barr, 0);
		return JNI_NULL;
	}

	jint *length_barr = jenv->GetIntArrayElements(lengthArray, NULL); 
	if( !length_barr ) {
		if( !jenv->ExceptionOccurred() )
			throwOutOfMem(jenv);

		jenv->ReleaseByteArrayElements(keyValueArray, (jbyte *)keyvalues_barr, 0);
		return JNI_NULL;
	}

	int offset = 0;
	for(int i = 0; i < count; i++) {
		memcpy(keyvalues_barr + offset, kvs[i].key, kvs[i].key_length);
		length_barr[ i * 2 ] = kvs[i].key_length;
		offset += kvs[i].key_length;

		memcpy(keyvalues_barr + offset, kvs[i].value, kvs[i].value_length);
		length_barr[ (i * 2) + 1 ] = kvs[i].value_length;
		offset += kvs[i].value_length;
	}

	jenv->ReleaseByteArrayElements(keyValueArray, (jbyte *)keyvalues_barr, 0);
	jenv->ReleaseIntArrayElements(lengthArray, length_barr, 0);

	jobject result = jenv->NewObject(resultCls, resultCtorId, keyValueArray, lengthArray, (jboolean)more);
	if( jenv->ExceptionOccurred() )
		return JNI_NULL;

	return result;
}

// SOMEDAY: explore doing this more efficiently with Direct ByteBuffers
JNIEXPORT jbyteArray JNICALL Java_com_apple_foundationdb_FutureResult_FutureResult_1get(JNIEnv *jenv, jobject, jlong future) {
	if( !future ) {
		throwParamNotNull(jenv);
		return JNI_NULL;
	}
	FDBFuture *f = (FDBFuture *)future;

	fdb_bool_t present;
	const uint8_t *value;
	int length;
	fdb_error_t err = fdb_future_get_value(f, &present, &value, &length);
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
		return JNI_NULL;
	}

	if( !present )
		return JNI_NULL;

	jbyteArray result = jenv->NewByteArray(length);
	if( !result ) {
		if( !jenv->ExceptionOccurred() )
			throwOutOfMem(jenv);
		return JNI_NULL;
	}

	jenv->SetByteArrayRegion(result, 0, length, (const jbyte *)value);
	return result;
}

JNIEXPORT jbyteArray JNICALL Java_com_apple_foundationdb_FutureKey_FutureKey_1get(JNIEnv * jenv, jclass, jlong future) {
	if( !future ) {
		throwParamNotNull(jenv);
		return JNI_NULL;
	}
	FDBFuture *f = (FDBFuture *)future;

	const uint8_t *value;
	int length;
	fdb_error_t err = fdb_future_get_key(f, &value, &length);
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
		return JNI_NULL;
	}

	jbyteArray result = jenv->NewByteArray(length);
	if( !result ) {
		if( !jenv->ExceptionOccurred() )
			throwOutOfMem(jenv);
		return JNI_NULL;
	}

	jenv->SetByteArrayRegion(result, 0, length, (const jbyte *)value);
	return result;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FutureCluster_FutureCluster_1get(JNIEnv *jenv, jobject, jlong future) {
	if( !future ) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBFuture *f = (FDBFuture *)future;

	FDBCluster *cluster;
	fdb_error_t err = fdb_future_get_cluster(f, &cluster);
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
		return 0;
	}
	return (jlong)cluster;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FutureDatabase_FutureDatabase_1get(JNIEnv *jenv, jobject, jlong future) {
	if( !future ) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBFuture *f = (FDBFuture *)future;

	FDBDatabase *database;
	fdb_error_t err = fdb_future_get_database(f, &database);
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
		return 0;
	}
	return (jlong)database;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBDatabase_Database_1createTransaction(JNIEnv *jenv, jobject, jlong dbPtr) {
	if( !dbPtr ) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBDatabase *database = (FDBDatabase *)dbPtr;
	FDBTransaction *tr;
	fdb_error_t err = fdb_database_create_transaction(database, &tr);
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
		return 0;
	}
	return (jlong)tr;
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBDatabase_Database_1dispose(JNIEnv *jenv, jobject, jlong dPtr) {
	if( !dPtr ) {
		throwParamNotNull(jenv);
		return;
	}
	fdb_database_destroy( (FDBDatabase *)dPtr );
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBDatabase_Database_1setOption(JNIEnv *jenv, jobject, jlong dPtr, jint code, jbyteArray value) {
	if( !dPtr ) {
		throwParamNotNull(jenv);
		return;
	}
	FDBDatabase *c = (FDBDatabase *)dPtr;
	uint8_t *barr = NULL;
	int size = 0;

	if(value != 0) {
		barr = (uint8_t *)jenv->GetByteArrayElements( value, NULL );
		if (!barr) {
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
			return;
		}
		size = jenv->GetArrayLength( value );
	}
	fdb_error_t err = fdb_database_set_option( c, (FDBDatabaseOption)code, barr, size );
	if(value != 0)
		jenv->ReleaseByteArrayElements( value, (jbyte *)barr, JNI_ABORT );
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
	}
}

JNIEXPORT jboolean JNICALL Java_com_apple_foundationdb_FDB_Error_1predicate(JNIEnv *jenv, jobject, jint predicate, jint code) {
	return (jboolean)fdb_error_predicate(predicate, code);
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDB_Cluster_1create(JNIEnv *jenv, jobject, jstring clusterFileName) {
	const char* fileName = 0;
	if(clusterFileName != 0) {
		fileName = jenv->GetStringUTFChars(clusterFileName, 0);
		if( jenv->ExceptionOccurred() )
			return 0;
	}
	FDBFuture *cluster = fdb_create_cluster( fileName );
	if(clusterFileName != 0)
		jenv->ReleaseStringUTFChars( clusterFileName, fileName );
	return (jlong)cluster;
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_Cluster_Cluster_1setOption(JNIEnv *jenv, jobject, jlong cPtr, jint code, jbyteArray value) {
	if( !cPtr ) {
		throwParamNotNull(jenv);
		return;
	}
	FDBCluster *c = (FDBCluster *)cPtr;
	uint8_t *barr = NULL;
	int size = 0;

	if(value != 0) {
		barr = (uint8_t *)jenv->GetByteArrayElements( value, NULL );
		if (!barr) {
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
			return;
		}
		size = jenv->GetArrayLength( value );
	}
	fdb_error_t err = fdb_cluster_set_option( c, (FDBClusterOption)code, barr, size );
	if(value != 0)
		jenv->ReleaseByteArrayElements( value, (jbyte *)barr, JNI_ABORT );
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_Cluster_Cluster_1dispose(JNIEnv *jenv, jobject, jlong cPtr) {
	if( !cPtr ) {
		throwParamNotNull(jenv);
		return;
	}
	fdb_cluster_destroy( (FDBCluster *)cPtr );
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_Cluster_Cluster_1createDatabase(JNIEnv *jenv, jobject, jlong cPtr, jbyteArray dbNameBytes) {
	if( !cPtr || !dbNameBytes ) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBCluster *cluster = (FDBCluster *)cPtr;

	uint8_t *barr = (uint8_t *)jenv->GetByteArrayElements( dbNameBytes, NULL );
	if (!barr) {
		throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return 0;
	}

	int size = jenv->GetArrayLength( dbNameBytes );
	FDBFuture * f = fdb_cluster_create_database( cluster, barr, size );
	jenv->ReleaseByteArrayElements( dbNameBytes, (jbyte *)barr, JNI_ABORT );
	return (jlong)f;
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1setVersion(JNIEnv *jenv, jobject, jlong tPtr, jlong version) {
	if( !tPtr ) {
		throwParamNotNull(jenv);
		return;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;
	fdb_transaction_set_read_version( tr, version );
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1getReadVersion(JNIEnv *jenv, jobject, jlong tPtr) {
	if( !tPtr ) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;
	FDBFuture *f = fdb_transaction_get_read_version( tr );
	return (jlong)f;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1get(JNIEnv *jenv, jobject, jlong tPtr, jbyteArray keyBytes, jboolean snapshot) {
	if( !tPtr || !keyBytes ) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;

	uint8_t *barr = (uint8_t *)jenv->GetByteArrayElements( keyBytes, NULL );
	if(!barr) {
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return 0;
	}

	FDBFuture *f = fdb_transaction_get( tr, barr, jenv->GetArrayLength( keyBytes ), (fdb_bool_t)snapshot );
	jenv->ReleaseByteArrayElements( keyBytes, (jbyte *)barr, JNI_ABORT );
	return (jlong)f;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1getKey(JNIEnv *jenv, jobject, jlong tPtr, 
		jbyteArray keyBytes, jboolean orEqual, jint offset, jboolean snapshot) {
	if( !tPtr || !keyBytes ) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;

	uint8_t *barr = (uint8_t *)jenv->GetByteArrayElements( keyBytes, NULL );
	if(!barr) {
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return 0;
	}

	FDBFuture *f = fdb_transaction_get_key( tr, barr, jenv->GetArrayLength( keyBytes ), orEqual, offset, (fdb_bool_t)snapshot );
	jenv->ReleaseByteArrayElements( keyBytes, (jbyte *)barr, JNI_ABORT );
	return (jlong)f;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1getRange
  (JNIEnv *jenv, jobject, jlong tPtr, jbyteArray keyBeginBytes, jboolean orEqualBegin, jint offsetBegin,
		jbyteArray keyEndBytes, jboolean orEqualEnd, jint offsetEnd, jint rowLimit, jint targetBytes, 
		jint streamingMode, jint iteration, jboolean snapshot, jboolean reverse) {
	if( !tPtr || !keyBeginBytes || !keyEndBytes ) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;

	uint8_t *barrBegin = (uint8_t *)jenv->GetByteArrayElements( keyBeginBytes, NULL );
	if (!barrBegin) {
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return 0;
	}

	uint8_t *barrEnd = (uint8_t *)jenv->GetByteArrayElements( keyEndBytes, NULL );
	if (!barrEnd) {
		jenv->ReleaseByteArrayElements( keyBeginBytes, (jbyte *)barrBegin, JNI_ABORT );
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return 0;
	}

	FDBFuture *f = fdb_transaction_get_range( tr, 
			barrBegin, jenv->GetArrayLength( keyBeginBytes ), orEqualBegin, offsetBegin,
			barrEnd, jenv->GetArrayLength( keyEndBytes ), orEqualEnd, offsetEnd, rowLimit,
			targetBytes, (FDBStreamingMode)streamingMode, iteration, snapshot, reverse);
	jenv->ReleaseByteArrayElements( keyBeginBytes, (jbyte *)barrBegin, JNI_ABORT );
	jenv->ReleaseByteArrayElements( keyEndBytes, (jbyte *)barrEnd, JNI_ABORT );
	return (jlong)f;
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1set(JNIEnv *jenv, jobject, jlong tPtr, jbyteArray keyBytes, jbyteArray valueBytes) {
	if( !tPtr || !keyBytes || !valueBytes ) {
		throwParamNotNull(jenv);
		return;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;

	uint8_t *barrKey = (uint8_t *)jenv->GetByteArrayElements( keyBytes, NULL );
	if (!barrKey) {
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return;
	}

	uint8_t *barrValue = (uint8_t *)jenv->GetByteArrayElements( valueBytes, NULL );
	if (!barrValue) {
		jenv->ReleaseByteArrayElements( keyBytes, (jbyte *)barrKey, JNI_ABORT );
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return;
	}

	fdb_transaction_set( tr, 
			barrKey, jenv->GetArrayLength( keyBytes ),
			barrValue, jenv->GetArrayLength( valueBytes ) );
	jenv->ReleaseByteArrayElements( keyBytes, (jbyte *)barrKey, JNI_ABORT );
	jenv->ReleaseByteArrayElements( valueBytes, (jbyte *)barrValue, JNI_ABORT );
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1clear__J_3B(JNIEnv *jenv, jobject, jlong tPtr, jbyteArray keyBytes) {
	if( !tPtr || !keyBytes ) {
		throwParamNotNull(jenv);
		return;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;

	uint8_t *barr = (uint8_t *)jenv->GetByteArrayElements( keyBytes, NULL );
	if (!barr) {
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return;
	}

	fdb_transaction_clear( tr, barr, jenv->GetArrayLength( keyBytes ) );
	jenv->ReleaseByteArrayElements( keyBytes, (jbyte *)barr, JNI_ABORT );
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1clear__J_3B_3B(JNIEnv *jenv, jobject, jlong tPtr, jbyteArray keyBeginBytes, jbyteArray keyEndBytes) {
	if( !tPtr || !keyBeginBytes || !keyEndBytes ) {
		throwParamNotNull(jenv);
		return;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;

	uint8_t *barrKeyBegin = (uint8_t *)jenv->GetByteArrayElements( keyBeginBytes, NULL );
	if (!barrKeyBegin) {
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return;
	}

	uint8_t *barrKeyEnd = (uint8_t *)jenv->GetByteArrayElements( keyEndBytes, NULL );
	if (!barrKeyEnd) {
		jenv->ReleaseByteArrayElements( keyBeginBytes, (jbyte *)barrKeyBegin, JNI_ABORT );
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return;
	}

	fdb_transaction_clear_range( tr, 
			barrKeyBegin, jenv->GetArrayLength( keyBeginBytes ),
			barrKeyEnd, jenv->GetArrayLength( keyEndBytes ) );
	jenv->ReleaseByteArrayElements( keyBeginBytes, (jbyte *)barrKeyBegin, JNI_ABORT );
	jenv->ReleaseByteArrayElements( keyEndBytes, (jbyte *)barrKeyEnd, JNI_ABORT );
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1mutate(JNIEnv *jenv, jobject, jlong tPtr, jint code,
																		jbyteArray key, jbyteArray value ) {
	if( !tPtr || !key || !value ) {
		throwParamNotNull(jenv);
		return;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;

	uint8_t *barrKey = (uint8_t *)jenv->GetByteArrayElements( key, NULL );
	if (!barrKey) {
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return;
	}

	uint8_t *barrValue = (uint8_t *)jenv->GetByteArrayElements( value, NULL );
	if (!barrValue) {
		jenv->ReleaseByteArrayElements( key, (jbyte *)barrKey, JNI_ABORT );
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return;
	}

	fdb_transaction_atomic_op( tr, 
			barrKey, jenv->GetArrayLength( key ),
			barrValue, jenv->GetArrayLength( value ),
			(FDBMutationType)code);

	jenv->ReleaseByteArrayElements( key, (jbyte *)barrKey, JNI_ABORT );
	jenv->ReleaseByteArrayElements( value, (jbyte *)barrValue, JNI_ABORT );
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1commit(JNIEnv *jenv, jobject, jlong tPtr) {
	if( !tPtr ) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;
	FDBFuture *f = fdb_transaction_commit( tr );
	return (jlong)f;
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1setOption(JNIEnv *jenv, jobject, jlong tPtr, jint code, jbyteArray value) {
	if( !tPtr ) {
		throwParamNotNull(jenv);
		return;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;
	uint8_t *barr = NULL;
	int size = 0;

	if(value != 0) {
		barr = (uint8_t *)jenv->GetByteArrayElements( value, NULL );
		if (!barr) {
			if( !jenv->ExceptionOccurred() )
				throwRuntimeEx( jenv, "Error getting handle to native resources" );
			return;
		}
		size = jenv->GetArrayLength( value );
	}
	fdb_error_t err = fdb_transaction_set_option( tr, (FDBTransactionOption)code, barr, size );
	if(value != 0)
		jenv->ReleaseByteArrayElements( value, (jbyte *)barr, JNI_ABORT );
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
	}
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1getCommittedVersion(JNIEnv *jenv, jobject, jlong tPtr) {
	if( !tPtr ) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;
	int64_t version;
	fdb_error_t err = fdb_transaction_get_committed_version( tr, &version );
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
		return 0;
	}
	return (jlong)version;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1getVersionstamp(JNIEnv *jenv, jobject, jlong tPtr) {
	if (!tPtr) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;
	FDBFuture *f = fdb_transaction_get_versionstamp(tr);
	return (jlong)f;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1getKeyLocations(JNIEnv *jenv, jobject, jlong tPtr, jbyteArray key) {
	if( !tPtr || !key ) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;

	uint8_t *barr = (uint8_t *)jenv->GetByteArrayElements( key, NULL );
	if (!barr) {
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return 0;
	}
	int size = jenv->GetArrayLength( key );

	FDBFuture *f = fdb_transaction_get_addresses_for_key( tr, barr, size );

	jenv->ReleaseByteArrayElements( key, (jbyte *)barr, JNI_ABORT );
	return (jlong)f;
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1onError(JNIEnv *jenv, jobject, jlong tPtr, jint errorCode) {
	if( !tPtr ) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;
	FDBFuture *f = fdb_transaction_on_error( tr, (fdb_error_t)errorCode );
	return (jlong)f;
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1dispose(JNIEnv *jenv, jobject, jlong tPtr) {
	if( !tPtr ) {
		throwParamNotNull(jenv);
		return;
	}
	fdb_transaction_destroy( (FDBTransaction *)tPtr );
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1reset(JNIEnv *jenv, jobject, jlong tPtr) {
	if( !tPtr ) {
		throwParamNotNull(jenv);
		return;
	}
	fdb_transaction_reset( (FDBTransaction *)tPtr );
}

JNIEXPORT jlong JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1watch(JNIEnv *jenv, jobject, jlong tPtr, jbyteArray key) {
	if( !tPtr || !key ) {
		throwParamNotNull(jenv);
		return 0;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;

	uint8_t *barr = (uint8_t *)jenv->GetByteArrayElements( key, NULL );
	if (!barr) {
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return 0;
	}
	int size = jenv->GetArrayLength( key );
	FDBFuture *f = fdb_transaction_watch( tr, barr, size );

	jenv->ReleaseByteArrayElements( key, (jbyte *)barr, JNI_ABORT );
	return (jlong)f;
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1cancel(JNIEnv *jenv, jobject, jlong tPtr) {
	if( !tPtr ) {
		throwParamNotNull(jenv);
		return;
	}
	fdb_transaction_cancel( (FDBTransaction *)tPtr );
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDBTransaction_Transaction_1addConflictRange(
		JNIEnv *jenv, jobject, jlong tPtr, jbyteArray keyBegin, jbyteArray keyEnd, jint conflictType) {
	if( !tPtr || !keyBegin || !keyEnd ) {
		throwParamNotNull(jenv);
		return;
	}
	FDBTransaction *tr = (FDBTransaction *)tPtr;

	uint8_t *begin_barr = (uint8_t *)jenv->GetByteArrayElements( keyBegin, NULL );
	if (!begin_barr) {
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return;
	}
	int begin_size = jenv->GetArrayLength( keyBegin );

	uint8_t *end_barr = (uint8_t *)jenv->GetByteArrayElements( keyEnd, NULL );
	if (!end_barr) {
		jenv->ReleaseByteArrayElements( keyBegin, (jbyte *)begin_barr, JNI_ABORT );
		if( !jenv->ExceptionOccurred() )
			throwRuntimeEx( jenv, "Error getting handle to native resources" );
		return;
	}
	int end_size = jenv->GetArrayLength( keyEnd );

	fdb_error_t err = fdb_transaction_add_conflict_range( tr, begin_barr, begin_size, end_barr, end_size, (FDBConflictRangeType)conflictType );

	jenv->ReleaseByteArrayElements( keyBegin, (jbyte *)begin_barr, JNI_ABORT );
	jenv->ReleaseByteArrayElements( keyEnd, (jbyte *)end_barr, JNI_ABORT );

	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDB_Select_1API_1version(JNIEnv *jenv, jclass, jint version) {
	fdb_error_t err = fdb_select_api_version( (int)version );
	if( err ) {
		if( err == 2203 ) {
			int maxSupportedVersion = fdb_get_max_api_version();

			char errorStr[1024];
			if(FDB_API_VERSION > maxSupportedVersion) {
				snprintf(errorStr, sizeof(errorStr), "This version of the FoundationDB Java binding is not supported by the installed "
						 							 "FoundationDB C library. The binding requires a library that supports API version " 
						 							 "%d, but the installed library supports a maximum version of %d.",
													 FDB_API_VERSION, maxSupportedVersion);
			}
			else {
				snprintf(errorStr, sizeof(errorStr), "API version %d is not supported by the installed FoundationDB C library.", version);
			}

			safeThrow( jenv, getThrowable( jenv, err, errorStr ) );
		}
		else {
			safeThrow( jenv, getThrowable( jenv, err ) );
		}
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDB_Network_1setOption(JNIEnv *jenv, jobject, jint code, jbyteArray value) {
	uint8_t *barr = NULL;
	int size = 0;
	if(value != 0) {
		barr = (uint8_t *)jenv->GetByteArrayElements( value, NULL );
		if (!barr) {
			if( !jenv->ExceptionOccurred() )
				throwRuntimeEx( jenv, "Error getting handle to native resources" );
			return;
		}
		size = jenv->GetArrayLength( value );
	}
	fdb_error_t err = fdb_network_set_option((FDBNetworkOption)code, barr, size);
	if(value != 0)
		jenv->ReleaseByteArrayElements( value, (jbyte *)barr, JNI_ABORT );
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDB_Network_1setup(JNIEnv *jenv, jobject) {
	fdb_error_t err = fdb_setup_network();
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDB_Network_1run(JNIEnv *jenv, jobject) {
	// initialize things for the callbacks on the network thread
	g_thread_jenv = jenv;
	if( !g_IFutureCallback_call_methodID ) {
		if( !findCallbackMethods( jenv ) )
			return;
	}

	fdb_error_t hookErr = fdb_add_network_thread_completion_hook( &detachIfExternalThread, NULL );
	if( hookErr ) {
		safeThrow( jenv, getThrowable( jenv, hookErr ) );
	}

	fdb_error_t err = fdb_run_network();
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
	}
}

JNIEXPORT void JNICALL Java_com_apple_foundationdb_FDB_Network_1stop(JNIEnv *jenv, jobject) {
	fdb_error_t err = fdb_stop_network();
	if( err ) {
		safeThrow( jenv, getThrowable( jenv, err ) );
	}
}

jint JNI_OnLoad(JavaVM *vm, void *reserved) {
	g_jvm = vm;
	return JNI_VERSION_1_1;
}

#ifdef __cplusplus
}
#endif

