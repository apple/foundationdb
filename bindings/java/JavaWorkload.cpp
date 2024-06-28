/*
 * JavaWorkload.cpp
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

#include <foundationdb/CppWorkload.h>
#define FDB_USE_LATEST_BINDINGS_API_VERSION
#include <foundationdb/fdb_c.h>

#include "com_apple_foundationdb_testing_AbstractWorkload.h"
#include "com_apple_foundationdb_testing_Promise.h"
#include "com_apple_foundationdb_testing_WorkloadContext.h"

#include <jni.h>
#include <set>
#include <iostream>
#include <boost/algorithm/string.hpp>

namespace {
// to make logging more convenient
// this should be fine as it is guarded by
// a anon namespace
auto debug = FDBSeverity::Debug;
auto info = FDBSeverity::Info;
auto error = FDBSeverity::Error;

void printTrace(JNIEnv* env, jclass, jlong logger, jint severity, jstring message, jobject details) {
	auto log = reinterpret_cast<FDBLogger*>(logger);
	jboolean isCopy;
	const char* msg = env->GetStringUTFChars(message, &isCopy);
	std::vector<std::pair<std::string, std::string>> detailsMap;
	if (details != nullptr) {
		jclass mapClass = env->FindClass("java/util/Map");
		jclass setClass = env->FindClass("java/util/Set");
		jclass iteratorClass = env->FindClass("java/util/Iterator");
		jmethodID keySetID = env->GetMethodID(mapClass, "keySet", "()Ljava/util/Set;");
		jobject keySet = env->CallObjectMethod(details, keySetID);
		jmethodID iteratorMethodID = env->GetMethodID(setClass, "iterator", "()Ljava/util/Iterator;");
		jobject iterator = env->CallObjectMethod(keySet, iteratorMethodID);
		jmethodID hasNextID = env->GetMethodID(iteratorClass, "hasNext", "()Z");
		jmethodID nextID = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");
		jmethodID getID = env->GetMethodID(mapClass, "get", "(Ljava/lang/Object;)Ljava/lang/Object;");
		while (env->CallBooleanMethod(iterator, hasNextID)) {
			jobject next = env->CallObjectMethod(iterator, nextID);
			jstring key = jstring(next);
			jstring value = jstring(env->CallObjectMethod(details, getID, next));
			auto keyStr = env->GetStringUTFChars(key, nullptr);
			auto keyLen = env->GetStringUTFLength(key);
			auto valueStr = env->GetStringUTFChars(value, nullptr);
			auto valueLen = env->GetStringUTFLength(value);
			detailsMap.emplace_back(std::string(keyStr, keyLen), std::string(valueStr, valueLen));
			env->ReleaseStringUTFChars(key, keyStr);
			env->ReleaseStringUTFChars(value, valueStr);
			env->DeleteLocalRef(key);
			env->DeleteLocalRef(value);
		}
	}
	FDBSeverity sev;
	if (severity < 10) {
		sev = debug;
	} else if (severity < 20) {
		sev = FDBSeverity::Info;
	} else if (severity < 30) {
		sev = FDBSeverity::Warn;
	} else if (severity < 40) {
		sev = FDBSeverity::WarnAlways;
	} else {
		assert(false);
		std::abort();
	}
	log->trace(sev, msg, detailsMap);
	if (isCopy) {
		env->ReleaseStringUTFChars(message, msg);
	}
}

jlong getProcessID(JNIEnv* env, jclass, jlong self) {
	FDBWorkloadContext* context = reinterpret_cast<FDBWorkloadContext*>(self);
	return jlong(context->getProcessID());
}

void setProcessID(JNIEnv* env, jclass, jlong self, jlong processID) {
	FDBWorkloadContext* context = reinterpret_cast<FDBWorkloadContext*>(self);
	context->setProcessID(processID);
}

jboolean getOptionBool(JNIEnv* env, jclass, jlong self, jstring name, jboolean defaultValue) {
	FDBWorkloadContext* context = reinterpret_cast<FDBWorkloadContext*>(self);
	jboolean isCopy = true;
	const char* utf = env->GetStringUTFChars(name, &isCopy);
	auto res = jboolean(context->getOption(utf, bool(defaultValue)));
	if (isCopy) {
		env->ReleaseStringUTFChars(name, utf);
	}
	return res;
}

jlong getOptionLong(JNIEnv* env, jclass, jlong self, jstring name, jlong defaultValue) {
	FDBWorkloadContext* context = reinterpret_cast<FDBWorkloadContext*>(self);
	jboolean isCopy = true;
	const char* utf = env->GetStringUTFChars(name, &isCopy);
	auto res = jlong(context->getOption(utf, long(defaultValue)));
	if (isCopy) {
		env->ReleaseStringUTFChars(name, utf);
	}
	return res;
}

jdouble getOptionDouble(JNIEnv* env, jclass, jlong self, jstring name, jdouble defaultValue) {
	FDBWorkloadContext* context = reinterpret_cast<FDBWorkloadContext*>(self);
	jboolean isCopy = true;
	const char* utf = env->GetStringUTFChars(name, &isCopy);
	auto res = jdouble(context->getOption(utf, double(defaultValue)));
	if (isCopy) {
		env->ReleaseStringUTFChars(name, utf);
	}
	return res;
}

jstring getOptionString(JNIEnv* env, jclass, jlong self, jstring name, jstring defaultValue) {
	FDBWorkloadContext* context = reinterpret_cast<FDBWorkloadContext*>(self);
	jboolean isCopy;
	jboolean defIsCopy;
	const char* nameStr = env->GetStringUTFChars(name, &isCopy);
	const char* defStr = env->GetStringUTFChars(defaultValue, &defIsCopy);
	auto res = context->getOption(nameStr, std::string(defStr));
	if (isCopy) {
		env->ReleaseStringUTFChars(name, nameStr);
	}
	if (defIsCopy) {
		env->ReleaseStringUTFChars(defaultValue, defStr);
	}
	return env->NewStringUTF(res.c_str());
}

jint getClientID(JNIEnv* env, jclass, jlong self) {
	FDBWorkloadContext* context = reinterpret_cast<FDBWorkloadContext*>(self);
	return jint(context->clientId());
}

jint getClientCount(JNIEnv* env, jclass, jlong self) {
	FDBWorkloadContext* context = reinterpret_cast<FDBWorkloadContext*>(self);
	return jint(context->clientCount());
}

jlong getSharedRandomNumber(JNIEnv* env, jclass, jlong self) {
	FDBWorkloadContext* context = reinterpret_cast<FDBWorkloadContext*>(self);
	return jlong(context->sharedRandomNumber());
}

struct JavaPromise {
	GenericPromise<bool> impl;
	JavaPromise(GenericPromise<bool>&& promise) : impl(std::move(promise)) {}

	void send(bool val) {
		impl.send(val);
		delete this;
	}
};

void promiseSend(JNIEnv, jclass, jlong self, jboolean value) {
	auto p = reinterpret_cast<JavaPromise*>(self);
	p->send(bool(value));
}

struct JNIError {
	JNIEnv* env;
	jthrowable throwable{ nullptr };
	const char* file{ nullptr };
	int line{ 0 };

	std::string location() const {
		if (file == nullptr) {
			return "UNKNOWN";
		} else {
			return file + std::string(":") + std::to_string(line);
		}
	}

	std::string toString() {
		if (!throwable) {
			return "JNIError";
		} else {
			jboolean isCopy = false;
			jmethodID toStringM =
			    env->GetMethodID(env->FindClass("java/lang/Object"), "toString", "()Ljava/lang/String;");
			jstring s = (jstring)env->CallObjectMethod(throwable, toStringM);
			const char* utf = env->GetStringUTFChars(s, &isCopy);
			std::string res(utf);
			env->ReleaseStringUTFChars(s, utf);
			return res;
		}
	}
};

struct JVM {
	FDBLogger* log;
	JavaVM* jvm;
	JNIEnv* env;
	std::set<std::string> classPath;
	bool healthy = false;
	jclass throwableClass;
	jclass abstractWorkloadClass = nullptr;
	//  this is a bit ugly - but JNINativeMethod requires
	// char*  not const char *
	std::vector<char*> charArrays;

	void checkExceptionImpl(const char* file, int line) {
		if (env->ExceptionCheck()) {
			throw JNIError{ env, env->ExceptionOccurred(), file, line };
		}
	}

#define checkException() checkExceptionImpl(__FILE__, __LINE__)

	void success(int res) {
		bool didThrow = env->ExceptionCheck();
		if (res == JNI_ERR || didThrow) {
			throw JNIError{ env, didThrow ? env->ExceptionOccurred() : nullptr };
		}
	}

	JVM(FDBLogger* log) : log(log) {
		try {
			log->trace(FDBSeverity::Debug, "InitializeJVM", {});
			JavaVMInitArgs args;
			args.version = JNI_VERSION_1_6;
			args.ignoreUnrecognized = JNI_TRUE;
			args.nOptions = 0;
			success(JNI_CreateJavaVM(&jvm, reinterpret_cast<void**>(&env), &args));
			log->trace(debug, "JVMCreated", {});
			throwableClass = env->FindClass("java/lang/Throwable");
		} catch (JNIError& e) {
			healthy = false;
			env->ExceptionClear();
		}
	}

	~JVM() {
		log->trace(debug, "JVMDestruct", {});
		if (jvm) {
			jvm->DestroyJavaVM();
		}
		for (auto& a : charArrays) {
			delete[] a;
		}
		log->trace(debug, "JVMDestructDone", {});
	}

	void setNativeMethods(jclass clazz,
	                      const std::initializer_list<std::tuple<std::string_view, std::string_view, void*>>& methods) {
		charArrays.reserve(charArrays.size() + 2 * methods.size());
		std::unique_ptr<JNINativeMethod[]> nativeMethods;
		int numNativeMethods = methods.size();
		nativeMethods.reset(new JNINativeMethod[numNativeMethods]);
		int i = 0;
		for (const auto& t : methods) {
			auto& w = nativeMethods[i];
			auto nameStr = std::get<0>(t);
			auto sigStr = std::get<1>(t);
			charArrays.push_back(new char[nameStr.size() + 1]);
			char* name = charArrays.back();
			charArrays.push_back(new char[sigStr.size() + 1]);
			char* sig = charArrays.back();
			memcpy(name, nameStr.data(), nameStr.size());
			memcpy(sig, sigStr.data(), sigStr.size());
			name[nameStr.size()] = '\0';
			sig[sigStr.size()] = '\0';
			w.name = name;
			w.signature = sig;
			w.fnPtr = std::get<2>(t);
			log->trace(info,
			           "PreparedNativeMethod",
			           { { "Name", w.name },
			             { "Signature", w.signature },
			             { "Ptr", std::to_string(reinterpret_cast<uintptr_t>(w.fnPtr)) } });
			++i;
		}
		env->RegisterNatives(clazz, nativeMethods.get(), numNativeMethods);
		checkException();
	}

	jclass getClassImpl(const char* file, int line, const char* name) {
		auto res = env->FindClass(name);
		checkExceptionImpl(file, line);
		return res;
	}

#define getClass(name) getClassImpl(__FILE__, __LINE__, name)

	jmethodID getMethodImpl(const char* file, int line, jclass clazz, const char* name, const char* signature) {
		auto res = env->GetMethodID(clazz, name, signature);
		checkExceptionImpl(file, line);
		return res;
	}

#define getMethod(clazz, name, signature) getMethodImpl(__FILE__, __LINE__, clazz, name, signature)

	jfieldID getFieldImpl(const char* file, int line, jclass clazz, const char* name, const char* signature) {
		auto res = env->GetFieldID(clazz, name, signature);
		checkException();
		return res;
	}

#define getField(clazz, name, signature) getFieldImpl(__FILE__, __LINE__, clazz, name, signature)

	void addToClassPath(const std::string& path) {
		log->trace(info, "TryAddToClassPath", { { "Path", path } });
		if (!env) {
			throw JNIError{};
		}
		if (classPath.count(path) > 0) {
			// already added
			return;
		}
		auto p = env->NewStringUTF(path.c_str());
		checkException();
		auto fileClass = getClass("java/io/File");
		auto file = env->NewObject(fileClass, getMethod(fileClass, "<init>", "(Ljava/lang/String;)V"), p);
		checkException();
		auto uri = env->CallObjectMethod(file, env->GetMethodID(fileClass, "toURI", "()Ljava/net/URI;"));
		checkException();
		auto uriClass = getClass("java/net/URI");
		auto url = env->CallObjectMethod(uri, getMethod(uriClass, "toURL", "()Ljava/net/URL;"));
		checkException();
		auto classLoaderClass = getClass("java/lang/ClassLoader");
		auto sysLoaderMethod =
		    env->GetStaticMethodID(classLoaderClass, "getSystemClassLoader", "()Ljava/lang/ClassLoader;");
		checkException();
		auto classLoader = env->CallStaticObjectMethod(classLoaderClass, sysLoaderMethod);
		checkException();
		auto urlLoaderClass = getClass("java/net/URLClassLoader");
		env->CallVoidMethod(classLoader, getMethod(urlLoaderClass, "addURL", "(Ljava/net/URL;)V"), url);
		env->DeleteLocalRef(classLoader);
		checkException();
	}

	void init() {
		if (abstractWorkloadClass != nullptr) {
			return;
		}
		abstractWorkloadClass = getClass("com/apple/foundationdb/testing/AbstractWorkload");
		setNativeMethods(abstractWorkloadClass,
		                 { { "log", "(JILjava/lang/String;Ljava/util/Map;)V", reinterpret_cast<void*>(&printTrace) } });
		auto loggerField = env->GetStaticFieldID(abstractWorkloadClass, "logger", "J");
		checkException();
		env->SetStaticLongField(abstractWorkloadClass, loggerField, reinterpret_cast<jlong>(log));
		log->trace(info, "SetLogger", { { "Logger", std::to_string(reinterpret_cast<jlong>(log)) } });
		setNativeMethods(getClass("com/apple/foundationdb/testing/WorkloadContext"),
		                 { { "getProcessID", "(J)J", reinterpret_cast<void*>(&getProcessID) },
		                   { "setProcessID", "(JJ)V", reinterpret_cast<void*>(&setProcessID) },
		                   { "getOption", "(JLjava/lang/String;Z)Z", reinterpret_cast<void*>(&getOptionBool) },
		                   { "getOption", "(JLjava/lang/String;J)J", reinterpret_cast<void*>(&getOptionLong) },
		                   { "getOption", "(JLjava/lang/String;D)D", reinterpret_cast<void*>(&getOptionDouble) },
		                   { "getOption",
		                     "(JLjava/lang/String;Ljava/lang/String;)Ljava/lang/String;",
		                     reinterpret_cast<void*>(&getOptionString) },
		                   { "getClientID", "(J)I", reinterpret_cast<void*>(&getClientID) },
		                   { "getClientCount", "(J)I", reinterpret_cast<void*>(&getClientCount) },
		                   { "getSharedRandomNumber", "(J)J", reinterpret_cast<void*>(&getSharedRandomNumber) } });
		setNativeMethods(getClass("com/apple/foundationdb/testing/Promise"),
		                 { { "send", "(JZ)V", reinterpret_cast<void*>(&promiseSend) } });
		auto fdbClass = getClass("com/apple/foundationdb/FDB");
		jmethodID selectMethod =
		    env->GetStaticMethodID(fdbClass, "selectAPIVersion", "(I)Lcom/apple/foundationdb/FDB;");
		checkException();
		auto fdbInstance = env->CallStaticObjectMethod(fdbClass, selectMethod, jint(FDB_API_VERSION));
		checkException();
		env->CallObjectMethod(fdbInstance, getMethod(fdbClass, "disableShutdownHook", "()V"));
		checkException();
	}

	jobject createWorkloadContext(FDBWorkloadContext* context) {
		auto clazz = getClass("com/apple/foundationdb/testing/WorkloadContext");
		auto constructor = getMethod(clazz, "<init>", "(J)V");
		auto jContext = reinterpret_cast<jlong>(context);
		jobject res = env->NewObject(clazz, constructor, jContext);
		std::cout.flush();
		checkException();
		auto field = env->GetFieldID(clazz, "impl", "J");
		checkException();
		auto impl = env->GetLongField(res, field);
		checkException();
		if (impl != jContext) {
			log->trace(error,
			           "ContextNotCorrect",
			           { { "Expected", std::to_string(jContext) }, { "Impl", std::to_string(impl) } });
			std::terminate();
		}
		return res;
	}

	jobject createWorkload(jobject context, const std::string& workloadName) {
		auto clazz = getClass(workloadName.c_str());
		if (!env->IsAssignableFrom(clazz, abstractWorkloadClass)) {
			log->trace(error, "ClassNotAWorkload", { { "Class", workloadName } });
			return nullptr;
		}
		auto constructor = getMethod(clazz, "<init>", "(Lcom/apple/foundationdb/testing/WorkloadContext;)V");
		auto res = env->NewObject(clazz, constructor, context);
		checkException();
		env->NewGlobalRef(res);
		return res;
	}

	jobject createPromise(GenericPromise<bool>&& promise) {
		auto p = std::make_unique<JavaPromise>(std::move(promise));
		auto clazz = getClass("com/apple/foundationdb/testing/Promise");
		auto res = env->NewObject(clazz, getMethod(clazz, "<init>", "(J)V"), reinterpret_cast<jlong>(p.get()));
		checkException();
		p.release();
		return res;
	}

	void shutdownWorkload(jobject workload, const std::string& workloadName) {
		auto clazz = getClass(workloadName.c_str());
		env->CallVoidMethod(workload, getMethod(clazz, "shutdown", "()V"));
		checkException();
	}

	std::string jtoStr(jstring str) {
		jboolean isCopy;
		auto arr = env->GetStringUTFChars(str, &isCopy);
		std::string res(arr);
		if (isCopy) {
			env->ReleaseStringUTFChars(str, arr);
		}
		return res;
	}

	void getMetrics(jobject workload, const std::string& workloadName, std::vector<FDBPerfMetric>& result) {
		auto clazz = getClass(workloadName.c_str());
		auto perfMetricClass = getClass("Lcom/apple/foundationdb/testing/PerfMetric;");
		auto nameId = getField(perfMetricClass, "name", "Ljava/lang/String;");
		auto valueId = getField(perfMetricClass, "value", "D");
		auto averagedId = getField(perfMetricClass, "averaged", "Z");
		auto formatCodeId = getField(perfMetricClass, "formatCode", "Ljava/lang/String;");
		auto v = env->CallObjectMethod(workload, getMethod(clazz, "getMetrics", "()Ljava/util/List;"));
		checkException();
		auto listClass = getClass("java/util/List");
		auto iter = env->CallObjectMethod(v, getMethod(listClass, "iterator", "()Ljava/util/Iterator;"));
		checkException();
		auto iterClass = getClass("java/util/Iterator");
		auto hasNextM = getMethod(iterClass, "hasNext", "()Z");
		auto nextM = getMethod(iterClass, "next", "()Ljava/lang/Object;");
		jboolean hasNext = env->CallBooleanMethod(iter, hasNextM);
		checkException();
		while (hasNext) {
			auto perfMetric = env->CallObjectMethod(iter, nextM);
			checkException();
			auto name = jtoStr(jstring(env->GetObjectField(perfMetric, nameId)));
			checkException();
			auto value = env->GetDoubleField(perfMetric, valueId);
			checkException();
			auto averaged = env->GetBooleanField(perfMetric, averagedId);
			checkException();
			auto formatCode = jtoStr(jstring(env->GetObjectField(perfMetric, formatCodeId)));
			result.emplace_back(FDBPerfMetric{ name, value, bool(averaged), formatCode });
			hasNext = env->CallBooleanMethod(iter, hasNextM);
			checkException();
		}
		return;
	}

	jobject createDatabase(jobject workload, FDBDatabase* db) {
		auto executor = env->CallObjectMethod(workload,
		                                      getMethod(getClass("com/apple/foundationdb/testing/AbstractWorkload"),
		                                                "getExecutor",
		                                                "()Ljava/util/concurrent/Executor;"));
		auto databaseClass = getClass("com/apple/foundationdb/FDBDatabase");
		jlong databasePtr = reinterpret_cast<jlong>(db);
		jobject javaDatabase = env->NewObject(databaseClass,
		                                      getMethod(databaseClass, "<init>", "(JLjava/util/concurrent/Executor;)V"),
		                                      databasePtr,
		                                      executor);
		env->DeleteLocalRef(executor);
		return javaDatabase;
	}

	void callWorkload(jobject workload, FDBDatabase* db, const char* method, GenericPromise<bool>&& promise) {
		jobject jPromise = nullptr;
		try {
			auto clazz = getClass("com/apple/foundationdb/testing/AbstractWorkload");
			auto jdb = createDatabase(workload, db);
			jPromise = createPromise(std::move(promise));
			env->CallVoidMethod(
			    workload,
			    getMethod(
			        clazz, method, "(Lcom/apple/foundationdb/Database;Lcom/apple/foundationdb/testing/Promise;)V"),
			    jdb,
			    jPromise);
			env->DeleteLocalRef(jdb);
			env->DeleteLocalRef(jPromise);
			jPromise = nullptr;
			checkException();
		} catch (...) {
			if (jPromise) {
				env->DeleteLocalRef(jPromise);
			}
			throw;
		}
	}
};

struct JavaWorkload final : FDBWorkload {
	std::shared_ptr<JVM> jvm;
	FDBLogger& log;
	FDBWorkloadContext* context = nullptr;
	std::string name;
	bool failed = false;
	jobject workload = nullptr;
	JavaWorkload(const std::shared_ptr<JVM>& jvm, FDBLogger& log, const std::string& name)
	  : jvm(jvm), log(log), name(name) {
		boost::replace_all(this->name, ".", "/");
	}
	~JavaWorkload() {
		if (workload) {
			try {
				jvm->shutdownWorkload(workload, name);
				jvm->env->DeleteGlobalRef(workload);
			} catch (JNIError& e) {
				log.trace(error, "JNIShutDownUnsucessful", { { "Error", e.toString() }, { "Location", e.location() } });
			}
		}
	}

	// std::string description() const override { return name; }
	bool init(FDBWorkloadContext* context) override {
		this->context = context;
		try {
			std::string classPath = context->getOption("classPath", std::string(""));
			std::vector<std::string> paths;
			boost::split(paths, classPath, boost::is_any_of(";,"), boost::token_compress_on);
			for (const auto& path : paths) {
				jvm->addToClassPath(path);
			}
			jvm->init();
			jobject jContext = jvm->createWorkloadContext(context);
			if (jContext == nullptr) {
				failed = true;
				return failed;
			}
			workload = jvm->createWorkload(jContext, name);
		} catch (JNIError& e) {
			failed = true;
			log.trace(error, "JNIError", { { "Location", e.location() }, { "Error", e.toString() } });
		}
		return failed;
	};
	void setup(FDBDatabase* db, GenericPromise<bool> done) override {
		if (failed) {
			done.send(false);
			return;
		}
		try {
			jvm->callWorkload(workload, db, "setup", std::move(done));
		} catch (JNIError& e) {
			failed = true;
			log.trace(error, "SetupFailedWithJNIError", { { "Error", e.toString() }, { "Location", e.location() } });
		}
	}
	void start(FDBDatabase* db, GenericPromise<bool> done) override {
		if (failed) {
			done.send(false);
			return;
		}
		try {
			jvm->callWorkload(workload, db, "start", std::move(done));
		} catch (JNIError& e) {
			failed = true;
			log.trace(error, "StartFailedWithJNIError", { { "Error", e.toString() }, { "Location", e.location() } });
		}
	}
	void check(FDBDatabase* db, GenericPromise<bool> done) override {
		if (failed) {
			done.send(false);
			return;
		}
		try {
			jvm->callWorkload(workload, db, "check", std::move(done));
		} catch (JNIError& e) {
			failed = true;
			log.trace(error, "CheckFailedWithJNIError", { { "Error", e.toString() }, { "Location", e.location() } });
		}
	}
	void getMetrics(std::vector<FDBPerfMetric>& out) const override { jvm->getMetrics(workload, name, out); }
};

struct JavaWorkloadFactory : FDBWorkloadFactory {
	FDBLogger* log;
	std::weak_ptr<JVM> jvm;
	JavaWorkloadFactory(FDBLogger* log) : log(log) {}
	JavaWorkloadFactory(const JavaWorkloadFactory&) = delete;
	JavaWorkloadFactory& operator=(const JavaWorkloadFactory&) = delete;
	std::shared_ptr<FDBWorkload> create(const std::string& name) override {
		auto jvmPtr = jvm.lock();
		if (!jvmPtr) {
			jvmPtr = std::make_shared<JVM>(log);
			jvm = jvmPtr;
		}
		return std::make_shared<JavaWorkload>(jvmPtr, *log, name);
	}
};

} // namespace

extern "C" DLLEXPORT FDBWorkloadFactory* workloadFactory(FDBLogger* logger);

FDBWorkloadFactory* workloadFactory(FDBLogger* logger) {
	static JavaWorkloadFactory factory(logger);
	return &factory;
}
