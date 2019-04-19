#include "workloads.actor.h"
#include <flow/ThreadHelper.actor.h>

#include <jni.h>
#include <fdbrpc/simulator.h>
#include <fdbclient/IClientApi.h>
#include <fdbclient/ThreadSafeTransaction.h>

#include <memory>

#include <flow/actorcompiler.h> // must be last include

extern void flushTraceFileVoid();

namespace {

void printTrace(JNIEnv* env, jobject self, jint severity, jstring message, jobject details) {
	jboolean isCopy;
	const char* msg = env->GetStringUTFChars(message, &isCopy);
	std::unordered_map<std::string, std::string> detailsMap;
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
			detailsMap.emplace(std::string(keyStr, keyLen), std::string(valueStr, valueLen));
			env->ReleaseStringUTFChars(key, keyStr);
			env->ReleaseStringUTFChars(value, valueStr);
			env->DeleteLocalRef(key);
			env->DeleteLocalRef(value);
		}
	}
	auto f = onMainThread([severity, &detailsMap, msg]() -> Future<Void> {
		TraceEvent evt(Severity(severity), msg);
		for (const auto& p : detailsMap) {
			evt.detail(p.first.c_str(), p.second);
		}
		return Void();
	});
	f.blockUntilReady();
	if (isCopy) {
		env->ReleaseStringUTFChars(message, msg);
	}
}

void sendVoid(JNIEnv* env, jobject self, jlong promisePtr) {
	auto f = onMainThread([promisePtr]() -> Future<Void> {
		Promise<Void>* p = reinterpret_cast<Promise<Void>*>(promisePtr);
		p->send(Void());
		delete p;
		return Void();
	});
}

void sendBool(JNIEnv* env, jobject self, jlong promisePtr, jboolean value) {
	auto f = onMainThread([promisePtr, value]() -> Future<Void> {
		Promise<bool>* p = reinterpret_cast<Promise<bool>*>(promisePtr);
		p->send(value);
		delete p;
		return Void();
	});
}

void setProcessID(JNIEnv* env, jobject self, jlong processID) {
	if (g_network->isSimulated()) {
		g_simulator.currentProcess = reinterpret_cast<ISimulator::ProcessInfo*>(processID);
	}
}

struct JVMContext {
	JNIEnv* env = nullptr;
	JavaVM* jvm = nullptr;
	// the JVM requires char* args
	std::vector<char*> jvmArgs;
	jclass workloadClass;
	jclass throwableClass;
	jclass fdbClass;
	jobject fdbObject;
	bool success = true;
	std::unique_ptr<JNINativeMethod[]>  workloadMethods;
	int numWorkloadMethods;
	//  this is a bit ugly - but JNINativeMethod requires
	// char*  not const char *
	std::vector<char*> charArrays;
	std::set<std::string> classPath;

	void setWorkloadMethods(const std::initializer_list<std::tuple<StringRef, StringRef, void*>>& methods) {
		charArrays.reserve(charArrays.size() + 2*methods.size());
		numWorkloadMethods = methods.size();
		workloadMethods.reset(new JNINativeMethod[numWorkloadMethods]);
		int i = 0;
		for (const auto& t : methods) {
			auto& w = workloadMethods[i];
			StringRef nameStr = std::get<0>(t);
			StringRef sigStr = std::get<1>(t);
			charArrays.push_back(new char[nameStr.size() + 1]);
			char* name = charArrays.back();
			charArrays.push_back(new char[sigStr.size() + 1]);
			char* sig = charArrays.back();
			memcpy(name, nameStr.begin(), nameStr.size());
			memcpy(sig, sigStr.begin(), sigStr.size());
			name[nameStr.size()] = '\0';
			sig[sigStr.size()] = '\0';
			w.name = name;
			w.signature = sig;
			w.fnPtr = std::get<2>(t);
			TraceEvent("PreparedNativeMethod")
				.detail("Name", w.name)
				.detail("Signature", w.signature)
				.detail("Ptr", reinterpret_cast<uintptr_t>(w.fnPtr));
			++i;
		}
	}

	template<class Args>
	JVMContext(Args&& jvmArgs)
		: jvmArgs(std::forward<Args>(jvmArgs))
		, fdbClass(nullptr)
		, fdbObject(nullptr) {
		setWorkloadMethods({
				{
					std::make_tuple<StringRef, StringRef, void*>(
						LiteralStringRef("log"),
						LiteralStringRef("(ILjava/lang/String;Ljava/util/Map;)V"),
						reinterpret_cast<void*>(&printTrace))
				},
				{
					std::make_tuple<StringRef, StringRef, void*>(
						LiteralStringRef("sendVoid"),
						LiteralStringRef("(J)V"),
						reinterpret_cast<void*>(&sendVoid))
				},
				{ std::make_tuple<StringRef, StringRef, void*>(
						LiteralStringRef("sendBool"),
						LiteralStringRef("(JZ)V"),
						reinterpret_cast<void*>(&sendBool))
				},
				{ std::make_tuple<StringRef, StringRef, void*>(
						LiteralStringRef("setProcessID"),
						LiteralStringRef("(J)V"),
						reinterpret_cast<void*>(&setProcessID))
				}});
		init();
	}

	~JVMContext() {
		TraceEvent(SevDebug, "JVMContextDestruct");
		flushTraceFileVoid();
		if (jvm) {
			if (fdbObject) {
				env->DeleteGlobalRef(fdbObject);
			}
			jvm->DestroyJavaVM();
		}
		for (auto& arr : jvmArgs) {
			delete[] arr;
		}
		for (auto& arr : charArrays) {
			delete[] arr;
		}
		TraceEvent(SevDebug, "JVMContextDestructDone");
		flushTraceFileVoid();
	}

	bool addToClassPath(const std::string& path) {
		TraceEvent("TryAddToClassPath")
			.detail("Path", "path");
		flushTraceFileVoid();
		if (!success) {
			return false;
		}
		if (classPath.count(path) > 0) {
			// already added
			return true;
		}
		auto addFileMethod = env->GetStaticMethodID(workloadClass, "addFile", "(Ljava/lang/String;)V");
		if (!checkException()) {
			return false;
		}
		auto p = env->NewStringUTF(path.c_str());
		env->CallStaticVoidMethod(workloadClass, addFileMethod, p);
		if (!checkException()) {
			return false;
		}
		classPath.insert(path);
		return true;
	}

	bool checkException() {
		auto flag = env->ExceptionCheck();
		if (flag) {
			jthrowable exception = env->ExceptionOccurred();
			TraceEvent(SevError, "JavaException");
			env->ExceptionDescribe();
			env->ExceptionClear();
			return false;
		}
		return true;
	}

	void initializeFDB() {
		if (!success) {
			return;
		}
		fdbClass = env->FindClass("com/apple/foundationdb/FDB");
		jmethodID selectMethod = env->GetStaticMethodID(fdbClass, "selectAPIVersion", "(IZ)Lcom/apple/foundationdb/FDB;");
		if (!checkException()) {
			success = false;
			return;
		}
		fdbObject = env->CallStaticObjectMethod(fdbClass, selectMethod, jint(610), jboolean(false));
		if (!checkException()) {
			success = false;
			return;
		}
	}

	void init() {
		TraceEvent(SevDebug, "InitializeJVM");
		flushTraceFileVoid();
		JavaVMInitArgs args;
		args.version = JNI_VERSION_1_6;
		args.ignoreUnrecognized = JNI_TRUE;
		args.nOptions = jvmArgs.size();
		std::unique_ptr<JavaVMOption[]> options(new JavaVMOption[args.nOptions]);
		for (int i = 0; i < args.nOptions; ++i) {
			options[i].optionString = jvmArgs[i];
			TraceEvent(SevDebug, "AddJVMOption")
				.detail("Option", reinterpret_cast<const char*>(options[i].optionString));
			flushTraceFileVoid();
		}
		args.options = options.get();
		{
			TraceEvent evt(SevDebug, "StartVM");
			for (int i = 0; i < args.nOptions; ++i) {
				evt.detail(format("Option-%d", i), reinterpret_cast<const char*>(options[i].optionString));
			}
		}
		flushTraceFileVoid();
		auto res = JNI_CreateJavaVM(&jvm, reinterpret_cast<void**>(&env), &args);
		if (res == JNI_ERR) {
			success = false;
			env->ExceptionDescribe();
			return;
		}
		TraceEvent(SevDebug, "JVMStarted");
		flushTraceFileVoid();
		throwableClass = env->FindClass("java/lang/Throwable");
		workloadClass = env->FindClass("com/apple/foundationdb/testing/AbstractWorkload");
		if (workloadClass == nullptr) {
			success = false;
			TraceEvent(SevError, "ClassNotFound")
				.detail("ClassName", "com/apple/foundationdb/testing/AbstractWorkload");
			return;
		}
		TraceEvent(SevDebug, "RegisterNatives")
			.detail("ThrowableClass", format("%x", reinterpret_cast<uintptr_t>(throwableClass)))
			.detail("WorkloadClass", format("%x", reinterpret_cast<uintptr_t>(workloadClass)))
			.detail("NumMethods", numWorkloadMethods);
		flushTraceFileVoid();
		env->RegisterNatives(workloadClass, workloadMethods.get(), numWorkloadMethods);
		success = checkException() && success;
		initializeFDB();
	}

};

struct JavaWorkload : TestWorkload {
	static const std::string name;
	// From https://docs.oracle.com/javase/8/docs/technotes/guides/jni/spec/invocation.html#unloading_the_vm
	// > Creation of multiple VMs in a single process is not supported.
	// This means, that we have to share the VM across workloads.
	static std::weak_ptr<JVMContext> globalVM;
	std::shared_ptr<JVMContext> vm;
	std::vector<std::string> classPath;

	std::string className;

	bool success = true;
	jclass implClass;
	jobject impl = nullptr;

	explicit JavaWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		className = getOption(options, LiteralStringRef("workloadClass"), LiteralStringRef("")).toString();
		if (className == "") {
			success = false;
			return;
		}
		auto jvmOptions = getOption(options, LiteralStringRef("jvmOptions"), std::vector<std::string>{});
		classPath = getOption(options, LiteralStringRef("classPath"), std::vector<std::string>{});
		vm = globalVM.lock();
		if (!vm) {
			std::vector<char*> args;
			args.reserve(jvmOptions.size());
			for (const auto& opt : jvmOptions) {
				char* option = new char[opt.size() + 1];
				option[opt.size()] = '\0';
				std::copy(opt.begin(), opt.end(), option);
				args.emplace_back(option);
			}
			vm = std::make_shared<JVMContext>(args);
			globalVM = vm;
			success = vm->success;
		} else {
			success = vm->success;
		}
		if (success) {
			TraceEvent("JVMRunning");
			flushTraceFileVoid();
			try {
				createContext();
			} catch (Error& e) {
				success = false;
				TraceEvent(SevError, "JavaContextCreationFailed")
					.error(e);
			}
		}
		TraceEvent(SevDebug, "JavaWorkloadConstructed")
			.detail("Success", success);
		flushTraceFileVoid();
	}

	~JavaWorkload() {
		if (vm && impl) {
			try {
				auto shutdownID = getMethodID(vm->workloadClass, "shutdown", "()V");
				vm->env->CallVoidMethod(impl, shutdownID);
				if (!checkException()) {
					TraceEvent(SevError, "JavaWorkloadShutdownFailed")
						.detail("Reason", "AbstractWorkload::shutdown call");
				}
			} catch (Error& e) {
				TraceEvent(SevError, "JavaWorkloadShutdownFailed")
						.detail("Reason", "Exception");
			}
		}
		TraceEvent(SevDebug, "DestroyJavaWorkload");
		flushTraceFileVoid();
		if (vm && vm->env && impl)
			vm->env->DeleteGlobalRef(impl);
		TraceEvent(SevDebug, "DestroyJavaWorkloadComplete");
		flushTraceFileVoid();
	}

	bool checkException() {
		return vm->checkException();
	}

	void createContext() {
		TraceEvent("AddClassPaths")
			.detail("Num", classPath.size());
		flushTraceFileVoid();
		for (const auto& p : classPath) {
			if (!vm->addToClassPath(p)) {
				TraceEvent("AddToClassPathFailed")
					.detail("Path", p);
				success = false;
				return;
			}
			TraceEvent("AddToClassPath")
				.detail("Path", p);
		}
		std::transform(className.begin(), className.end(), className.begin(), [](char c) {
			if (c == '.') return '/';
			return c;
		});
		implClass = vm->env->FindClass(className.c_str());
		if (implClass == nullptr) {
			success = false;
			TraceEvent(SevError, "JavaWorkloadNotFound").detail("JavaClass", className);
			return;
		}
		if (!vm->env->IsAssignableFrom(implClass, vm->workloadClass)) {
			success = false;
			TraceEvent(SevError, "JClassNotAWorkload").detail("Class", className);
			return;
		}
		jint initalCapacity = options.size() * 2;
		jclass hashMapCls = vm->env->FindClass("java/util/HashMap");
		if (hashMapCls == nullptr) {
			success = false;
			TraceEvent(SevError, "ClassNotFound")
				.detail("ClassName", "java/util/HashMap");
			return;
		}
		jmethodID put = vm->env->GetMethodID(hashMapCls, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
		if (put == nullptr) {
			checkException();
			TraceEvent(SevError, "JavaMethodNotFound")
				.detail("Class", "java/util/HashMap")
				.detail("MethodName", "put")
				.detail("Signature", "(Ljava/lang/Object;Ljava/lang/Object)Ljava/lang/Object;");
			success = false;
			return;
		}
		jmethodID constr = vm->env->GetMethodID(hashMapCls, "<init>", "(I)V");
		jobject hashMap = vm->env->NewObject(hashMapCls, constr, initalCapacity);
		if (hashMap == nullptr || !checkException()) {
			TraceEvent(SevError, "JavaConstructionFailed")
				.detail("Class", "java/util/HashMap");
			success = false;
			return;
		}
		for (auto& kv : options) {
			auto key = vm->env->NewStringUTF(reinterpret_cast<const char*>(kv.key.begin()));
			auto value = vm->env->NewStringUTF(reinterpret_cast<const char*>(kv.value.begin()));
			vm->env->CallVoidMethod(hashMap, put, key, value);
			vm->env->DeleteLocalRef(key);
			vm->env->DeleteLocalRef(value);
			kv.value = LiteralStringRef("");
		}
		auto workloadContextClass = findClass("com/apple/foundationdb/testing/WorkloadContext");
		auto workloadContextConstructor = getMethodID(workloadContextClass, "<init>", "(Ljava/util/Map;IIJJ)V");
		jlong processID = 0;
		if (g_network->isSimulated()) {
			processID = reinterpret_cast<jlong>(g_simulator.getCurrentProcess());
		}
		TraceEvent(SevDebug, "WorkloadContextConstructorFound")
			.detail("FieldID", format("%x", reinterpret_cast<uintptr_t>(workloadContextConstructor)));
		flushTraceFileVoid();
		auto workloadContext = vm->env->NewObject(workloadContextClass, workloadContextConstructor, hashMap,
												  jint(clientId), jint(clientCount), jlong(sharedRandomNumber),
												  processID);
		if (!checkException() || workloadContext == nullptr) {
			success = false;
			TraceEvent(SevError, "CouldNotCreateWorkloadContext");
		}
		TraceEvent(SevDebug, "WorkloadContextConstructed")
			.detail("Object", format("%x", reinterpret_cast<uintptr_t>(workloadContext)));
		flushTraceFileVoid();
		auto implConstr = vm->env->GetMethodID(implClass, "<init>", "(Lcom/apple/foundationdb/testing/WorkloadContext;)V");
		if (!checkException() || implConstr == nullptr) {
			success = false;
			TraceEvent(SevError, "JavaWorkloadNotDefaultConstructible").detail("Class", className);
			return;
		}
		impl = vm->env->NewObject(implClass, implConstr, workloadContext);
		if (!checkException() || impl == nullptr) {
			success = false;
			TraceEvent(SevError, "JavaWorkloadConstructionFailed").detail("Class", className);
			return;
		}
		vm->env->NewGlobalRef(impl);
	}

	std::string description() override { return JavaWorkload::name; }

	jclass findClass(const char* className) {
		jclass res = vm->env->FindClass(className);
		if (res == nullptr) {
			checkException();
			success = false;
			TraceEvent(SevError, "ClassNotFound")
				.detail("ClassName", className);
			throw internal_error();
		}
		return res;
	}

	jmethodID getMethodID(jclass clazz, const char* name, const char* sig) {
		auto res = vm->env->GetMethodID(clazz, name, sig);
		if (!checkException() || res == nullptr) {
			success = false;
			TraceEvent(SevError, "JavaMethodNotFound")
				.detail("Name", name)
				.detail("Signature", sig);
			throw internal_error();
		}
		return res;
	}

	jfieldID getStaticFieldID(jclass clazz, const char* name, const char* signature) {
		auto res = vm->env->GetStaticFieldID(clazz, name, signature);
		if (!checkException() || res == nullptr) {
			success = false;
			TraceEvent(SevError, "FieldNotFound")
				.detail("FieldName", name)
				.detail("Signature", signature);
			throw internal_error();
		}
		return res;
	}

	jobject getStaticObjectField(jclass clazz, jfieldID field) {
		auto res = vm->env->GetStaticObjectField(clazz, field);
		if (!checkException() || res != nullptr) {
			success = false;
			TraceEvent(SevError, "CouldNotGetStaticObjectField");
			throw operation_failed();
		}
		return res;
	}

	template<class Ret>
	Future<Ret> callJava(Database const& db, const char* method, Ret failed) {
		TraceEvent(SevDebug, "CallJava")
			.detail("Method", method);
		flushTraceFileVoid();
		try {
			auto cx = db.getPtr();
			cx->addref();
			// First we need an executor for the Database class
			jmethodID executorMethod = getMethodID(vm->workloadClass, "getExecutor", "()Ljava/util/concurrent/Executor;");
			jobject executor = vm->env->CallObjectMethod(impl, executorMethod);
			if (!checkException()) {
				success = false;
				return failed;
			}
			if (executor == nullptr) {
				TraceEvent(SevError, "JavaExecutorIsVoid");
				success = false;
				return failed;
			}
			Reference<IDatabase> database(new ThreadSafeDatabase(cx));
			jlong databasePtr = reinterpret_cast<jlong>(database.extractPtr());
			jclass databaseClass = findClass("com/apple/foundationdb/FDBDatabase");

			// now we can create the Java Database object
			auto sig =  "(JLjava/util/concurrent/Executor;)V";
			jmethodID databaseConstructor = getMethodID(databaseClass, "<init>", sig);
			jobject javaDatabase = vm->env->NewObject(databaseClass, databaseConstructor, databasePtr, executor);
			if (!checkException() || javaDatabase == nullptr) {
				TraceEvent(SevError, "ConstructingDatabaseFailed")
					.detail("ConstructirSignature", sig);
				success = false;
				return failed;
			}

			auto p = new Promise<Ret>();
			jmethodID methodID = getMethodID(vm->workloadClass, method, "(Lcom/apple/foundationdb/Database;J)V");
			vm->env->CallVoidMethod(impl, methodID, javaDatabase, reinterpret_cast<jlong>(p));
			checkException();
			if (!checkException() || !success) {
				delete p;
				return failed;
			}
			return p->getFuture();
			// and now we can call the method with the created Database
		} catch (Error& e) {
			TraceEvent("CallJavaFailed")
				.error(e);
			success = false;
			return failed;
		}
	}

	Future<Void> setup(Database const& cx) override {
		if (!success) {
			return Void();
		}
		return callJava<Void>(cx, "setup", Void());
	}
	Future<Void> start(Database const& cx) override {
		if (!success) {
			return Void();
		}
		return callJava<Void>(cx, "start", Void());
	}
	Future<bool> check(Database const& cx) override {
		if (!success) {
			return false;
		}
		return callJava<bool>(cx, "check", false);
	}
	void getMetrics(vector<PerfMetric>& m) override {
		if (!success) {
			return;
		}
	}

	virtual double getCheckTimeout() {
		if (!success) {
			return 3000;
		}
		jmethodID methodID = vm->env->GetMethodID(implClass, "getCheckTimeout", "()D");
		jdouble res = vm->env->CallDoubleMethod(impl, methodID);
		checkException();
		if (!success) {
			return 3000;
		}
		return res;
	}
};

const std::string JavaWorkload::name = "JavaWorkload";
std::weak_ptr<JVMContext> JavaWorkload::globalVM;
} // namespace

WorkloadFactory<JavaWorkload> JavaWorkloadFactory(JavaWorkload::name.c_str());
