#include "SysTestWorkload.h"
#include <memory>

namespace FDBSystemTester {

void WorkloadBase::init(ITransactionExecutor* txExecutor, IScheduler* sched, TTaskFct cont) {
	this->txExecutor = txExecutor;
	this->scheduler = sched;
	this->doneCont = cont;
}

void WorkloadBase::schedule(TTaskFct task) {
	tasksScheduled++;
	scheduler->schedule([this, task]() {
		tasksScheduled--;
		task();
		contIfDone();
	});
}

void WorkloadBase::execTransaction(std::shared_ptr<ITransactionActor> tx, TTaskFct cont) {
	txRunning++;
	txExecutor->execute(tx, [this, cont]() {
		txRunning--;
		cont();
		contIfDone();
	});
}

void WorkloadBase::contIfDone() {
	if (txRunning == 0 && tasksScheduled == 0) {
		doneCont();
	}
}

} // namespace FDBSystemTester