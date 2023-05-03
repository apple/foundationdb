// Scans the FoundationDB C++ source code for classes that serializable by FDBRPC

#include <cstdlib>

#include <iostream>
#include <iomanip>
#include <string>
#include <vector>

#include <boost/json.hpp>

#include <clang/Tooling/CommonOptionsParser.h>
#include <clang/Tooling/Tooling.h>
#include <llvm/Support/CommandLine.h>

static llvm::cl::OptionCategory toolCategory("Options");

#include <clang/AST/ASTConsumer.h>
#include <clang/AST/ASTContext.h>
#include <clang/AST/RecursiveASTVisitor.h>
#include <clang/ASTMatchers/ASTMatchFinder.h>
#include <clang/Frontend/CompilerInstance.h>

struct SerializableClassInfo {
	struct SerializableClassMemberVariable {
		std::string name;
		std::string type;

		SerializableClassMemberVariable(const std::string& name_, const std::string& type_)
		  : name(name_), type(type_) {}
	};
	std::string className;
	std::string sourceFilePath;
	int lineNumber;
	std::vector<SerializableClassMemberVariable> variables;
	std::string rawSerializeCode;
};

// Pretty print SerializableClassInfo
std::ostream& operator<<(std::ostream& ostream, const SerializableClassInfo& serializableClassInfo) {
	ostream << "[" << serializableClassInfo.className << "]  (" << serializableClassInfo.sourceFilePath << ":"
	        << serializableClassInfo.lineNumber << ")" << std::endl;
	std::cout << "Variables: " << serializableClassInfo.variables.size() << std::endl;
	for (const auto& variable : serializableClassInfo.variables) {
		std::cout << "  " << std::setw(20) << variable.name << "  " << variable.type << std::endl;
	}

	return ostream;
}

std::string toJson(const SerializableClassInfo& classInfo) {
	using namespace boost::json;
	object jsonObject;
	jsonObject.emplace("className", classInfo.className);
	jsonObject.emplace("sourceFilePath", classInfo.sourceFilePath);
	jsonObject.emplace("lineNumber", classInfo.lineNumber);

	array variables;
	for (const auto& var_ : classInfo.variables) {
		object variable;
		variable.emplace("name", var_.name);
		variable.emplace("type", var_.type);
		variables.push_back(variable);
	}

	jsonObject.emplace("variables", variables);
	jsonObject.emplace("raw", classInfo.rawSerializeCode);

	return serialize(jsonObject);
}

std::vector<SerializableClassInfo> knownClasses;

namespace {
using namespace clang::ast_matchers;

DeclarationMatcher serializableClassMatcher =
    cxxRecordDecl(has(functionTemplateDecl(has(templateTypeParmDecl().bind("archiverType")),
                                           has(cxxMethodDecl(hasName("serialize"),
                                                             parameterCountIs(1),
                                                             hasParameter(0, parmVarDecl().bind("archiver")),
                                                             has(compoundStmt().bind("serializeFuncBody")))
                                                   .bind("serializeFunc")))))
        .bind("serializableClass");

} // namespace

class SerializableClassMemberVariableCollector
  : public clang::RecursiveASTVisitor<SerializableClassMemberVariableCollector> {
	using Base_t = clang::RecursiveASTVisitor<SerializableClassMemberVariableCollector>;
	SerializableClassInfo& classInfo;

public:
	SerializableClassMemberVariableCollector(SerializableClassInfo& classInfo_) : Base_t(), classInfo(classInfo_) {}

	bool VisitFieldDecl(clang::FieldDecl* fieldDecl) {
		std::string variableName = fieldDecl->getDeclName().getAsString();
		std::string variableType = fieldDecl->getType().getAsString();
		classInfo.variables.emplace_back(variableName, variableType);
		return true;
	}
};

class SerializableClassMatchCallback : public clang::ast_matchers::MatchFinder::MatchCallback {
	bool checkParameterType(const clang::ast_matchers::BoundNodes& nodes) {
		const auto* templateParm = nodes.getNodeAs<clang::TemplateTypeParmDecl>("archiverType");
		const auto* paramVar = nodes.getNodeAs<clang::ParmVarDecl>("archiver");

		auto templateName = templateParm->getDeclName().getAsString();
		auto paramVarType = paramVar->getType();
		/* This check is only valid in LLVM 16
		if (!paramVarType.isReferenceable()) {
		    // Not Ar&
		    return false;
		} */

		return templateName == paramVarType.getNonReferenceType().getAsString();
	}

	void initializeClassInfo(SerializableClassInfo& classInfo,
	                         const clang::CXXRecordDecl* classDecl,
	                         const clang::SourceManager* sourceManager) {
		classInfo.className = classDecl->getDeclName().getAsString();
		const auto location = classDecl->getLocation();
		const auto sourceFile = sourceManager->getFilename(location);
		classInfo.sourceFilePath = sourceFile.str();
		const auto sourceFileLineNumber = sourceManager->getSpellingLineNumber(location);
		classInfo.lineNumber = sourceFileLineNumber;
	}

	void tryParseSerializeFuncBody(const clang::CompoundStmt* serializeFuncBody,
	                               SerializableClassInfo& classInfo,
	                               clang::SourceManager* sourceManager) {
		// At this stage, we will just store the body of the serialize code
		if (serializeFuncBody == nullptr) {
			// Defined somewhere else
			classInfo.rawSerializeCode = "[Not found]";
			return;
		}
		// NOTE: In the current implementation of libtooling (16.0.1 and 15.0), the sourceManager->getCharacterData
		// would fail on CXXMethodDecl with a macro expansion, e.g. in file A
		//     template<typename Ar> force_inline void serialize(Ar& ar) {}
		// where force_inline is in another file B. In this case `begin` will points to B while end will point to A,
		// which confuses std::string. Hence we care the child node SerializeFuncBody
		auto begin = serializeFuncBody->getBeginLoc();
		auto end_ = serializeFuncBody->getEndLoc();
		auto end = clang::Lexer::getLocForEndOfToken(end_, 0, *sourceManager, clang::LangOptions());
		auto pBegin = sourceManager->getCharacterData(begin);
		auto pEnd = sourceManager->getCharacterData(end);

		classInfo.rawSerializeCode = std::string(pBegin, pEnd - pBegin);
	}

public:
	virtual void run(const clang::ast_matchers::MatchFinder::MatchResult& result) override {
		const clang::CXXRecordDecl* classDecl = result.Nodes.getNodeAs<clang::CXXRecordDecl>("serializableClass");
		if (classDecl == nullptr) {
			return;
		}
		if (!checkParameterType(result.Nodes)) {
			return;
		}

		SerializableClassInfo classInfo;
		initializeClassInfo(classInfo, classDecl, result.SourceManager);

		SerializableClassMemberVariableCollector memberVariableCollector(classInfo);
		// XXX: Understand why const_cast is required, and figure out if there is a better way of doing this
		memberVariableCollector.TraverseCXXRecordDecl(const_cast<clang::CXXRecordDecl*>(classDecl));

		const auto* serializeFuncBody = result.Nodes.getNodeAs<clang::CompoundStmt>("serializeFuncBody");
		tryParseSerializeFuncBody(serializeFuncBody, classInfo, result.SourceManager);

		std::cout << toJson(classInfo) << std::endl;

		return;
	}
};

// Intended to leak the memory
clang::ast_matchers::MatchFinder* getSerializableClassMatchFinder() {
	auto* matchFinder = new clang::ast_matchers::MatchFinder();

	matchFinder->addMatcher(traverse(clang::TK_IgnoreUnlessSpelledInSource, serializableClassMatcher),
	                        new SerializableClassMatchCallback());

	return matchFinder;
}

// Entry point
int main(int argc, const char* argv[]) {
	auto expectedParser = clang::tooling::CommonOptionsParser::create(argc, argv, toolCategory);
	if (!expectedParser) {
		llvm::errs() << expectedParser.takeError();
		return EXIT_FAILURE;
	}
	auto& optionsParser = expectedParser.get();
	clang::tooling::ClangTool tool(optionsParser.getCompilations(), optionsParser.getSourcePathList());

	const auto retVal = tool.run(clang::tooling::newFrontendActionFactory(getSerializableClassMatchFinder()).get());

	return retVal;
}
