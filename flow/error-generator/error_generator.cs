using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using System;

namespace error_generator {
    class Error
    {
        public string name { get; set; }
        public string cpp_ident { get { return "error_code_" + name; } }
        public int code { get; set; }
        public string description { get; set; }
        public string comment { get; set; }

        public bool provide_custom_impl = false;
    }

    class Category
    {
        public string name { get; set; }
        public string type { get { return name + "ErrorCode"; } }
        public string error_category {
            get { 
                if (system_error_category)
                    return name;
                return name.ToLower() + "_error_category";
            }
        }
        public bool system_error_category = false;
        public List<Error> errors { get; set; }
    }

    static class error_generator {
        static int Main(string[] args) {
            var code_format = "enum {{ error_code_{0} = {1} }};";
            var impl_format = "inline Error {0}_impl(const char* call_site) {{ return Error({1}, call_site); }}";
            var macr_format = "#define {0}() {0}_impl(CALL_SITE)";

            List<Category> categories = parseCategories(args[0]);
            // we need to make sure that all error codes appear only once. We could do this lazily while
            // generating the file. But we don't want to generate anything if this invariant doesn't hold.
            // Furthermore, efficiency is not of great importance here and the code should be more
            // readable like this.
            var codes = new Dictionary<int, string>();
            var errorFormat = "ERROR: {0} has same code ({1}) as {2}";
            var noDuplicates = true;
            foreach (var category in categories) {
                foreach (var error in category.errors) {
                    if (codes.ContainsKey(error.code)) {
                        Console.Error.WriteLine(String.Format(errorFormat, error.name, error.code, codes[error.code]));
                        noDuplicates = false;
                    }
                }
            }
            if (!noDuplicates) {
                return 1;
            }
            using (var header = File.Open(args[1], FileMode.Create, FileAccess.Write))
            {
                TextWriter outFile = new StreamWriter(header);
                foreach (var category in categories) {
                    foreach (var error in category.errors) {
                        outFile.WriteLine("/**");
                        outFile.WriteLine(" * " + error.description);
                        if (error.comment != null) {
                            outFile.WriteLine(" *");
                            outFile.WriteLine(" * " + error.comment);
                        }
                        outFile.WriteLine("*/");
                        var impl = String.Format(impl_format, error.name, error.code);
                        var code = String.Format(code_format, error.name, error.code);
                        var macro = String.Format(macr_format, error.name);
                        outFile.WriteLine(code);
                        if (!error.provide_custom_impl) {
                            outFile.WriteLine(impl);
                            outFile.WriteLine(macro);
                        }
                        outFile.WriteLine();
                    }
                }
                outFile.Flush();
            }
            using (var codeTable = File.Open(args[2], FileMode.Create, FileAccess.Write)) {
                TextWriter outFile = new StreamWriter(codeTable);
                outFile.WriteLine("#include <flow/Error.h>");
                outFile.WriteLine();
                outFile.WriteLine("ErrorCodeTable::ErrorCodeTable() {");
                var addFormat = "addCode({0}, {1}, {2});";
                foreach (var category in categories) {
                    foreach (var error in category.errors) {
                        var add = String.Format(addFormat, error.code, error.name, error.description);
                    }
                }
                outFile.WriteLine("}");
                outFile.WriteLine();
                outFile.Flush();
            }
            Console.WriteLine("Done");
            return 0;
        }

        private static List<Category> parseCategories(string path)
        {
            var categories = new List<Category>();
            var errorsDoc = XDocument.Load(path).Element("Categories");
            foreach (var categoryDoc in errorsDoc.Elements("Category"))
            {
                var errors = new List<Error>();
                var system_error_category_attr = categoryDoc.Attribute("system_error_category");
                foreach (var errorDoc in categoryDoc.Elements("Error"))
                {
                    var provide_custom_impl_attr = errorDoc.Attribute("no-impl");
                    var commentAttr = errorDoc.Attribute("comment");
                    errors.Add(new Error
                    {
                        name = errorDoc.AttributeNonNull("name"),
                        code = int.Parse(errorDoc.AttributeNonNull("code")),
                        description = errorDoc.AttributeNonNull("description"),
                        comment = commentAttr != null ? commentAttr.Value : null,
                        provide_custom_impl = provide_custom_impl_attr != null && provide_custom_impl_attr.Value.ToLower() == "true",
                    });
                }
                categories.Add(new Category {
                            name = categoryDoc.AttributeNonNull("name"),
                            errors=errors,
                            system_error_category = system_error_category_attr != null && system_error_category_attr.Value.ToLower() == "true",
                        });
            }
            return categories;
        }

        public static string AttributeNonNull(this XElement e, string name)
        {
            var attr = e.Attribute(name);
            if (attr == null)
                throw new Exception(string.Format("Attribute {0} must exist and have a value", name));
            return attr.Value;
        }
    }
}
