// copy_test.go
//
// This source file is part of the FoundationDB open source project
//
// Copyright 2023 Apple Inc. and the FoundationDB project authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"fmt"
	"os"
	"path"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Testing the copy methods", func() {
	When("getting the copy details", func() {
		var copyFiles, copyBinaries, copyLibraries, requiredCopyFiles []string
		var currentContainerVersion, inputDir, copyPrimaryLibrary, binaryOutputDirectory string

		AfterEach(func() {
			copyFiles = []string{}
			copyBinaries = []string{}
			copyLibraries = []string{}
			requiredCopyFiles = []string{}
		})

		BeforeEach(func() {
			currentContainerVersion = "7.1.43"
		})

		When("no files should be copied", func() {
			It("no error should be thrown", func() {
				copyDetails, requiredCopyMap, err := getCopyDetails(inputDir, copyPrimaryLibrary, binaryOutputDirectory, copyFiles, copyBinaries, copyLibraries, requiredCopyFiles, currentContainerVersion, executionModeInit)
				Expect(err).NotTo(HaveOccurred())
				Expect(copyDetails).To(BeEmpty())
				Expect(requiredCopyMap).To(BeEmpty())
			})
		})

		When("the fdbserver binary should be copied", func() {
			When("the binary output directory is not set", func() {
				BeforeEach(func() {
					copyBinaries = []string{"fdbserver"}
				})

				When("the execution mode is init", func() {
					It("no error should be thrown", func() {
						copyDetails, requiredCopyMap, err := getCopyDetails(inputDir, copyPrimaryLibrary, binaryOutputDirectory, copyFiles, copyBinaries, copyLibraries, requiredCopyFiles, currentContainerVersion, executionModeInit)
						Expect(err).NotTo(HaveOccurred())
						Expect(copyDetails).To(HaveKeyWithValue("/usr/bin/fdbserver", "7.1/fdbserver"))
						Expect(copyDetails).To(HaveLen(1))
						Expect(requiredCopyMap).To(BeEmpty())
					})
				})

				When("the execution mode is sidecar", func() {
					It("no error should be thrown", func() {
						copyDetails, requiredCopyMap, err := getCopyDetails(inputDir, copyPrimaryLibrary, binaryOutputDirectory, copyFiles, copyBinaries, copyLibraries, requiredCopyFiles, currentContainerVersion, executionModeSidecar)
						Expect(err).NotTo(HaveOccurred())
						Expect(copyDetails).To(HaveKeyWithValue("/usr/bin/fdbserver", "bin/7.1.43/fdbserver"))
						Expect(copyDetails).To(HaveLen(1))
						Expect(requiredCopyMap).To(BeEmpty())
					})
				})
			})

			When("the binary output directory is set", func() {
				BeforeEach(func() {
					copyBinaries = []string{"fdbserver"}
					binaryOutputDirectory = "testing"
				})

				AfterEach(func() {
					binaryOutputDirectory = ""
				})

				It("no error should be thrown", func() {
					copyDetails, requiredCopyMap, err := getCopyDetails(inputDir, copyPrimaryLibrary, binaryOutputDirectory, copyFiles, copyBinaries, copyLibraries, requiredCopyFiles, currentContainerVersion, executionModeInit)
					Expect(err).NotTo(HaveOccurred())
					Expect(copyDetails).To(HaveKeyWithValue("/usr/bin/fdbserver", "testing/fdbserver"))
					Expect(copyDetails).To(HaveLen(1))
					Expect(requiredCopyMap).To(BeEmpty())
				})
			})
		})

		When("the fdbserver and fdbbackup binary should be copied", func() {
			BeforeEach(func() {
				copyBinaries = []string{"fdbserver", "fdbbackup"}
			})

			It("no error should be thrown", func() {
				copyDetails, requiredCopyMap, err := getCopyDetails(inputDir, copyPrimaryLibrary, binaryOutputDirectory, copyFiles, copyBinaries, copyLibraries, requiredCopyFiles, currentContainerVersion, executionModeInit)
				Expect(err).NotTo(HaveOccurred())
				Expect(copyDetails).To(
					And(
						HaveKeyWithValue("/usr/bin/fdbserver", "7.1/fdbserver"),
						HaveKeyWithValue("/usr/bin/fdbbackup", "7.1/fdbbackup"),
					),
				)
				Expect(copyDetails).To(HaveLen(2))
				Expect(requiredCopyMap).To(BeEmpty())
			})
		})

		When("libraries should be copied", func() {
			BeforeEach(func() {
				copyLibraries = []string{"7.1.43", "7.3.27"}
			})

			It("no error should be thrown", func() {
				copyDetails, requiredCopyMap, err := getCopyDetails(inputDir, copyPrimaryLibrary, binaryOutputDirectory, copyFiles, copyBinaries, copyLibraries, requiredCopyFiles, currentContainerVersion, executionModeInit)
				Expect(err).NotTo(HaveOccurred())
				Expect(copyDetails).To(
					And(
						HaveKeyWithValue("/usr/lib/fdb/multiversion/libfdb_c_7.1.43.so", "libfdb_c_7.1.43.so"),
						HaveKeyWithValue("/usr/lib/fdb/multiversion/libfdb_c_7.3.27.so", "libfdb_c_7.3.27.so"),
					),
				)
				Expect(copyDetails).To(HaveLen(2))
				Expect(requiredCopyMap).To(BeEmpty())
			})
		})

		When("the fdbserver and fdbbackup binary should be copied", func() {
			BeforeEach(func() {
				copyBinaries = []string{"fdbserver", "fdbbackup"}
			})

			It("no error should be thrown", func() {
				copyDetails, requiredCopyMap, err := getCopyDetails(inputDir, copyPrimaryLibrary, binaryOutputDirectory, copyFiles, copyBinaries, copyLibraries, requiredCopyFiles, currentContainerVersion, executionModeInit)
				Expect(err).NotTo(HaveOccurred())
				Expect(copyDetails).To(
					And(
						HaveKeyWithValue("/usr/bin/fdbserver", "7.1/fdbserver"),
						HaveKeyWithValue("/usr/bin/fdbbackup", "7.1/fdbbackup"),
					),
				)
				Expect(copyDetails).To(HaveLen(2))
				Expect(requiredCopyMap).To(BeEmpty())
			})
		})

		When("libraries and binaries should be copied", func() {
			BeforeEach(func() {
				copyLibraries = []string{"7.1.43", "7.3.27"}
				copyBinaries = []string{"fdbserver", "fdbbackup"}
			})

			It("no error should be thrown", func() {
				copyDetails, requiredCopyMap, err := getCopyDetails(inputDir, copyPrimaryLibrary, binaryOutputDirectory, copyFiles, copyBinaries, copyLibraries, requiredCopyFiles, currentContainerVersion, executionModeInit)
				Expect(err).NotTo(HaveOccurred())
				Expect(copyDetails).To(
					And(
						HaveKeyWithValue("/usr/bin/fdbserver", "7.1/fdbserver"),
						HaveKeyWithValue("/usr/bin/fdbbackup", "7.1/fdbbackup"),
						HaveKeyWithValue("/usr/lib/fdb/multiversion/libfdb_c_7.1.43.so", "libfdb_c_7.1.43.so"),
						HaveKeyWithValue("/usr/lib/fdb/multiversion/libfdb_c_7.3.27.so", "libfdb_c_7.3.27.so"),
					),
				)
				Expect(copyDetails).To(HaveLen(4))
				Expect(requiredCopyMap).To(BeEmpty())
			})
		})

		When("files should be copied", func() {
			BeforeEach(func() {
				copyFiles = []string{"testFile"}
			})

			When("no input directory is set", func() {
				It("no error should be thrown", func() {
					copyDetails, requiredCopyMap, err := getCopyDetails(inputDir, copyPrimaryLibrary, binaryOutputDirectory, copyFiles, copyBinaries, copyLibraries, requiredCopyFiles, currentContainerVersion, executionModeInit)
					Expect(err).NotTo(HaveOccurred())
					Expect(copyDetails).To(
						And(
							HaveKeyWithValue("testFile", ""),
						),
					)
					Expect(copyDetails).To(HaveLen(1))
					Expect(requiredCopyMap).To(BeEmpty())
				})
			})

			When("the input directory is set", func() {
				BeforeEach(func() {
					inputDir = "/testing"
				})

				AfterEach(func() {
					inputDir = ""
				})

				It("no error should be thrown", func() {
					copyDetails, requiredCopyMap, err := getCopyDetails(inputDir, copyPrimaryLibrary, binaryOutputDirectory, copyFiles, copyBinaries, copyLibraries, requiredCopyFiles, currentContainerVersion, executionModeInit)
					Expect(err).NotTo(HaveOccurred())
					Expect(copyDetails).To(
						And(
							HaveKeyWithValue("/testing/testFile", ""),
						),
					)
					Expect(copyDetails).To(HaveLen(1))
					Expect(requiredCopyMap).To(BeEmpty())
				})
			})
		})

		When("the primary fdb library should be copied", func() {
			BeforeEach(func() {
				copyPrimaryLibrary = "7.1.43"
			})

			AfterEach(func() {
				copyPrimaryLibrary = ""
			})

			It("no error should be thrown", func() {
				copyDetails, requiredCopyMap, err := getCopyDetails(inputDir, copyPrimaryLibrary, binaryOutputDirectory, copyFiles, copyBinaries, copyLibraries, requiredCopyFiles, currentContainerVersion, executionModeInit)
				Expect(err).NotTo(HaveOccurred())
				Expect(copyDetails).To(
					And(
						HaveKeyWithValue("/usr/lib/fdb/multiversion/libfdb_c_7.1.43.so", "libfdb_c.so"),
					),
				)
				Expect(copyDetails).To(HaveLen(1))
				Expect(requiredCopyMap).To(BeEmpty())
			})
		})

		When("some files are required not to be empty", func() {
			BeforeEach(func() {
				requiredCopyFiles = []string{"testFile"}
				copyBinaries = []string{"fdbserver"}
				inputDir = "/testing"
			})

			AfterEach(func() {
				inputDir = ""
			})

			When("a file is present in the require not empty list but should not be copied", func() {
				BeforeEach(func() {
					copyFiles = nil
				})

				It("should throw an error", func() {
					copyDetails, requiredCopyMap, err := getCopyDetails(inputDir, copyPrimaryLibrary, binaryOutputDirectory, copyFiles, copyBinaries, copyLibraries, requiredCopyFiles, currentContainerVersion, executionModeInit)
					Expect(err).To(HaveOccurred())
					Expect(copyDetails).To(BeEmpty())
					Expect(requiredCopyMap).To(BeEmpty())
				})
			})

			When("all files are present", func() {
				BeforeEach(func() {
					copyFiles = requiredCopyFiles
				})

				It("should not throw an error and include all files", func() {
					copyDetails, requiredCopyMap, err := getCopyDetails(inputDir, copyPrimaryLibrary, binaryOutputDirectory, copyFiles, copyBinaries, copyLibraries, requiredCopyFiles, currentContainerVersion, executionModeInit)
					Expect(err).NotTo(HaveOccurred())
					Expect(copyDetails).To(
						And(
							HaveKeyWithValue("/usr/bin/fdbserver", "7.1/fdbserver"),
							HaveKeyWithValue("/testing/testFile", ""),
						),
					)
					Expect(copyDetails).To(HaveLen(2))
					Expect(requiredCopyMap).To(HaveLen(1))
					Expect(requiredCopyMap).To(HaveKeyWithValue("/testing/testFile", true))
				})
			})
		})
	})

	When("copying the binaries from the input directory to the output directory", func() {
		var inputBinaryDir, outputBinaryDir string
		binaries := []string{"fdbserver", "fdbbackup", "fdbrestore"}

		BeforeEach(func() {
			inputBinaryDir = GinkgoT().TempDir()
			outputBinaryDir = GinkgoT().TempDir()

			for _, binary := range binaries {
				_, err := os.Create(path.Join(inputBinaryDir, binary))
				Expect(err).NotTo(HaveOccurred())
			}
		})

		When("copying the binaries", func() {
			When("the execution mode is init", func() {
				BeforeEach(func() {
					// Simulate the binary directory
					GinkgoT().Setenv(binaryTestDirectoryEnv, inputBinaryDir)
					copyDetails, _, err := getCopyDetails("", "", "", nil, binaries, nil, nil, "7.1.43", executionModeInit)
					Expect(err).NotTo(HaveOccurred())
					Expect(copyDetails).To(HaveLen(3))
					Expect(copyFiles(GinkgoLogr, outputBinaryDir, copyDetails, map[string]bool{})).NotTo(HaveOccurred())
				})

				It("should copy all the files", func() {
					Expect(path.Join(outputBinaryDir, "7.1", "fdbserver")).Should(BeAnExistingFile())
					Expect(path.Join(outputBinaryDir, "7.1", "fdbrestore")).Should(BeAnExistingFile())
					Expect(path.Join(outputBinaryDir, "7.1", "fdbbackup")).Should(BeAnExistingFile())
				})
			})

			When("the execution mode is sidecar", func() {
				BeforeEach(func() {
					// Simulate the binary directory
					GinkgoT().Setenv(binaryTestDirectoryEnv, inputBinaryDir)
					copyDetails, _, err := getCopyDetails("", "", "", nil, binaries, nil, nil, "7.1.43", executionModeSidecar)
					Expect(err).NotTo(HaveOccurred())
					Expect(copyDetails).To(HaveLen(3))
					Expect(copyFiles(GinkgoLogr, outputBinaryDir, copyDetails, map[string]bool{})).NotTo(HaveOccurred())
				})

				It("should copy all the files", func() {
					Expect(path.Join(outputBinaryDir, "bin", "7.1.43", "fdbserver")).Should(BeAnExistingFile())
					Expect(path.Join(outputBinaryDir, "bin", "7.1.43", "fdbrestore")).Should(BeAnExistingFile())
					Expect(path.Join(outputBinaryDir, "bin", "7.1.43", "fdbbackup")).Should(BeAnExistingFile())
				})
			})

		})
	})

	When("copying the libraries from the input directory to the output directory", func() {
		var inputLibraryDir, outputLibraryDir string
		libraries := []string{"7.1", "7.3", "6.3"}

		BeforeEach(func() {
			inputLibraryDir = GinkgoT().TempDir()
			outputLibraryDir = GinkgoT().TempDir()

			for _, library := range libraries {
				_, err := os.Create(path.Join(inputLibraryDir, fmt.Sprintf("libfdb_c_%s.so", library)))
				Expect(err).NotTo(HaveOccurred())
			}

			// Simulate the binary directory
			GinkgoT().Setenv(libraryTestDirectoryEnv, inputLibraryDir)
		})

		When("copying the libraries without a primary library", func() {
			var copyDetails map[string]string

			BeforeEach(func() {
				var err error
				copyDetails, _, err = getCopyDetails("", "", "", nil, nil, libraries, nil, "7.1.43", executionModeInit)
				Expect(err).NotTo(HaveOccurred())
				Expect(copyDetails).To(HaveLen(3))
				Expect(copyFiles(GinkgoLogr, outputLibraryDir, copyDetails, map[string]bool{})).NotTo(HaveOccurred())
			})

			It("should copy all the files", func() {
				Expect(path.Join(outputLibraryDir, "libfdb_c_7.1.so")).Should(BeAnExistingFile())
				Expect(path.Join(outputLibraryDir, "libfdb_c_6.3.so")).Should(BeAnExistingFile())
				Expect(path.Join(outputLibraryDir, "libfdb_c_7.3.so")).Should(BeAnExistingFile())
			})
		})

		When("copying the libraries with a primary library", func() {
			var copyDetails map[string]string

			BeforeEach(func() {
				var err error
				copyDetails, _, err = getCopyDetails("", "7.1", "", nil, nil, libraries, nil, "7.1.43", executionModeInit)
				Expect(err).NotTo(HaveOccurred())
				Expect(copyDetails).To(HaveLen(3))
				Expect(copyFiles(GinkgoLogr, outputLibraryDir, copyDetails, map[string]bool{})).NotTo(HaveOccurred())
			})

			It("should copy all the files", func() {
				Expect(path.Join(outputLibraryDir, "libfdb_c.so")).Should(BeAnExistingFile())
				Expect(path.Join(outputLibraryDir, "libfdb_c_6.3.so")).Should(BeAnExistingFile())
				Expect(path.Join(outputLibraryDir, "libfdb_c_7.3.so")).Should(BeAnExistingFile())
			})
		})

		When("copying the binaries from the input directory to the output directory", func() {
			var testInputDir, testOutputDir string

			BeforeEach(func() {
				testInputDir = GinkgoT().TempDir()
				testOutputDir = GinkgoT().TempDir()

				_, err := os.Create(path.Join(testInputDir, "testfile"))
				Expect(err).NotTo(HaveOccurred())
			})

			When("copying the file", func() {
				var copyDetails map[string]string

				BeforeEach(func() {
					var err error
					copyDetails, _, err = getCopyDetails(testInputDir, "", "", []string{"testfile"}, nil, nil, nil, "7.1.43", executionModeInit)
					Expect(err).NotTo(HaveOccurred())
					Expect(copyDetails).To(HaveLen(1))
					Expect(copyFiles(GinkgoLogr, testOutputDir, copyDetails, map[string]bool{})).NotTo(HaveOccurred())
				})

				It("should copy the file", func() {
					Expect(path.Join(testOutputDir, "testfile")).Should(BeAnExistingFile())
				})
			})

			When("the file is required to not be empty", func() {
				var copyDetails map[string]string
				var requiredFiles map[string]bool

				BeforeEach(func() {
					var err error
					copyDetails, requiredFiles, err = getCopyDetails(testInputDir, "", "", []string{"testfile"}, nil, nil, []string{"testfile"}, "7.1.43", executionModeInit)
					Expect(err).NotTo(HaveOccurred())
					Expect(copyDetails).To(HaveLen(1))
				})

				When("the file is empty", func() {
					It("should not copy the file", func() {
						Expect(copyFiles(GinkgoLogr, testOutputDir, copyDetails, requiredFiles)).To(HaveOccurred())
						Expect(path.Join(testOutputDir, "testfile")).NotTo(BeAnExistingFile())
					})
				})

				When("the file is not empty", func() {
					BeforeEach(func() {
						Expect(os.WriteFile(path.Join(testInputDir, "testfile"), []byte("Hello World"), 0644)).NotTo(HaveOccurred())
					})

					It("should copy the file", func() {
						Expect(copyFiles(GinkgoLogr, testOutputDir, copyDetails, requiredFiles)).NotTo(HaveOccurred())
						Expect(path.Join(testOutputDir, "testfile")).To(BeAnExistingFile())
					})
				})
			})
		})
	})
})
