/*
 * Copyright (c) 2014-2016, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#include "nvwal_test_common.hpp"

#include <execinfo.h>
#include <errno.h>
#include <signal.h>
#include <tinyxml2.h>
#include <unistd.h>
#include <valgrind.h>

#include <fstream>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <boost/filesystem.hpp>

#include "nvwal_api.h"
#include "nvwal_stacktrace.hpp"
#include "nvwal_types.h"

namespace nvwaltest {
nvwal_error_t TestContext::init_all() {
  std::string random_name = get_random_name();
  boost::filesystem::path root_path = boost::filesystem::system_complete(random_name);
  unique_root_path_ = root_path.string();

  boost::filesystem::remove_all(unique_root_path_);

  if (!boost::filesystem::create_directories(unique_root_path_)) {
    std::cerr << "TestContext::init_all() : Fatal! failed to create the folder:"
       << unique_root_path_ << ". Check permissions etc." << std::endl;
    return ENOENT;
  }

  wal_instances_.resize(wal_count_);

  // TODO(Hideaki) : following must be based on sizing_
  const uint64_t kWriterBufferSize = 1ULL << 12;
  const uint16_t kWriterCount = 2;
  const uint32_t kBlockSegSize = 1U << 12;
  const uint32_t kNvSegSize = 1U << 12;
  const uint64_t kNvQuota = 1ULL << 20;

  uint64_t total_buffer_size = wal_count_ * kWriterBufferSize * kWriterCount;
  writer_buffer_memory_.reset(new char[total_buffer_size]);
  if (writer_buffer_memory_.get() == nullptr) {
    std::cerr << "TestContext::init_all() : Fatal! failed to allocate writer buffers"
       << total_buffer_size << " bytes. Check memory availability." << std::endl;
    return ENOMEM;
  }


  nvwal_byte_t* cur_buffer_pos = reinterpret_cast<nvwal_byte_t*>(writer_buffer_memory_.get());
  for (int w = 0; w < wal_count_; ++w) {
    auto* wal = get_wal(w);
    std::memset(wal, 0, sizeof(*wal));

    std::string w_str = std::to_string(w);
    boost::filesystem::path wal_root = root_path;
    wal_root /= w_str;
    if (!boost::filesystem::create_directory(wal_root)) {
      std::cerr << "TestContext::init_all() : Fatal! failed to create the folder:"
        << wal_root.string() << ". Check permissions etc." << std::endl;
      return ENOENT;
    }

    NvwalConfig config;
    std::memset(&config, 0, sizeof(config));
    // Both disk_root and nv_root are wal_root.
    std::memcpy(config.disk_root_, wal_root.string().data(), wal_root.string().length());
    std::memcpy(config.nv_root_, wal_root.string().data(), wal_root.string().length());
    config.block_seg_size_ = kBlockSegSize;
    config.numa_domain_ = w;
    config.nv_quota_ = kNvQuota;
    config.nv_seg_size_ = kNvSegSize;
    config.writer_buffer_size_ = kWriterBufferSize;
    for (uint16_t wr = 0; wr < kWriterCount; ++wr) {
      config.writer_buffers_[wr] = cur_buffer_pos;
      cur_buffer_pos += kWriterBufferSize;
    }
    config.writer_count_ = kWriterCount;

    auto ret = nvwal_init(&config, wal);
    if (ret) {
      std::cerr << "TestContext::init_all() : Fatal! failed to initialize WAL instance-"
        << w << ". errno=" << ret << std::endl;
      return ret;
    }
  }

  return 0;
}

nvwal_error_t TestContext::uninit_all() {
  nvwal_error_t last_error = 0;
  for (int w = 0; w < wal_count_; ++w) {
    auto* wal = get_wal(w);
    // Here, we assume nvwal_uninit is idempotent.
    auto ret = nvwal_uninit(wal);
    if (last_error) {
      last_error = ret;
    }
  }
  boost::filesystem::remove_all(unique_root_path_);
  writer_buffer_memory_.reset(nullptr);
  return last_error;
}

std::string TestContext::get_random_name() {
  // In this unittest suite, we are lazy.
  // We just use process ID.
  // We never concurrently run multiple testcases from the same process,
  // so this should be safe.
  const char* kHexChars = "0123456789abcdef";
  uint32_t seed32 = ::getpid();
  std::string s("%%%%_%%%%_%%%%_%%%%");
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] == '%') {                 // digit request
      seed32 = ::rand_r(&seed32);
      s[i] = kHexChars[seed32 & 0xf];  // convert to hex digit and replace
    }
  }
  return s;
}

std::string to_signal_name(int sig) {
  switch (sig) {
  case SIGHUP    : return "Hangup (POSIX).";
  case SIGINT    : return "Interrupt (ANSI).";
  case SIGQUIT   : return "Quit (POSIX).";
  case SIGILL    : return "Illegal instruction (ANSI).";
  case SIGTRAP   : return "Trace trap (POSIX).";
  case SIGABRT   : return "Abort (ANSI).";
  case SIGBUS    : return "BUS error (4.2 BSD).";
  case SIGFPE    : return "Floating-point exception (ANSI).";
  case SIGKILL   : return "Kill, unblockable (POSIX).";
  case SIGUSR1   : return "User-defined signal 1 (POSIX).";
  case SIGSEGV   : return "Segmentation violation (ANSI).";
  case SIGUSR2   : return "User-defined signal 2 (POSIX).";
  case SIGPIPE   : return "Broken pipe (POSIX).";
  case SIGALRM   : return "Alarm clock (POSIX).";
  case SIGTERM   : return "Termination (ANSI).";
  case SIGSTKFLT : return "Stack fault.";
  case SIGCHLD   : return "Child status has changed (POSIX).";
  case SIGCONT   : return "Continue (POSIX).";
  case SIGSTOP   : return "Stop, unblockable (POSIX).";
  case SIGTSTP   : return "Keyboard stop (POSIX).";
  case SIGTTIN   : return "Background read from tty (POSIX).";
  case SIGTTOU   : return "Background write to tty (POSIX).";
  case SIGURG    : return "Urgent condition on socket (4.2 BSD).";
  case SIGXCPU   : return "CPU limit exceeded (4.2 BSD).";
  case SIGXFSZ   : return "File size limit exceeded (4.2 BSD).";
  case SIGVTALRM : return "Virtual alarm clock (4.2 BSD).";
  case SIGPROF   : return "Profiling alarm clock (4.2 BSD).";
  case SIGWINCH  : return "Window size change (4.3 BSD, Sun).";
  case SIGIO   : return "I/O now possible (4.2 BSD).";
  case SIGPWR    : return "Power failure restart (System V).";
  case SIGSYS    : return "Bad system call.";
  default:
    return "UNKNOWN";
  }
}
std::string gtest_xml_path;
std::string gtest_individual_test;
std::string gtest_test_case_name;
std::string generate_failure_xml(const std::string& type, const std::string& details) {
  // The XML must be in JUnit format
  // https://svn.jenkins-ci.org/trunk/hudson/dtkit/dtkit-format/dtkit-junit-model/src/main/resources/com/thalesgroup/dtkit/junit/model/xsd/junit-4.xsd
  // http://windyroad.com.au/dl/Open%20Source/JUnit.xsd
  tinyxml2::XMLDocument doc;
  tinyxml2::XMLElement* root = doc.NewElement("testsuites");
  root->SetAttribute("name", "AllTests");
  root->SetAttribute("tests", 1);
  root->SetAttribute("failures", 1);
  root->SetAttribute("errors", 0);
  root->SetAttribute("time", 0);
  doc.InsertFirstChild(root);

  tinyxml2::XMLElement* suite = doc.NewElement("testsuite");
  suite->SetAttribute("name", gtest_test_case_name.c_str());
  suite->SetAttribute("tests", 1);
  suite->SetAttribute("failures", 1);
  suite->SetAttribute("errors", 0);
  suite->SetAttribute("disabled", 0);
  suite->SetAttribute("time", 0);
  root->InsertFirstChild(suite);

  tinyxml2::XMLElement* testcase = doc.NewElement("testcase");
  testcase->SetAttribute("name", gtest_individual_test.c_str());
  testcase->SetAttribute("status", "run");
  testcase->SetAttribute("classname", gtest_test_case_name.c_str());
  testcase->SetAttribute("time", 0);
  suite->InsertFirstChild(testcase);

  tinyxml2::XMLElement* test = doc.NewElement("failure");
  test->SetAttribute("type", type.c_str());
  test->SetAttribute("message", details.c_str());
  testcase->InsertFirstChild(test);

  tinyxml2::XMLPrinter printer;
  doc.Print(&printer);
  return printer.CStr();
}
std::string generate_failure_xml(int sig, const std::string& details) {
  return generate_failure_xml(to_signal_name(sig), details);
}
static void handle_signals(int sig, siginfo_t* si, void* /*unused*/) {
  std::stringstream str;
  str << "================================================================" << std::endl;
  str << "====   SIGNAL Received While Running Testcase" << std::endl;
  str << "====   SIGNAL Code=" << sig << "("<< to_signal_name(sig) << ")" << std::endl;
  str << "====   At address=" << si->si_addr << std::endl;
  str << "================================================================" << std::endl;

  std::vector<std::string> traces = get_backtrace(true);

  str << "=== Stack frame (length=" << traces.size() << ")" << std::endl;
  for (uint16_t i = 0; i < traces.size(); ++i) {
    str << "- [" << i << "/" << traces.size() << "] " << traces[i] << std::endl;
  }

  std::string details = str.str();
  std::cerr << details;

  if (gtest_xml_path.size() == 0) {
    std::cerr << "XML Output file was not specified, so we exit as a usual crash" << std::endl;
    ::exit(1);
  } else {
    std::cerr << "Converting the signal to a testcase failure in " << gtest_xml_path << std::endl;
    // We report this error in the result XML.
    std::string xml = generate_failure_xml(sig, details);
    std::cerr << "Xml content: " << std::endl << xml << std::endl;

    std::ofstream out;
    out.open(gtest_xml_path, std::ios_base::out | std::ios_base::trunc);
    if (!out.is_open()) {
      std::cerr << "Couldn't open xml file. os_error= " << errno << std::endl;
      ::exit(1);
    }
    out << xml;
    out.flush();
    out.close();
    std::cerr << "Wrote out result xml file. Now exitting.." << std::endl;
    ::exit(1);
  }
}
void register_signal_handlers(
  const char* test_case_name,
  int argc,
  char** argv) {
  std::cout << "****************************************************************" << std::endl;
  std::cout << "*****  Started NVWAL Unit Testcase " << std::endl;
  std::cout << "*****  Testcase name: " << test_case_name << std::endl;
  std::cout << "*****  Arguments (argc=" << argc << "): " << std::endl;

  gtest_test_case_name = test_case_name;
  gtest_xml_path = "";
  gtest_individual_test = "";
  for (int i = 0; i < argc; ++i) {
    std::cout << "*****    argv[" << i << "]: " << argv[i] << std::endl;
    std::string str(argv[i]);
    if (str.find("--gtest_output=xml:") == 0) {
      gtest_xml_path = str.substr(std::string("--gtest_output=xml:").size());
    } else if (str.find("--gtest_filter=*.") == 0) {
      gtest_individual_test = str.substr(std::string("--gtest_filter=*.").size());
    }
  }
  if (gtest_xml_path.size() > 0) {
    std::cout << "*****  XML Output: " << gtest_xml_path << std::endl;
  } else {
    std::cout << "*****  XML Output file was not specified. Executed manually?" << std::endl;
  }
  if (gtest_individual_test.size() > 0) {
    std::cout << "*****  Running an individual test: " << gtest_individual_test << std::endl;
  } else {
    std::cout << "*****  Individual test was not specified. Executed manually?" << std::endl;
  }
  std::cout << "****************************************************************" << std::endl;

  struct sigaction sa;
  sa.sa_flags = SA_SIGINFO;
  ::sigemptyset(&sa.sa_mask);
  sa.sa_sigaction = handle_signals;

  // we do not capture all signals. Only the followings are considered as 'expected'
  // testcase failures.
  ::sigaction(SIGABRT, &sa, nullptr);
  ::sigaction(SIGBUS, &sa, nullptr);
  ::sigaction(SIGFPE, &sa, nullptr);
  ::sigaction(SIGSEGV, &sa, nullptr);
  // Not surprisingly, SIGKILL/SIGSTOP cannot be captured:
  //  http://man7.org/linux/man-pages/man2/sigaction.2.html
  // This means we cannot capture timeout-kill by ctest which uses STOP (see kwsys/ProcessUNIX.c).
  // as we ignore exit-code of ctest in jenkins, this means timeout is silent. mm...
  // As a compromise, we pre-populate result xml as follows.
}

void pre_populate_error_result_xml() {
  if (gtest_xml_path.size() > 0) {
    std::string xml = generate_failure_xml(
      std::string("Pre-populated Error. Test timeout happned?"),
      std::string("This is an initially written gtest xml before test execution."
      " If you are receiving this result, most likely the process has disappeared without trace."
      " This can happen when ctest kills the process via SIGSTOP, or someone killed the process"
      " via SIGKILL, etc."));

    std::ofstream out;
    out.open(gtest_xml_path, std::ios_base::out | std::ios_base::trunc);
    if (out.is_open()) {
      out << xml;
      out.flush();
      out.close();
    }
  }
}
}  // namespace nvwaltest
