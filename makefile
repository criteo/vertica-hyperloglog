SDK_HOME?=/opt/vertica/sdk
CXX?=g++
CXXFLAGS:=$(CXXFLAGS) -I $(SDK_HOME)/include -I HelperLibraries -g -Wall -Wno-unused-value -shared -fPIC -O3
## BUILD_DIR?=$(abspath build)
BUILD_DIR?=/tmp/build
BUILD_TMPDIR?=$(BUILD_DIR)/tmp

all: HllLib

$(BUILD_DIR)/.exists:
	test -d $(BUILD_DIR) || mkdir -p $(BUILD_DIR)
	touch $(BUILD_DIR)/.exists

$(BUILD_TMPDIR):
	mkdir -p $(BUILD_TMPDIR)

HllLib: $(BUILD_DIR)/HllLib.so

$(BUILD_DIR)/HllLib.so: src/main/cpp/hlllib/*.cpp $(SDK_HOME)/include/Vertica.cpp $(SDK_HOME)/include/BuildInfo.h $(BUILD_DIR)/.exists
	$(CXX) $(CXXFLAGS) -o $@ src/main/cpp/hlllib/*.cpp $(SDK_HOME)/include/Vertica.cpp

install: HllLib
	cp src/main/cpp/hlllib/install.sql ${BUILD_DIR}/install_HllLib.sql
	/opt/vertica/bin/vsql -U dbadmin -f $(BUILD_DIR)/install_HllLib.sql    

clean:
	rm -rf $(BUILD_TMPDIR)
	rm -f $(BUILD_DIR)/*.so 
	rm -f $(BUILD_DIR)/*.jar
	rm -rf $(BUILD_DIR)/Java*
	-rmdir $(BUILD_DIR) >/dev/null 2>&1 || true
