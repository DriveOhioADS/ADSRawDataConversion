PY_PROTOS := \
proto/record.proto \
proto/proto_desc.proto

PBPYS := $(PY_PROTOS:%.proto=%_pb2.py)

cyberreader-py: $(PBPYS)

%_pb2.py: %.proto
	@mkdir -p apollopy
	@protoc $< --python_out=apollopy

deps: protobuf

# if protoc specific version is needed
# otherwise

protobuf:
	sudo apt install -y protobuf-compiler
	sudo apt install -y python3-protobuf
	
# protobuf:
# 	rm -rf protobuf protobuf-cpp-3.3.0.tar.gz
# 	wget -nc https://github.com/google/protobuf/releases/download/v3.3.0/protobuf-cpp-3.3.0.tar.gz
# 	tar xzf protobuf-cpp-3.3.0.tar.gz
# 	mv protobuf-3.3.0 protobuf
# 	(cd protobuf; CXXFLAGS="-std=c++11 -fPIC" ./configure; make -j $(CPUS))

# if the proto files are needed
#apollo:
#	rm -rf apollo v5.5.0.tar.gz
#	wget -nc https://github.com/ApolloAuto/apollo/archive/refs/tags/v5.5.0.tar.gz
#	tar zxvf v5.5.0.tar.gz
#	mv apollo-5.5.0 apollo

