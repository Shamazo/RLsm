import os

print("configuring flatbuffers cmake")
os.chdir("flatbuffers")
cmake = os.popen('cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release')
output = cmake.read()
# print(output)

print("Making flatbuffers")
make = os.popen("make all")
output = make.read()
# print(output)

print("Generating flatbuffers for python")
python_buffers = os.popen("./flatc --python -o ../systemtesting/ ../libmessages/flatbuffers/api.fbs --gen-all")

output = python_buffers.read()
# print(output)

print("generating flatbuffers for rust")
rust_buffers = os.popen("./flatc --rust -o ../libmessages/src/ ../libmessages/flatbuffers/api.fbs --gen-all")
output = rust_buffers.read()
# print(output)
