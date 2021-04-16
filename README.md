# gRPC_Python_QS_SSE
$>python3 --version

  Python 3.7.3

$>sudo apt install python3-pip

$>pip3 install grpcio

$>pip3 install grpcio-tools

$>pip3 list

  grpcio        1.36.1

  grpcio-tools  1.36.1

  protobuf      3.15.6

$>python3 -m grpc_tools.protoc --proto_path=[search_path] --python_out=[protobuf_path] --grpc_python_out=[grpc_path] proto_file_path

$>ls *_pb2*

  xxxx_pb2.py

  xxxx_pb2_grpc.py





$>python3 -m grpc_tools.protoc --proto_path=./ --python_out=. --grpc_python_out=. ./helloworld.proto

$>python3 greeter_server.py

$>python3 greeter_client.py





$>python3 -m grpc_tools.protoc --proto_path=./ --python_out=. --grpc_python_out=. ./hellostreamingworld.proto

$>python3 greeter_stream_server.py

$>python3 greeter_stream_client.py





$>openssl req -newkey rsa:2048 -nodes -keyout server.key -x509 -days 3650 -out server.crt

$>python3 -m grpc_tools.protoc --proto_path=./ --python_out=. --grpc_python_out=. ./helloworld.proto

$>python3 greeter_ssl_server.py

$>python3 greeter_ssl_client.py





$>pip3 install numpy

$>python3 -m grpc_tools.protoc --proto_path=./ --python_out=. --grpc_python_out=. ./ServerSideExtension.proto

$>python3 SSE_Example.py



C:\Users\[user]\Documents\Qlik\Sense\Settings.ini

------

[Settings 7]

SSEPlugin=Column,localhost:50053



------

