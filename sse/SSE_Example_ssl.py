from concurrent import futures
import grpc
import ServerSideExtension_pb2 as SSE
import ServerSideExtension_pb2_grpc

class ExtensionService(ServerSideExtension_pb2_grpc.ConnectorServicer):
  def __init__(self):
    pass

  @staticmethod
  def SumOfColumn(request_iterator, context):
    print('SumOfColumn')
    params = []
    for bundled_rows in request_iterator:
      for row in bundled_rows.rows:
        param = [d.numData for d in row.duals][0] # row=[Col1]
        params.append(param)
    result = sum(params) # Col1 + Col1 + ...
    duals = iter([SSE.Dual(numData=result)])
    yield SSE.BundledRows(rows=[SSE.Row(duals=duals)])

  @staticmethod
  def SumOfRows(request_iterator, context):
    print('SumOfRows')
    for bundled_rows in request_iterator:
      response_rows = []
      for row in bundled_rows.rows:
        params = [d.numData for d in row.duals] # row=[Col1,Col2]
        result = sum(params) # Col1 + Col2
        duals  = iter([SSE.Dual(numData=result)])
        response_rows.append(SSE.Row(duals=duals))
      yield SSE.BundledRows(rows=response_rows)

  @staticmethod
  def GetFunctionId(context):
    # Read gRPC metadata
    metadict = dict(context.invocation_metadata())
    header = SSE.FunctionRequestHeader()
    header.ParseFromString(metadict['qlik-functionrequestheader-bin'])
    return header.functionId

  def ExecuteFunction(self, request_iterator, context):
    print('ExecuteFunction')
    md = (('qlik-cache', 'no-store'),) # Disable caching
    context.send_initial_metadata(md)

    func_id = self.GetFunctionId(context)
    if func_id == 0:
      return self.SumOfColumn(request_iterator, context)
    elif func_id == 1:
      return self.SumOfRows(request_iterator, context)
    else:
      context.set_code(grpc.StatusCode.UNIMPLEMENTED)
      context.set_details('Method not implemented!')
      raise NotImplementedError('Method not implemented!')

  def GetCapabilities(self, request, context):
    print('GetCapabilities')
    capabilities = SSE.Capabilities(allowScript=False,
                                    pluginIdentifier='Simple SSE Test',
                                    pluginVersion='v0.0.1')
    # SumOfColumn
    func0 = capabilities.functions.add()
    func0.functionId = 0                   # 関数ID
    func0.name = 'SumOfColumn'             # 関数名
    func0.functionType = SSE.AGGREGATION   # 関数タイプ=0=スカラー,1=集計,2=テンソル
    func0.returnType   = SSE.NUMERIC       # 関数戻り値=0=文字列,1=数値,2=Dual
    func0.params.add(name='col1',          # パラメータ名
                     dataType=SSE.NUMERIC) # パラメータタイプ=0=文字列,1=数値,2=Dual
    # SumOfRows
    func1 = capabilities.functions.add()
    func1.functionId   = 1                 # 関数ID
    func1.name = 'SumOfRows'               # 関数名
    func1.functionType = SSE.TENSOR        # 関数タイプ=0=スカラー,1=集計,2=テンソル
    func1.returnType = SSE.NUMERIC         # 関数戻り値=0=文字列,1=数値,2=Dual
    func1.params.add(name='col1',          # パラメータ名
                     dataType=SSE.NUMERIC) # パラメータタイプ=0=文字列,1=数値,2=Dual
    func1.params.add(name='col2',          # パラメータ名
                     dataType=SSE.NUMERIC) # パラメータタイプ=0=文字列,1=数値,2=Dual
    return capabilities

if __name__ == '__main__':
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  ServerSideExtension_pb2_grpc.add_ConnectorServicer_to_server(
                                                ExtensionService(), server)
  with open('./sse_server_key.pem', 'rb') as f:
    SERVER_CERT_KEY = f.read()
  with open('./sse_server_cert.pem', 'rb') as f:
    SERVER_CERT = f.read()
  with open('./root_cert.pem', 'rb') as f:
    ROOT_CERT = f.read()
  server_credentials = grpc.ssl_server_credentials(
                            [(SERVER_CERT_KEY, SERVER_CERT)], ROOT_CERT, True)
  server.add_secure_port('[::]:50053', server_credentials)
  server.start()
  try:
    server.wait_for_termination()
  except KeyboardInterrupt:
    server.stop(0)
