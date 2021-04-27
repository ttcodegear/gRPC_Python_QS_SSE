from concurrent import futures
import grpc
import ServerSideExtension_pb2 as SSE
import ServerSideExtension_pb2_grpc

import numpy

class ExtensionService(ServerSideExtension_pb2_grpc.ConnectorServicer):
  def __init__(self):
    pass

  # Function Name | Function Type  | Argument     | TypeReturn Type
  # ScriptEval    | Scalar, Tensor | Numeric      | Numeric
  # ScriptEvalEx  | Scalar, Tensor | Dual(N or S) | Numeric
  @staticmethod
  def ScriptEval(header, request_iterator, context):
    print('script=' + header.script)
    # パラメータがあるか否かをチェック
    if header.params:
      for bundled_rows in request_iterator:
        all_args = []
        for row in bundled_rows.rows:
          script_args = []
          for param, dual in zip(header.params, row.duals):
            if param.dataType == SSE.NUMERIC or param.dataType == SSE.DUAL:
              script_args.append(dual.numData)
            else:
              script_args.append(dual.strData)
          print('args=', script_args)
          all_args.append(script_args)
        all_results = []
        for script_args in all_args:
          result = eval(header.script, {'args': script_args, 'numpy': numpy})
          all_results.append(result)
        response_rows = []
        for result in all_results:
          if isinstance(result, str) or not hasattr(result, '__iter__'):
            duals = iter([SSE.Dual(numData=float(result))])
            response_rows.append(SSE.Row(duals=duals))
          else:
            for row in result:
              duals = iter([SSE.Dual(numData=float(row))])
              response_rows.append(SSE.Row(duals=duals))
        yield SSE.BundledRows(rows=response_rows)
    else:
      script_args = []
      result = eval(header.script, {'args': script_args, 'numpy': numpy})
      if isinstance(result, str) or not hasattr(result, '__iter__'):
        duals = iter([SSE.Dual(numData=float(result))])
        yield SSE.BundledRows(rows=[SSE.Row(duals=duals)])
      else:
        response_rows = []
        for row in result:
          duals = iter([SSE.Dual(numData=float(row))])
          response_rows.append(SSE.Row(duals=duals))
        yield SSE.BundledRows(rows=response_rows)

  # Function Name   | Function Type | Argument     | TypeReturn Type
  # ScriptAggrStr   | Aggregation   | String       | String
  # ScriptAggrExStr | Aggregation   | Dual(N or S) | String
  @staticmethod
  def ScriptAggrStr(header, request_iterator, context):
    print('script=' + header.script)
    # パラメータがあるか否かをチェック
    if header.params:
      all_args = []
      for bundled_rows in request_iterator:
        for row in bundled_rows.rows:
          script_args = []
          for param, dual in zip(header.params, row.duals):
            if param.dataType == SSE.STRING or param.dataType == SSE.DUAL:
              script_args.append(dual.strData)
            else:
              script_args.append(dual.numData)
          all_args.append(script_args)
      print('args=', all_args)
      result = ''
      try:
        result = eval(header.script, {'args': all_args, 'numpy': numpy})
      except Exception as e:
        print(e)
      if isinstance(result, str) or not hasattr(result, '__iter__'):
        duals = iter([SSE.Dual(strData=str(result))])
        yield SSE.BundledRows(rows=[SSE.Row(duals=duals)])
      else:
        response_rows = []
        for row in result:
          duals = iter([SSE.Dual(strData=str(row))])
          response_rows.append(SSE.Row(duals=duals))
        yield SSE.BundledRows(rows=response_rows)
    else:
      script_args = []
      result = ''
      try:
        result = eval(header.script, {'args': script_args, 'numpy': numpy})
      except Exception as e:
        print(e)
      if isinstance(result, str) or not hasattr(result, '__iter__'):
        duals = iter([SSE.Dual(strData=str(result))])
        yield SSE.BundledRows(rows=[SSE.Row(duals=duals)])
      else:
        response_rows = []
        for row in result:
          duals = iter([SSE.Dual(strData=str(row))])
          response_rows.append(SSE.Row(duals=duals))
        yield SSE.BundledRows(rows=response_rows)

  # https://github.com/qlik-oss/server-side-extension/blob/master/docs/writing_a_plugin.md#script-evaluation
  @staticmethod
  def GetFunctionName(header):
    func_type = header.functionType
    arg_types = [param.dataType for param in header.params]
    ret_type  = header.returnType
    '''
    if (func_type == SSE.SCALAR) or (func_type == SSE.TENSOR):
      print('func_type SCALAR TENSOR')
    elif func_type == SSE.AGGREGATION:
      print('func_type AGGREGATION')
    
    if not arg_types:
      print('arg_type Empty')
    elif all(arg_type == SSE.NUMERIC for arg_type in arg_types):
      print('arg_type NUMERIC')
    elif all(arg_type == SSE.STRING for arg_type in arg_types):
      print('arg_type STRING')
    elif (len(set(arg_types)) >= 2) or all(arg_type == SSE.DUAL for arg_type in arg_types):
      print('arg_type DUAL')
    
    if ret_type == SSE.NUMERIC:
      print('ret_type NUMERIC')
    elif ret_type == SSE.STRING:
      print('ret_type STRING')
    '''
    if (func_type == SSE.SCALAR) or (func_type == SSE.TENSOR):
      if (not arg_types) or all(arg_type == SSE.NUMERIC for arg_type in arg_types):
        if ret_type == SSE.NUMERIC:
          return 'ScriptEval'

    if (func_type == SSE.SCALAR) or (func_type == SSE.TENSOR):
      if (len(set(arg_types)) >= 2) or all(arg_type == SSE.DUAL for arg_type in arg_types):
        if ret_type == SSE.NUMERIC:
          return 'ScriptEvalEx'

    if func_type == SSE.AGGREGATION:
      if (not arg_types) or all(arg_type == SSE.STRING for arg_type in arg_types):
        if ret_type == SSE.STRING:
          return 'ScriptAggrStr'

    if func_type == SSE.AGGREGATION:
      if (len(set(arg_types)) >= 2) or all(arg_type == SSE.DUAL for arg_type in arg_types):
        if ret_type == SSE.STRING:
          return 'ScriptAggrExStr'

    return 'Unsupported Function Name'

  def EvaluateScript(self, request_iterator, context):
    print('EvaluateScript')
    md = (('qlik-cache', 'no-store'),) # Disable caching
    context.send_initial_metadata(md)

    metadata = dict(context.invocation_metadata())
    header = SSE.ScriptRequestHeader()
    header.ParseFromString(metadata['qlik-scriptrequestheader-bin'])
    func_name = self.GetFunctionName(header)
    if func_name == 'ScriptEval' or func_name == 'ScriptEvalEx':
      return self.ScriptEval(header, request_iterator, context)
    if func_name == 'ScriptAggrStr' or func_name == 'ScriptAggrExStr':
      return self.ScriptAggrStr(header, request_iterator, context)

    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetCapabilities(self, request, context):
    print('GetCapabilities')
    capabilities = SSE.Capabilities(allowScript=True,
                                    pluginIdentifier='Simple SSE Test',
                                    pluginVersion='v0.0.1')
    return capabilities

if __name__ == '__main__':
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  ServerSideExtension_pb2_grpc.add_ConnectorServicer_to_server(
                                                ExtensionService(), server)
  server.add_insecure_port('[::]:50053')
  server.start()
  try:
    server.wait_for_termination()
  except KeyboardInterrupt:
    server.stop(0)
