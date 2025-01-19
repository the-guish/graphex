import base64
import json
import zlib

from graphex.model import DAGData


def mermaid_chart(dag: DAGData):
    txt = 'flowchart TD\n'

    for step in dag.steps.values():

        for ip in step.arg_var_names + list(step.string_keyword_args.values()):
            txt += f'    {ip}([{ip}]) --> {step.name}\n'

        for o in step.outputs:
            txt += f'    {step.name} --> {o}([{o}])\n'

    return txt


def js_string_to_byte(data):
    return bytes(data, 'ascii')


def js_bytes_to_string(data):
    return data.decode('ascii')


def js_btoa(data):
    return base64.b64encode(data)


def pako_deflate(data):
    compress = zlib.compressobj(9, zlib.DEFLATED, 15, 8, zlib.Z_DEFAULT_STRATEGY)
    compressed_data = compress.compress(data)
    compressed_data += compress.flush()
    return compressed_data


def mermaid_live_link(graphMarkdown: str):
    jGraph = {
            "code": graphMarkdown,
            "mermaid": {"theme": "default"}
        }
    byteStr = js_string_to_byte(json.dumps(jGraph))
    deflated = pako_deflate(byteStr)
    dEncode = js_btoa(deflated)
    link = 'http://mermaid.live/view#pako:' + js_bytes_to_string(dEncode)
    return link


def mermaid_link_from_dag(dag: DAGData):
    txt = mermaid_chart(dag)
    return mermaid_live_link(txt)
