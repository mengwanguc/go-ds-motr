
import json
import argparse
import io
import logging
import simplejson
import sys
from subprocess import PIPE, Popen
from typing import Any, Dict, List, NamedTuple, Optional

from consul import Consul, ConsulException
from requests.exceptions import RequestException
from urllib3.exceptions import HTTPError

Pool = NamedTuple('Pool', [('fid', str), ('name', str)])

def kv_item(cns: Consul, key: str, recurse: bool = False) -> Any:
    """Gets Consul KV item.
    May throw HAConsistencyException in case of an intermittent connection
    error or while Consul is re-electing internal Raft leader.
    The _value_ returned is for the specified key, or if `recurse` is True
    a list of _values_ for all keys with the given prefix is returned.
    Each _value_ looks like this:
    ```
    {
      "CreateIndex": 100,
      "ModifyIndex": 200,
      "LockIndex": 200,
      "Key": "foo",
      "Flags": 0,
      "Value": "bar",
      "Session": "adf4238a-882b-9ddc-4a9d-5b6758e4159e"
    }
    ```
    Returns None if the requested `key` does not exists.
    """
    try:
        # See https://python-consul.readthedocs.io/en/latest/#consul-kv
        val = cns.kv.get(key, recurse=recurse)[1]
        assert val is None or recurse == (type(val) is list)
        return val
    except (ConsulException, HTTPError, RequestException) as e:
        raise HAConsistencyException('Could not access Consul KV') from e



def kv_value_as_str(cns: Consul, key: str) -> Optional[str]:
    item = kv_item(cns, key)
    return None if item is None else item['Value'].decode()



def get_fs_stats(cns: Consul) -> Any:
    stats = kv_value_as_str(cns, 'stats/filesystem')
    return {'stats': {}} if stats is None else json.loads(stats)

def sns_pools(cns: Consul) -> List[Pool]:
    return [Pool(fid=x['Key'].split('/')[-1], name=x['Value'].decode())
            for x in kv_item(cns, 'm0conf/pools/', recurse=True)]


cns = Consul()
stats = get_fs_stats(cns)
print(stats)
