#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
from typing import Optional

from skywalking import Layer, Component, config
from skywalking.trace.context import get_context
from skywalking.trace.tags import TagDbType, TagDbInstance, TagDbStatement, \
    TagDbSqlParameters

link_vector = ['https://pypi.org/project/neo4j/']
support_matrix = {
    'neo4j': {
        '>=3.7': ['5.8.*', '5.9.*']
    }
}
note = """"""


def install():
    from neo4j._sync.work.transaction import TransactionBase
    from neo4j._async.work.transaction import AsyncTransactionBase

    _run = TransactionBase.run
    _async_run = AsyncTransactionBase.run

    def _archive_span(span, database: Optional[str], query: str, parameters: Optional[dict]):
        span.layer = Layer.Database
        span.tag(TagDbType('Neo4j'))
        span.tag(TagDbInstance(database or ''))
        span.tag(TagDbStatement(query))

        if config.plugin_sql_parameters_max_length and parameters:
            parameter = json.dumps(parameters, ensure_ascii=False)
            max_len = config.plugin_sql_parameters_max_length
            parameter = f'{parameter[0:max_len]}...' if len(
                parameter) > max_len else parameter
            span.tag(TagDbSqlParameters(f'[{parameter}]'))

    def _sw_run(self, query: str, parameters: Optional[dict], **kwargs):
        addr = self._connection.unresolved_address
        peer = f'{addr.host}:{addr.port}'
        with get_context().new_exit_span(op='Neo4j/Transaction/run',
                                         peer=peer,
                                         component=Component.Neo4j) as span:
            _archive_span(span, self._database, query, parameters)
            return _run(self, query, parameters, **kwargs)

    async def _sw_async_run(self, query: str, parameters: Optional[dict],
                            **kwargs):
        addr = self._connection.unresolved_address
        peer = f'{addr.host}:{addr.port}'
        with get_context().new_exit_span(op='Neo4j/AsyncTransaction/run',
                                         peer=peer,
                                         component=Component.Neo4j) as span:
            _archive_span(span, self._database, query, parameters)
            return await _async_run(self, query, parameters, **kwargs)

    TransactionBase.run = _sw_run
    AsyncTransactionBase.run = _sw_async_run
