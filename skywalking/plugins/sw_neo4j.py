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

from skywalking import Layer, Component, config
from skywalking.trace.context import get_context
from skywalking.trace.tags import TagDbType, TagDbInstance, TagDbStatement, \
    TagDbSqlParameters

link_vector = ['https://pypi.org/project/neo4j/']
support_matrix = {
    'neo4j': {
        '<=3.9': ['4.4.0', '4.4.1', '4.4.2', '4.4.3', '4.4.4', '4.4.5', '4.4.6', '4.4.7', '4.4.8'],
        '<=3.10': ['4.4.9', '5.0.*', '5.1.*', '5.2.*'],
        '<=3.11': ['4.4.10', '4.4.11', '5.3.*', '5.4.*', '5.5.*', '5.6.*', '5.7.*', '5.8.*', '5.9.*'],
    }
}
note = """"""


def install():
    from neo4j import __version__

    def _archive_span(span, database, query, parameters, **kwargs):
        span.layer = Layer.Database
        span.tag(TagDbType('Neo4j'))
        span.tag(TagDbInstance(database or ''))
        span.tag(TagDbStatement(query))

        parameters = dict(parameters or {}, **kwargs)
        if config.plugin_sql_parameters_max_length and parameters:
            parameter = json.dumps(parameters, ensure_ascii=False)
            max_len = config.plugin_sql_parameters_max_length
            parameter = f'{parameter[0:max_len]}...' if len(
                parameter) > max_len else parameter
            span.tag(TagDbSqlParameters(f'[{parameter}]'))

    def get_peer(address):
        return f'{address.host}:{address.port}'

    def _sw_session_run(self, query, parameters, **kwargs):
        with get_context().new_exit_span(op='Neo4j/Session/run',
                                         peer=get_peer(self._pool.address),
                                         component=Component.Neo4j) as span:
            _archive_span(span, self._config.database, query, parameters, **kwargs)
            return _session_run(self, query, parameters, **kwargs)

    def _sw_transaction_run(self, query, parameters=None, **kwargs):
        with get_context().new_exit_span(op='Neo4j/Transaction/run',
                                         peer=get_peer(self._connection.unresolved_address),
                                         component=Component.Neo4j) as span:
            database = self._connection.pool.workspace_config.database
            _archive_span(span, database, query, parameters, **kwargs)
            return _transaction_run(self, query, parameters, **kwargs)

    if __version__.startswith('4.4.'):
        from neo4j import Session, Transaction

        _session_run = Session.run
        _transaction_run = Transaction.run

        Session.run = _sw_session_run
        Transaction.run = _sw_transaction_run

    elif __version__.startswith('5.'):
        from neo4j import AsyncSession, Session
        from neo4j._sync.work.transaction import TransactionBase
        from neo4j._async.work.transaction import AsyncTransactionBase

        _session_run = Session.run
        _async_session_run = AsyncSession.run
        _transaction_run = TransactionBase.run
        _async_transaction_run = AsyncTransactionBase.run

        async def _sw_async_session_run(self, query, parameters, **kwargs):
            with get_context().new_exit_span(op='Neo4j/AsyncSession/run',
                                             peer=get_peer(self._pool.address),
                                             component=Component.Neo4j) as span:
                _archive_span(span, self._config.database, query, parameters, **kwargs)
            return await _async_session_run(self, query, parameters, **kwargs)

        async def _sw_async_transaction_run(self, query, parameters, **kwargs):
            with get_context().new_exit_span(op='Neo4j/AsyncTransaction/run',
                                             peer=get_peer(self._connection.unresolved_address),
                                             component=Component.Neo4j) as span:
                _archive_span(span, self._database, query, parameters, **kwargs)
                return await _async_transaction_run(self, query, parameters, **kwargs)

        Session.run = _sw_session_run
        AsyncSession.run = _sw_async_session_run
        TransactionBase.run = _sw_transaction_run
        AsyncTransactionBase.run = _sw_async_transaction_run
