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


if __name__ == '__main__':
    from fastapi import FastAPI
    import uvicorn
    import neo4j

    app = FastAPI()


    @app.get('/users')
    async def application():
        driver = neo4j.GraphDatabase.driver('bolt://neo4j:7687/')
        with driver.session(database='neo4j') as session:
            session.run('MATCH (n: stage) WHERE n.age=$age', {'age': 10})

        with driver.session(database='neo4j') as session:
            with session.begin_transaction() as tx:
                tx.run('MATCH (n: stage) WHERE n.age=$age', {'age': 10})

        driver.close()

        return {'song': 'Despacito', 'artist': 'Luis Fonsi'}


    PORT = 9091
    app.run(host='0.0.0.0', port=PORT, debug=True)
