# Copyright 2016 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

from kubernetes_asyncio.client import apis, models
import typing as _t

LEADER_ELECTION_RECORD_ANNOTATION_KEY = (
    "control-plane.alpha.kubernetes.io/leader"
)


class EndpointAdditionalPorts(_t.NamedTuple):
    name: str
    port: int
    protocol: str = "TCP"


class EndpointLockConfig(_t.NamedTuple):
    identity: str
    ports: _t.List[EndpointAdditionalPorts]
    my_ip: str


def new(namespace, name, rlc, client=None, additional_ports=[]):
    return EndpointsLock(
        object_meta=models.V1ObjectMeta(namespace=namespace, name=name),
        client=apis.CoreV1Api(client),
        lock_config=rlc,
        additional_ports=additional_ports,
    )


class EndpointsLock:
    """Derived from resourcelock.ConfigMapLock but configures the endpoint
    itself to create an elected Service endpoint"""

    new = staticmethod(new)

    def __init__(
        self, object_meta, client, lock_config, additional_ports=[]
    ):
        self.object_meta = object_meta
        self.client = client
        self.lock_config = lock_config
        self._ep = None

    async def get(self):
        self._ep = await self.client.read_namespaced_endpoints(
            self.object_meta.name, self.object_meta.namespace
        )
        raw_record = (
            self._ep.metadata.annotations
            and self._ep.metadata.annotations.get(
                LEADER_ELECTION_RECORD_ANNOTATION_KEY
            )
        )
        record = raw_record and json.loads(raw_record)
        return record, raw_record

    async def create(self, leader_election_record):
        raw_record = json.dumps(leader_election_record)
        self._ep = await self.client.create_namespaced_endpoints(
            self.object_meta.namespace,
            models.V1Endpoints(
                metadata=models.V1ObjectMeta(
                    name=self.object_meta.name,
                    namespace=self.object_meta.namespace,
                    annotations={
                        LEADER_ELECTION_RECORD_ANNOTATION_KEY: raw_record
                    },
                ),
                subsets=list(
                    models.V1EndpointSubset(
                        addresses=list(
                            models.V1EndpointAddress(ip=self.lock_config.my_ip)
                        ),
                        ports=list(
                            models.V1EndpointPort(name, port, protocol)
                            for name, port, protocol in self.lock_config.ports
                        ),
                    )
                ),
            ),
        )

    async def update(self, leader_election_record):
        if not self._ep:
            raise Exception(
                "configmap not initialized, call get or create first"
            )
        raw_record = json.dumps(leader_election_record)
        self._ep.metadata.annotations[
            LEADER_ELECTION_RECORD_ANNOTATION_KEY
        ] = raw_record
        self._ep.subsets = list(
            models.V1EndpointSubset(
                addresses=list(
                    models.V1EndpointAddress(ip=self.lock_config.my_ip)
                ),
                ports=list(
                    models.V1EndpointPort(name, port, protocol)
                    for name, port, protocol in self.lock_config.ports
                ),
            )
        )
        self._ep = await self.client.replace_namespaced_endpoints(
            self.object_meta.name, self.object_meta.namespace, self._ep
        )

    def describe(self):
        return f"{self.object_meta.namespace}/{self.object_meta.name}"

    def identity(self):
        return self.lock_config.identity
