# frozen_string_literal: true
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# StarRocks sink definition generator class.
class StarRocks
  class << self
    def connector_name
      'flink-cdc-pipeline-connector-starrocks'
    end

    def prepend_to_docker_compose_yaml(docker_compose_yaml)
      docker_compose_yaml['services']['starrocks'] = {
        'image' => 'starrocks/allin1-ubuntu:3.2.6',
        'hostname' => 'starrocks',
        'ports' => %w[8080 9030],
        'volumes' => ["#{CDC_DATA_VOLUME}:/data"]
      }
    end

    def prepend_to_pipeline_yaml(pipeline_yaml)
      pipeline_yaml['sink'] = {
        'type' => 'starrocks',
        'jdbc-url' => 'jdbc:mysql://starrocks:9030',
        'load-url' => 'starrocks:8080',
        'username' => 'root',
        'password' => '',
        'table.create.properties.replication_num' => 1
      }
    end
  end
end
