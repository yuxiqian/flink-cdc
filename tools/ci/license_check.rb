#!/usr/bin/env ruby
# frozen_string_literal: true

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

require 'zip'

# These packages don't need to be checked
EXCLUDED_PACKAGES = %w[flink-cdc-dist flink-cdc-e2e-tests].freeze

# Questionable license statements which shouldn't occur in packaged jar files
QUESTIONABLE_STATEMENTS = [
  'Binary Code License',
  'Intel Simplified Software License',
  'JSR 275',
  'Microsoft Limited Public License',
  'Amazon Software License',
  # Java SDK for Satori RTM license
  'as necessary for your use of Satori services',
  'REDIS SOURCE AVAILABLE LICENSE',
  'Booz Allen Public License',
  'Confluent Community License Agreement Version 1.0',
  # “Commons Clause” License Condition v1.0
  'the License does not grant to you, the right to Sell the Software.',
  'Sun Community Source License Version 3.0',
  'GNU General Public License',
  'GNU Affero General Public License',
  'GNU Lesser General Public License',
  'Q Public License',
  'Sleepycat License',
  'Server Side Public License',
  'Code Project Open License',
  # BSD 4-Clause
  ' All advertising materials mentioning features or use of this software must display the following acknowledgement',
  # Facebook Patent clause v1
  'The license granted hereunder will terminate, automatically and without notice, for anyone that makes any claim',
  # Facebook Patent clause v2
  'The license granted hereunder will terminate, automatically and without notice, if you (or any of your subsidiaries, corporate affiliates or agents) initiate directly or indirectly, or take a direct financial interest in, any Patent Assertion: (i) against Facebook',
  'Netscape Public License',
  'SOLIPSISTIC ECLIPSE PUBLIC LICENSE',
  # DON'T BE A DICK PUBLIC LICENSE
  "Do whatever you like with the original work, just don't be a dick.",
  # JSON License
  'The Software shall be used for Good, not Evil.',
  # can sometimes be found in "funny" licenses
  'Don’t be evil'
].freeze

# Extract Flink CDC revision number from global pom.xml
begin
  REVISION_NUMBER = File.read('pom.xml').scan(%r{<revision>(.*)</revision>}).last[0]
rescue NoMethodError
  abort 'Could not extract Flink CDC revision number from pom.xml'
end

puts "Flink CDC version: '#{REVISION_NUMBER}'"

# Traversing maven module in given path
def traverse_module(path)
  module_name = File.basename path
  return if EXCLUDED_PACKAGES.include?(module_name)

  jar_file = File.join path, 'target', "#{module_name}-#{REVISION_NUMBER}.jar"
  check_jar_license jar_file if File.exist? jar_file

  File.read(File.join(path, 'pom.xml')).scan(%r{<module>(.*)</module>}).map(&:first).each do |submodule|
    traverse_module File.join(path, submodule.to_s) unless submodule.nil?
  end
end

@tainted_records = []

# Check license issues in given jar file
def check_jar_license(jar_file)
  puts "Checking jar file #{jar_file}"
  Zip::File.open(jar_file) do |jar|
    jar.filter { |e| e.ftype == :file }
       .filter { |e| !File.basename(e.name).end_with? '.class', '.dylib', '.so', '.dll', '.gif', '.ico' }
       .filter { |e| File.basename(e.name).downcase != 'dependencies' } # frequent false-positives due to dual-licensing
       .filter { |e| !File.basename(e.name).downcase.start_with? 'license' } # false-positives due to dual-licensing
       .filter { |e| !File.basename(e.name).downcase.start_with? 'notice' } # false-positives due to optional components
       .filter { |e| !e.name.downcase.start_with? 'org/glassfish/jersey' } # dual-licensed under GPL 2 and EPL 2.0
       .filter { |e| !e.name.downcase.include? 'org.glassfish.hk2' } # dual-licensed under GPL 2 and EPL 2.0
       .filter { |e| !e.name.downcase.include? 'org.glassfish.jersey' } # dual-licensed under GPL 2 and EPL 2.0
       .map do |e|
         content = e.get_input_stream.read.force_encoding('UTF-8')
         next unless QUESTIONABLE_STATEMENTS.map { |stmt| content.include?(stmt) }.any?

         @tainted_records.push({
                                 jar_file: File.basename(jar_file),
                                 suspicious_file: e.name
                               })
       end
  end
end

traverse_module '.'

unless @tainted_records.empty?
  puts "\nError: packaged jar contains files with incompatible licenses:"
  puts @tainted_records.map { |e| "  -> In #{e[:jar_file]}: #{e[:suspicious_file]}" }.join("\n")
  abort 'See https://www.apache.org/legal/resolved.html for allowed license details.'
end

puts 'License check passed.'
