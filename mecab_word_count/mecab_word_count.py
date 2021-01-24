#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

# pytype: skip-file
from __future__ import absolute_import

import argparse
import logging
import re
from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp import gcsio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import json
import MeCab
import os
import time

class WordExtractingDoFn(beam.DoFn):
  def __init__(self,dicpath):
    super(WordExtractingDoFn, self).__init__()
    self.mecab = None
    self.dicpath = dicpath
 
  def setup(self):
    logging.info('into setup')
    gcs = gcsio.GcsIO()
    dicdirname='dic/mecab-ipadic-neologd'
    path=os.getcwd()+os.sep+dicdirname
    if not os.path.isdir(path):
      os.makedirs(path)
    for f in ('char.bin','dicrc','left-id.def','matrix.bin','pos-id.def','rewrite.def','right-id.def','sys.dic','unk.dic'):
      gcs_path = self.dicpath + '/' + f
      dstpath = 'dic/mecab-ipadic-neologd/{}'.format(f)
      dstpath=os.getcwd()+os.sep+dstpath
      if not os.path.isfile(dstpath):
        src=gcs.open(gcs_path)
        logging.info("gcs_path={} dstpath={}".format(gcs_path,dstpath))     
        with open(dstpath,'wb') as outf:
          outf.write(src.read())
    time.sleep(120)
    path = os.getcwd()+os.sep+'dic/mecab-ipadic-neologd'
    self.mecab = MeCab.Tagger('-r/dev/null -d {}'.format(path))

  def __parse(self,mecab,text):
    ex_word=(u'RT',u'さん',u'様',u'ちゃん',u'くん',u'笑',u'氏',u'君',u'ツイ',u'さま')
    names=[]
    text=re.sub('https?://[^ ]+','',text)
    text=re.sub('@[^ ]+','',text)
    if not text:
      return names
    node = self.mecab.parseToNode(text)
    while node:
      word = node.surface
      pos = node.feature.split(',')
      if pos[2]==u"人名" and word not in ex_word:
        names.append(word)
      node = node.next
    return names
  """Parse each line of input text into words."""
  def process(self, element):
    created_at=element['created_at']
    text=element['text']
    names=self.__parse(self.mecab,text)
    result=[]
    for e in names:
      elm=u"{}_{}".format(created_at.year,e)
      result.append(elm)
    return result

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--input',
    dest='input',
    required=True,
    help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  parser.add_argument(
      '--dicpath',
      dest='dicpath',
      required=True,
      help='mecab dic path on gcs')
  
  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  temp_location = pipeline_options.view_as(SetupOptions)
  temp_location.save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:
    # Read the text file[pattern] into a PCollection.
    lines = p | 'Read' >> beam.io.gcp.bigquery.ReadFromBigQuery(table=known_args.input)
    counts = (
      lines
      | 'Split' >> (beam.ParDo(WordExtractingDoFn(known_args.dicpath)).with_output_types(unicode))
      | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
      | 'GroupAndSum' >> beam.CombinePerKey(sum)
    )
    # Format the counts into a PCollection of strings.
    def format_result(word, count):
      return u'{}: {}'.format(word, count)

    output = counts | 'Format' >> beam.MapTuple(format_result)
    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'Write' >> WriteToText(known_args.output)


  
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
