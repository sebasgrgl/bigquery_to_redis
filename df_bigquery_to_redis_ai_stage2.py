#!/home/mteran/icarrier-data-dataflow/env/bin/python
"""fonYou iCarrier Data bigquery to redis Dataflow script"""

from __future__ import absolute_import

import json
import redis
import sys
import argparse
import logging
import re

import apache_beam as beam

from apache_beam.io import iobase
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.io.gcp.internal.clients import bigquery

from Crypto.Cipher import AES
from Crypto import Random

def mysql_aes_decrypt(val, key):

    def mysql_aes_key(key):
        final_key = bytearray(16)
        for i, c in enumerate(key):
            final_key[i%16] ^= ord(key[i])
        return bytes(final_key)

    def mysql_aes_val(val):
        pad_value = 16 - (len(val) % 16)
        return '%s%s' % (val, chr(pad_value)*pad_value)

    k = mysql_aes_key(key)
    #v = mysql_aes_val(val)

    cipher = AES.new(k, AES.MODE_ECB)

    return cipher.decrypt(val)

def mysql_aes_encrypt(val, key):

    def mysql_aes_key(key):
        final_key = bytearray(16)
        for i, c in enumerate(key):
            final_key[i%16] ^= ord(key[i])
        return bytes(final_key)

    def mysql_aes_val(val):
        pad_value = 16 - (len(val) % 16)
        return '%s%s' % (val, chr(pad_value)*pad_value)

    k = mysql_aes_key(key)
    v = mysql_aes_val(val)

    cipher = AES.new(k, AES.MODE_ECB)

    return cipher.encrypt(v)

class CustomOptions(PipelineOptions):
  """Custom Apache Beam pipeline options"""

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--bq_dataset',
                        help='Bigquery table dataset',
                        default='icarrier_data_test')
    parser.add_argument('--bq_table',
                        help='Bigquery table name',
                        default='icarrier_data_master_msisdn_profile')
    parser.add_argument('--redis_conn',
                        help='Redis connection string (format: ip:port,ip:port)',
                        default='127.0.0.1:6379')
    parser.add_argument('--redis_type',
                        help='Redis type (production or preproduction)',
                        default='preproduction')
    parser.add_argument('--key_name',
                        help='table key name (msisdn or hostname)',
                        default='offer')
    parser.add_argument('--value_name',
                        help='table key name (service_offer_ids or offer_ids)',
                        default='service_offer_ids')
    parser.add_argument('--key_prefix',
                        help='Redis element key prefix',
                        default='ai_')
    parser.add_argument('--key_encrypted',
                        help='Field is encrypted flag (true or false)',
                        default='false')
    parser.add_argument('--field_encrypted',
                        help='field is encrypted flag (true or false)',
                        default='true')
    parser.add_argument('--field_column',
                        help='Hashed Column Number',
                        default='0')
    parser.add_argument('--field_products',
                        help='Hashed Column Number',
                        default='1')
    parser.add_argument('--column_key',
                        help='HSET name identifier',
                        default='name')
    parser.add_argument('--column_field',
                        help='HSET key identifier',
                        default='msisdn')
    parser.add_argument('--column_value',
                        help='HSET value identifier',
                        default='products')
    parser.add_argument('--balancing',
                        help='Set to false to SET al values in all hosts',
                        default='true')
    parser.add_argument('--redis_environment',
                        help='Redis environment pre or prod',
                        default='pre')
    parser.add_argument('--backup_dataset',
                        help='Name of the backup dataset',
                        default='pre')
    parser.add_argument('--backup_table',
                        help='Name of the backup table',
                        default='pre')
    parser.add_argument('--product_max_rows',
                        help='Name of the backup table',
                        default=1)

class hash_msisdn(beam.DoFn):
  def process(self, line, columns_to_hash):
    line_array = line.split(';')
    columns2hash = columns_to_hash.split(',')
    for x in range(0,len(line_array)):
      if str(x) in str(columns2hash):
        line_array[x] = mysql_aes_encrypt("H"+line_array[x]+"4", "d568c31adcffc4739fd2ac9c5f21c02210bcaa6d0d8").encode("hex").upper()
    return [";".join(line_array)]

class hash_msisdn_from_dict(beam.DoFn):
  def process(self, line):
    line['msisdn'] = mysql_aes_encrypt("H"+line.get('msisdn')+"4", "d568c31adcffc4739fd2ac9c5f21c02210bcaa6d0d8").encode("hex").upper()
    return [line]

class unhash_msisdn(beam.DoFn):
  def process(self, line, columns_to_unhash):
    line_array = line.split(';')
    columns2unhash = columns_to_unhash.split(',')
    for x in range(0,len(line_array)):
      if str(x) in str(columns2unhash):
        try:
          line_array[x] = mysql_aes_decrypt(line_array[x].decode("hex"), "d568c31adcffc4739fd2ac9c5f21c02210bcaa6d0d8")[1:13]
        except TypeError:
          pass
    #Remove non ascii chars
    stripped = lambda s: "".join(i for i in s if 31 < ord(i) < 127)
    return [stripped(";".join(line_array))]

class split_products(beam.DoFn):
  def process(self, line, columns_to_split):
    line_array = line.split(';')
    columns2split = columns_to_split.split(',')
    for x in range(0,len(line_array)):
      if str(x) in str(columns2split):
          line_array[x] = line_array[x].replace(",",";")
    return [";".join(line_array)]

class FormatDataFn(beam.DoFn):
    """Formats big query data into a key/value tuple with a json formatted value"""

    def process(self, element, key, prefix):
        key_value = 'myhash'
        #element.pop(key, None)
        #element = {k: v for k, v in element.items() if v is not None}
        return [(key_value, json.dumps(element))]

class JoinCSVFn(beam.DoFn):
    """Formats big query data into a key/value tuple with a json formatted value"""

    def process(self, element, key, prefix):
        key_value = key
        #element.pop(key, None)
        #element = {k: v for k, v in element.items() if v is not None}
        return [(prefix + key_value , element)]

class FilterByFn(beam.DoFn):
    """Filters data based in its final character value"""

    def process(self, element, divisor, module, base):
        if int(element[0][-1:], base)%divisor == module:
            return [element]


class RedisWriteFn(beam.DoFn):
    """Writes data into redis using a redis MULTI EXEC pipeline of SET commands"""

    def __init__(self, host, port):
       self.r = redis.Redis(host=host, port=port)

    def start_bundle(self):
        self.pipe = self.r.pipeline()


    # def process(self, element, column_key, column_field, column_value):
    #     myhash =
    #     self.pipe.hset(element[column_key], element[column_field], element[column_value])
        # keys_lenght = len(keys)
        # for key in keys:
        #
        # self.pipe.hset(element[column_key], element[column_field], element[column_value])
    def process(self, element):
        hash = str(element["name"]) + "_" + str(element["msisdn"])
        element.pop('name')
        element.pop('msisdn')

        for key,value in element.items():
            if key[:5] == 'order':
                order_key = key[5:]
                if element[key] != "":
                    redis_key = str(element[key])
                    redis_value = str(element["product" + order_key])
                    self.pipe.hset(hash, redis_key, redis_value)

    def finish_bundle(self):
        self.pipe.execute()


def parse_conn_string(conn_string):
    """Parse a connection string with format ip1:port1,ip2:port2.. into a connection array"""

    parsed = []
    conns = conn_string.split(",")
    for conn in conns:
        info = conn.split(':')
        parsed.append({'host': info[0], 'port': info[1]})

    return parsed


def run():

    ## Parse args

    options = CustomOptions(flags=sys.argv)

    project = options.view_as(GoogleCloudOptions).project
    dataset = options.bq_dataset
    table_name = options.bq_table
    conns = parse_conn_string(options.redis_conn)
    value = options.value_name
    key = options.key_name
    key_prefix = options.key_prefix
    field_encrypted = options.field_encrypted
    field_column = options.field_column
    field_products = options.field_products
    base = 16 if (options.key_encrypted == 'true') else 10
    balancing = (options.balancing == 'true')
    column_key = options.column_key
    column_field = options.column_field
    column_value = options.column_value
    redis_environment = options.redis_environment
    backup_table_name = options.backup_table
    backup_dataset_name = options.backup_dataset
    product_max_rows = options.product_max_rows

    ## Function to transform a csv line to a redis dictionary type
    def _to_dictionary(line):
        line_array = line.split(';')
        header_total_rows = (2*int(product_max_rows))
        header_names = ['name','msisdn']
        result = {}
        for header_index_x in range(1,header_total_rows+1):
            for header_index_y in range(1,3):
                if (header_index_y % 2) == 0:
                    header_names.append('product'+ str(header_index_x))
                else:
                    header_names.append('order' + str(header_index_x))

        for x in range(0,len(header_names)):
            try:
                result.update( {header_names[x] : line_array[x]} )
            except IndexError:
                if (len(header_names)>len(line_array)):
                    result.update( {header_names[x] : ''})
        return result

    def split_words(dict):
        dict['products'] = dict['products'].split(',')
        return text.split(',')

    #pipeline_options.view_as(SetupOptions).save_main_session = True

    ## Set p as a beam.pipeline class
    p = beam.Pipeline(options=options)

    ## Read Big Query data
    ## Set Values for BigQuery Origin table
    table_spec = bigquery.TableReference(
        projectId=project,
        datasetId=dataset,
        tableId=table_name)
    ## Set Values for Backup table
    backup_table_spec = bigquery.TableReference(
        projectId=project,
        datasetId=backup_dataset_name,
        tableId=backup_table_name)
    ## Set backup table schema
    backup_table_schema = 'name:STRING, msisdn:STRING, products:STRING'

    BQ_DATA =  p | 'ReadTableFromBQ'  >> beam.io.Read(beam.io.BigQuerySource(table_spec))
    BQ_DATA | 'WriteBQData' >> beam.io.WriteToText('gs://sirius_ai_test/data/data_bq')
    BQ_VALUES = BQ_DATA | 'GetValuesFromBQData' >> beam.Map(lambda x: x.values())


    if field_encrypted:
        BQ_CSV = BQ_VALUES | 'CSVformatValues' >> beam.Map(lambda row: ';'.join([str(column) for column in row]))
        hashes = (BQ_CSV | beam.ParDo(unhash_msisdn(), columns_to_unhash=(field_column)))
        BQ_CSV_R = (hashes | 'hex-format' >> beam.Map(lambda x: '%s' % x))
    else:
        BQ_CSV_R = BQ_VALUES | 'CSVformatValues' >> beam.Map(lambda row: ';'.join([str(column) for column in row]))

    BQ_CSV_R | 'WriteCSVData' >> beam.io.WriteToText('gs://sirius_ai_test/data/data_csv', file_name_suffix='.csv', header='msisdn;products')
    BQ_CSV_SPL = (BQ_CSV_R | beam.ParDo(split_products(), columns_to_split=(field_products)))
    BQ_CSV_SPL | 'WriteSPLITData' >> beam.io.WriteToText('gs://sirius_ai_test/data/data_split')

    BQ_CSV_EXT = (BQ_CSV_SPL | 'ExtendCSVData'>> beam.ParDo(JoinCSVFn(), key=key, prefix=key_prefix))
    BQ_CSV_EXT | 'WriteCSVEXTData' >> beam.io.WriteToText('gs://sirius_ai_test/data/data_csv_ext')
    BQ_CSV_EXT_DEF = BQ_CSV_EXT | 'CSVEXTformatValues' >> beam.Map(lambda row: ';'.join([str(column) for column in row]))
    BQ_CSV_EXT_DEF | 'WriteCSVEXTDEFData' >> beam.io.WriteToText('gs://sirius_ai_test/data/data_csv_ext_def', file_name_suffix='.csv', header='name;msisdn;products')
    BQ_DICT = (BQ_CSV_EXT_DEF | 'ConvertCSVtoDict' >> beam.Map(_to_dictionary))
    BQ_DICT | 'WriteDICTData' >> beam.io.WriteToText('gs://sirius_ai_test/data/data_dict')

    ## Append Data Set to Backup Table on BigQuery including the key used in this Stage if the updated environment is production


    if redis_environment == 'prod':
        BQ_BACKUP_CSV = BQ_DICT | 'BCKDictionaryToCSV' >> beam.Map(lambda row: ';'.join([str(column) for column in row]))
        BQ_DICT_HASH = (BQ_DICT | 'HashMsisdns' >> beam.ParDo(hash_msisdn_from_dict()))
        BQ_DICT_HASH | 'WriteDICTHASHData' >> beam.io.WriteToText('gs://sirius_ai_test/data/data_dict_hash')
        BQ_DICT_HASH | 'BCKWriteTable' >> beam.io.WriteToBigQuery(backup_table_spec,schema=backup_table_schema,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    #table_test = (BQ_DICT | 'FormatDictData' >> beam.ParDo(FormatDataFn(), key=key, prefix=key_prefix))
    #table_test | 'WriteDictFormattedData' >> beam.io.WriteToText('gs://sirius_ai_test/data/data_dict_formated')

    #table = (p | 'ReadTable'  >> beam.io.Read(beam.io.BigQuerySource(table_spec))
    #    | 'FormatData' >> beam.ParDo(FormatDataFn(), key=key, prefix=key_prefix))

    ## Write into Redis

    for conn in conns:
        to_write = BQ_DICT
        #BQ_DICT_KEYS = BQ_DICT.keys()
        #if balancing:
            #to_write = table | 'Filter{}'.format(i) >> beam.ParDo(FilterByFn(), divisor=len(conns), module=i, base=base)
        #to_write | 'WriteFormatedOutput' >> beam.io.WriteToText('gs://sirius_ai_test/data/data_formated')
        to_write | 'WriteRedis' >> beam.ParDo(RedisWriteFn(host=conn['host'], port=conn['port']))

    ## Run & Wait

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
