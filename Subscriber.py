import logging, json, time
from datetime import datetime
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, DebugOptions, GoogleCloudOptions
import apache_beam.transforms.trigger as trigger
from google.cloud import pubsub_v1

subscription = "projects/hazem-data-engineer/subscriptions/pubsub_subcription"
# sub = pubsub_v1.SubscriberClient()
project_id = "hazem-data-engineer"


def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
    """Converts a unix timestamp into a formatted string."""
    return datetime.fromtimestamp(t).strftime(fmt)

def str2timestamp(s, fmt='%Y-%m-%d %H:%M:%S.000'):
  """Converts a string into a unix timestamp."""
  dt = datetime.strptime(s, fmt)
  epoch = datetime.utcfromtimestamp(0)
  return (dt - epoch).total_seconds()

class Split(beam.DoFn):
    # def process(self, element, window=beam.DoFn.WindowParam):
    def process(self, element):
        try:
            element = json.loads(element)
            yield {
                'deviceId': element["deviceId"],
                'temperature': element["temperature"],
                'longitude': element["location"][0],
                'latitude': element["location"][1],
                'time': element["time"]
            }
        except OverflowError as e:
            # Log and count parse errors
            logging.error('Parse error on "%s"', element)
            logging.error("OverflowError error: {0}".format(e))


# class ExtractAndAvgTemp(beam.PTransform):
#     def expand(self, element):
#         return (
#                 element
#                 | beam.Map(lambda elem: (elem["deviceId"], elem["temperature"]))
#                 | "Grouping deviceId by window" >> beam.GroupByKey()
#                 | "Calculating temp mean" >> beam.CombineValues(beam.combiners.MeanCombineFn())
#         )

class ExtractAndAvgTemp(beam.PTransform):
    def expand(self, element):
        return (
                element
                | beam.Map(lambda elem: (elem["deviceId"], elem["temperature"]))
                | "Grouping deviceId by window" >> beam.CombinePerKey(sum)
        )


class AddWindowInfo(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        start = timestamp2str(int(window.start))
        end = timestamp2str(int(window.end))
        element['window_start'] = start
        element['window_end'] = end
        yield element


class DeviceTempDict(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        device_id, temp = element
        start = timestamp2str(int(window.start))
        end = timestamp2str(int(window.end))
        return [{
            'deviceID': device_id,
            'avg_temp': temp,
            'window_start': start,
            'window_end': end
        }]


def run(argv=None):
    """Build and run the pipeline."""
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    options.view_as(DebugOptions).add_experiment("use_beam_bq_sink")
    p = beam.Pipeline(options=options)

    # Big query configs
    table_spec = "hazem-data-engineer:miniprojects.temp_data1"
    table_schema = "deviceId:STRING, temperature:FLOAT, longitude:FLOAT, latitude:FLOAT, time:TIMESTAMP," \
                   " window_start:TIMESTAMP, window_end:TIMESTAMP"
    avg_table_spec = "hazem-data-engineer:miniprojects.avg_temp_data"
    avg_table_schema = "deviceId:STRING, avg_temp:FLOAT, window_start:TIMESTAMP, window_end:TIMESTAMP"

    # Read from PubSub into a PCollection and divide into windows
    messages = (
            p | 'Read From PubSub' >> beam.io.ReadFromPubSub(subscription=subscription).with_output_types(bytes)
            | 'Decoding' >> beam.Map(lambda x: x.decode('utf-8'))
            | 'Extract all elements' >> beam.ParDo(Split().with_output_types('unicode'))
            | 'Windowing' >> beam.WindowInto(window.FixedWindows(1 * 60),
            #   | 'Windowing' >> beam.WindowInto(window.SlidingWindows(2 * 60, 30),
                                               #        trigger=trigger.Repeatedly(
                                               #            trigger.AfterAny(trigger.AfterCount(200),
                                               #                             trigger.AfterProcessingTime(1 * 60))),
                                                      accumulation_mode=trigger.AccumulationMode.DISCARDING)
    )

    # Write all records to BigQuery Table.
    raw_data = (
            messages
            | 'Add Window start and end' >> beam.ParDo(AddWindowInfo())
            | 'Write into BigQuery' >> beam.io.WriteToBigQuery(table=table_spec,
                                                               schema=table_schema,
                                                               method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                                                               triggering_frequency=2,
                                                               custom_gcs_temp_location="gs://temphazem/temp3",
                                                               # write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                                                               ))

    # Write average temperature per deviceID per window
    avg_data = (
            messages | ExtractAndAvgTemp()
            | beam.ParDo(DeviceTempDict())
            | beam.io.WriteToBigQuery(table=avg_table_spec,
                                      schema=avg_table_schema,
                                      # write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                                      )
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
