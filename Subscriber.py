import logging, json, time

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, DebugOptions, GoogleCloudOptions
import apache_beam.transforms.trigger as trigger

subscription = "projects/hazem-data-engineer/subscriptions/pubsub_subcription"
project_id = "hazem-data-engineer"


class Split(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam, **kwargs):
        element = json.loads(element)
        ts_format = '%Y-%m-%d %H:%M:%S.%f UTC'
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        return [{
            'deviceId': element["deviceId"],
            'temperature': element["temperature"],
            'longitude': element["location"][0],
            'latitude': element["location"][1],
            'time': element["time"],
            'window_start': window_start,
            'window_end': window_end
        }]

def run(argv=None):
    """Build and run the pipeline."""
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    options.view_as(DebugOptions).add_experiment("use_beam_bq_sink")
    #options.view_as(GoogleCloudOptions).
    p = beam.Pipeline(options=options)


    # Big query configs
    table_spec = "hazem-data-engineer:miniprojects.temp_data6"
    table_schema = "deviceId:STRING, temperature:FLOAT, longitude:FLOAT, latitude:FLOAT, time:TIMESTAMP," \
                   " window_start:TIMESTAMP, window_end:TIMESTAMP"

    # Read from PubSub into a PCollection.
    messages = (
            p | 'Read From PubSub' >> beam.io.ReadFromPubSub(subscription=subscription).with_output_types(bytes)
            | 'Decoding' >> beam.Map(lambda x: x.decode('utf-8'))
            # | 'Windowing' >> beam.WindowInto(window.FixedWindows(5 * 60),
            | 'Windowing' >> beam.WindowInto(window.SlidingWindows(60, 10),
                   trigger=trigger.Repeatedly(
                                                 trigger.AfterAny(trigger.AfterCount(1000),
                                                                  trigger.AfterProcessingTime(5 * 60))),
                                             accumulation_mode=trigger.AccumulationMode.DISCARDING)
            | 'Extract elements' >> beam.ParDo(Split().with_output_types('unicode'))
            | 'Write into BigQuery' >> beam.io.WriteToBigQuery(
                    table=table_spec,
                    schema=table_schema,
                    method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                    triggering_frequency=5,
                    custom_gcs_temp_location="gs://temphazem/temp2",
                    # write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
